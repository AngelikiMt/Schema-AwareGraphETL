"""
Core Ingestion and Migration ETL
What this file does:
1. Implements Memory-Efficient Batch Ingestion.
    - Reads relational data from database in chunks (10,000 rows).
    - Converts Data Types (Booleans, Datetimes, Null).
2. Uniqueness Constraints. Enforces Neo4j constraints using single or composite keys
3. Graph Construction. Builds nodes and directed edges for 1:N and N:M relationships
4. Integrity Audit. Compares row counts between MySQL and Neo4j to ensure no data loss
5. Performance Metrics. Tracks execution time, memory usage and chunks directly in the UI
"""

import json
import os
import time
import psutil
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
from neo4j import GraphDatabase
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

DATABASE_CONNECTION_URI = os.environ.get('DATABASE_CONNECTION_URI')
NEO4J_URI = os.environ.get('NEO4J_URI')
NEO4J_USER = os.environ.get('NEO4J_USER')
NEO4J_PASSWORD = os.environ.get('NEO4J_PASSWORD')
DATABASE_NAME = os.environ.get('DATABASE_NAME')
CHUNKSIZE = int(os.environ.get('CHUNKSIZE'))

try:
    mysql_engine = create_engine(DATABASE_CONNECTION_URI)
    neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    logger.success("Database engines initialized successfully.")
except Exception as e:
    logger.error(f"Failed to initialize database connectivity engines: {e}")
    raise e

def load_config():
    """Loads the JSON mapping_config file"""
    logger.info("Loading JSON mapping configuration")
    try:
        with open("mapping_config.json", "r", encoding="utf-8") as f:
            loaded_mapping_config_file = json.load(f)
            logger.info("Successfully loaded mapping_config file")
            return loaded_mapping_config_file
    except Exception as e:
        logger.error(f"Failed to load 'mapping_config.json': {e}")
        raise e

def create_constraints_and_nodes(config):
    """
    Executes Phase 1 of the ETL ingestion pipeline which is to create all Nodes and their constraints in Neo4j
    1. Reads node configuration from mapping_config.json
    2. Enforces uniqueness constraints in Neo4j for both single and composite primary keys
    3. Streams records from MySQL tables in chunks
    4. Transforms, normalizes and sanitizes relational data types 
        - Converting 0 and 1 values to Python False and True
        - Replacing SQL date or time with datetime in ISO-8601 format
        - Replacing NaN with NULLs
    5. Performs MERGE into Neo4j using the UNWIND
    """
    logger.info("Starting Phase 1: Schema Constraints Creation and Graph Node Ingestion")
    st.info("Phase 1: Creating constraints and streaming graph nodes...")
    total_chunks = 0
    chunk_idx = 0
    
    with neo4j_driver.session(database=DATABASE_NAME) as session:
        for node in config["nodes"]:
            primary_keys = node["primary_keys"]
            label = node["target_label"]

            constraint_property = "_composite_id" if len(primary_keys) > 1 else (primary_keys[0] if primary_keys else None)
            
            if constraint_property:
                constraint_query = f"CREATE CONSTRAINT FOR (n:`{label}`) REQUIRE n.`{constraint_property}` IS UNIQUE"
                try:
                    session.run(constraint_query)
                    logger.info(f"Enforced uniqueness constraint on Node Label '{label}' for property '{constraint_property}'.")
                except Exception:
                    logger.warning(f"Uniqueness constraint for Label '{label}' already exists. Skipping.")

        for node in config["nodes"]:
            table = node["table_name"]
            label = node["target_label"]
            primary_keys = node["primary_keys"]
            
            logger.info(f"Processing RDBMS Table: '{table}' ➔ Mapping to Target Graph Label: '{label}'")
            
            try:
                for chunk in pd.read_sql_query(f"SELECT * FROM `{table}`", mysql_engine, chunksize=CHUNKSIZE):
                    chunk_idx += 1
                    total_chunks += 1
                    logger.info(f"Extracting batch chunk #{chunk_idx} from table '{table}' containing {len(chunk)} records.")

                    for col in chunk.columns:
                        if chunk[col].dtype == 'int64' and chunk[col].nunique() <= 2 and set(chunk[col].dropna().unique()).issubset({0, 1}):
                            chunk[col] = chunk[col].astype(bool)
                        elif 'date' in col.lower() or 'time' in col.lower():
                            chunk[col] = pd.to_datetime(chunk[col]).dt.strftime('%Y-%m-%dT%H:%M:%S')
                    
                    chunk = chunk.astype(object).where(pd.notnull(chunk), None)

                    if len(primary_keys) > 1:
                        if chunk.empty:
                            chunk["_composite_id"] = pd.Series(dtype=str)
                        else:
                            chunk["_composite_id"] = chunk[primary_keys].astype(str).agg('_'.join, axis=1)
                        merge_primaryKey = "_composite_id"
                    else:
                        merge_primaryKey = primary_keys[0] if primary_keys else "id"
                    
                    batch_data = chunk.to_dict(orient="records")

                    # Build Nodes
                    cypher_query = f"""
                    UNWIND $batch AS row
                    MERGE (n:`{label}` {{{merge_primaryKey}: row.{merge_primaryKey}}})
                    ON CREATE SET n += row
                    ON MATCH SET n += row
                    """
                    session.run(cypher_query, batch=batch_data)
                    logger.info(f"Successfully unwound chunk #{chunk_idx} into label '{label}'.")
                    
                st.caption(f"Ingested table `{table}` as Graph Nodes `:{label}`")
                logger.success(f"Completed node ingestion mapping sequence for source table '{table}'.")
            except Exception as e:
                logger.error(f"Error during streaming node ingestion for table '{table}': {e}")
                raise e
                
    logger.info(f"Final number of total node chunks extracted: {total_chunks}.")
    return total_chunks

def create_relationships(config):
    """
    Executes Phase 2 of the ETL, generates directed graph edges (relationships) between Neo4j nodes:
    1. Checks the relationship type
        - MANY-TO-MANY: Connects source and target nodes through an intermediate junction table
        - ONE-TO-MANY: Directly links source and target nodes using foreign key (FK) references
    2. Reads relational data in chuncks of CHUNKSIZE to avoid OOM errors
    3. Resolves composite primary/foreign keys by concatenating columns into combined ID strings for node lookup (_composite_id)
    3. Sets the relationship arrow direction (FORWARD or REVERSE) chosen by the user
    4. Executes batch ingestion into Neo4j via Cypher's UNWIND and MERGE
    """
    logger.info("Starting Phase 2: Generating Directed Graph Edges")
    st.info("Phase 2: Building directed graph relationship edges...")
    total_chunks = 0
    chunk_idx = 0
    total_edges_created = 0
    
    with neo4j_driver.session(database=DATABASE_NAME) as session:
        for relationship in config["relationships"]:
            relationship_kind = relationship.get("type", "ONE_TO_MANY")

            if relationship_kind == "MANY_TO_MANY":

                junction_table = relationship["junction_table"]
                source_table = relationship["source_table"]
                target_table = relationship["target_table"]
                source_fk = relationship["source_fk"][0]
                source_pk = relationship["source_pk"][0]
                target_fk = relationship["target_fk"][0]
                target_pk = relationship["target_pk"][0]
                relationship_type = relationship["relationship_type"]
                direction = relationship["direction"]

                arrow = (
                    f"-[:`{relationship_type}`]->"
                    if direction == "FORWARD"
                    else f"<-[:`{relationship_type}`]-"
                )

                query_sql = f"SELECT * FROM `{junction_table}`"
                for chunk in pd.read_sql_query(query_sql, mysql_engine, chunksize=CHUNKSIZE):
                    batch_data = chunk.astype(object).where(pd.notnull(chunk), None)
                    batch_records = batch_data.to_dict(orient="records")

                    # Builds Edges
                    cypher = f"""
                    UNWIND $batch AS row
                    MATCH (source:`{source_table}` {{{source_pk}: row.{source_fk}}})
                    MATCH (target:`{target_table}` {{{target_pk}: row.{target_fk}}})
                    MERGE (source){arrow}(target)
                    """
                    session.run(cypher, batch=batch_records)
                    total_edges_created += len(batch_records)

            else:
                fk_table = relationship["fk_table"]
                pk_table = relationship["pk_table"]
                fk_columns = relationship["fk_columns"]
                pk_columns = relationship["pk_columns"]
                relationship_type = relationship["relationship_type"]
                direction = relationship["direction"]
            
                try:
                    source_label = next(n["target_label"] for n in config["nodes"] if n["table_name"] == fk_table)
                    target_label = next(n["target_label"] for n in config["nodes"] if n["table_name"] == pk_table)
                    source_pks = next(n["primary_keys"] for n in config["nodes"] if n["table_name"] == fk_table)
                    target_pks = next(n["primary_keys"] for n in config["nodes"] if n["table_name"] == pk_table)            
                except StopIteration:
                    logger.error(f"Mapping configuration schema mismatch for labels: {fk_table} or {pk_table}")
                    continue
                    
                logger.info(f"Mapping Relationship Edge [{relationship_type}] ({direction}) between nodes '{source_label}' and '{target_label}'")
                
                select_columns = list(set(fk_columns + source_pks))
                columns_str = ", ".join([f"`{c}`" for c in select_columns])
                query_sql = f"SELECT {columns_str} FROM `{fk_table}`"
                
                try:
                    for chunk in pd.read_sql_query(query_sql, mysql_engine, chunksize=CHUNKSIZE):
                        chunk_idx += 1
                        total_chunks += 1
                        
                        if len(source_pks) > 1:
                            if chunk.empty:
                                chunk["_source_composite_id"] = pd.Series(dtype=str)
                            else:
                                chunk["_source_composite_id"] = chunk[source_pks].astype(str).agg('_'.join, axis=1)
                            source_match = "source._composite_id = row._source_composite_id"
                        else:
                            source_match = f"source.{source_pks[0]} = row.{source_pks[0]}" if source_pks else "false"
                            
                        if len(target_pks) > 1:
                            if chunk.empty:
                                chunk["_target_composite_id"] = pd.Series(dtype=str)
                            else:
                                chunk["_target_composite_id"] = chunk[fk_columns].astype(str).agg('_'.join, axis=1)
                            target_match = "target._composite_id = row._target_composite_id"
                        else:
                            target_match = f"target.{pk_columns[0]} = row.{fk_columns[0]}"
                        
                        if direction == "FORWARD":
                            cypher_rel = f"MERGE (source)-[:`{relationship_type}`]->(target)"
                        else:
                            cypher_rel = f"MERGE (source)<-[:`{relationship_type}`]-(target)"
                        
                        batch_data = chunk.to_dict(orient="records")

                        # Buids Edges
                        cypher_query = f"""
                        UNWIND $batch AS row
                        MATCH (source:`{source_label}`) WHERE {source_match}
                        MATCH (target:`{target_label}`) WHERE {target_match}
                        {cypher_rel}
                        """
                        session.run(cypher_query, batch=batch_data)
                        
                        total_edges_created += len(batch_data)
                        logger.info(f"Linked edge relation chunk #{chunk_idx}. Chunk total: {len(batch_data)}")
                        
                    st.caption(f"Mapped edge constraint candidate: `{fk_table}` ➔ `{pk_table}` [{relationship_type}]")
                    logger.success(f"Successfully generated graph edges for relationship type [{relationship_type}].")
                except Exception as e:
                    logger.error(f"Edge generation failed for relationship type '{relationship_type}': {e}")
                    raise e
                
    logger.info(f"Final number of total relationship chunks extracted: {total_chunks}.")
    return total_chunks, total_edges_created

def validate_migration(config):
    """Performs an automated post-migration row-count data integrity audit."""
    logger.info("Starting Phase 3: Executing Cross-Database Data Integrity Verification Audit.")
    st.info("Phase 3: Executing Cross-Database Data Integrity Verification Audit...")
    all_passed = True
    audit_results = []
    
    with neo4j_driver.session(database=DATABASE_NAME) as session:
        for node in config["nodes"]:
            table = node["table_name"]
            label = node["target_label"]
            
            try:
                mysql_count = pd.read_sql_query(f"SELECT COUNT(*) AS cnt FROM `{table}`", mysql_engine)['cnt'].iloc[0]
                neo4j_res = session.run(f"MATCH (n:`{label}`) RETURN count(n) AS cnt").single()
                neo4j_count = neo4j_res["cnt"]
                
                status = (mysql_count == neo4j_count)
                if not status:
                    all_passed = False
                
                audit_results.append({
                    "Entity Label": label,
                    "Source Count (RDBMS)": int(mysql_count),
                    "Target Count (Graph)": int(neo4j_count),
                    "Audit Status": "PASS" if status else "FAIL"
                })
                logger.info(f"Audit Result for {label} -> Source: {mysql_count} | Graph: {neo4j_count} | Status: {'PASS' if status else 'FAIL'}")

            except Exception as e:
                logger.error(f"Data integrity row-count verification failed for entity block '{label}': {e}")
                all_passed = False
                
    return all_passed, audit_results

def run_full_migration():
    """
    Orchestrate the complete end-to-end migration pipeline with benchmarking
    1. Starts benchmark tracking, execution timer and starting RAM usage
    2. Clears the Neo4j database to ensure a clean migration workspace
    3. Executes the 3 core pipeline phases sequentially:
        - Creates constraints and nodes --> create_constraints_and_nodes()
        - Builds directed relationship edges --> create_relationships()
        - Validates cross-database data integrity --> validate_migration()
    4. Calculates performance metrics
    5. Renders a benchmark dashboard and validation table in the Streamlit UI
    """
    config_data = load_config()

    start_time = time.time()
    process = psutil.Process(os.getpid())
    start_memory = process.memory_info().rss / (1024 * 1024) 
    logger.info(f"Starting pipeline benchmarking. Initial RAM usage: {start_memory:.2f} MB")

    logger.info("Executing complete flush routine on target Neo4j active workspace graph database instance.")
    try:
        with neo4j_driver.session(database=DATABASE_NAME) as session:
            session.run("MATCH (n) DETACH DELETE n")
        logger.success("Target Neo4j graph storage memory space successfully cleared for fresh migration streams.")
    except Exception as e:
        logger.error(f"Database graph truncation flush statement execution crashed: {e}")
        raise e
        
    node_chunks = create_constraints_and_nodes(config_data)
    edge_chunks, total_edges = create_relationships(config_data)
    validation_status, audit_trail = validate_migration(config_data)

    end_time = time.time()
    end_memory = process.memory_info().rss / (1024 * 1024) 
    elapsed_time = end_time - start_time
    ram_used = end_memory - start_memory
    total_chunks = node_chunks + edge_chunks
    
    logger.success(f"--- BENCHMARKING REPORT ---")
    logger.success(f"Total Execution Time: {elapsed_time:.2f} seconds.")
    logger.success(f"Final RAM Usage: {end_memory:.2f} MB (Delta: {ram_used:.2f} MB).")
    logger.success(f"Total Relationships Discovered & Created: {total_edges}")
    logger.success(f"---------------------------")
 
    metrics = {
        "execution_time": elapsed_time,
        "ram_used": ram_used,
        "total_chunks": total_chunks,
        "total_relationships": total_edges,
        "validation_passed": validation_status,
        "audit_trail": audit_trail
    }
    
    st.success("Ingestion Pipeline and Cross-Database Verification Audit completed successfully!")
    st.divider()
    st.subheader("Pipeline Execution Performance Benchmarks")
    
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("⏱️ Execution Time", f"{elapsed_time:.2f} sec")
    c2.metric("💾 RAM Consumed", f"{ram_used:.2f} MB")
    c3.metric("📦 Total Chunks", f"{total_chunks} chunks")
    c4.metric("🖇️Total Unique Relationships", f"{total_edges} relationships")
    
    st.write("#### Data Integrity Audit Trail")
    st.table(audit_trail)
    
    return metrics

if __name__ == "__main__":
    run_full_migration()
    neo4j_driver.close()
    logger.info("Pipeline execution lifecycle finalized. Outgoing network drivers disconnected safely.")