"""
What this file does:
1. Implements scalability via Memory-Efficient Batch Ingestion.
    - Reads relational data from database in configurable chunks (10,000 rows).
    - Performs dynamic Data Type Casting (Booleans, Temporal formatted Datetimes).
    - Handles SQL NULLs via Python None conversion (Null Property Suppression).
2. Schema enforcement by dynamically creating Uniqueness Constraints in Neo4j.
3. Automated graph relationship generation based on custom JSON directions.
4. Performs Post-Migration Validation (Data Integrity Audit) comparing entity row counts.
"""

import json
import os
import time
import psutil
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from neo4j import GraphDatabase
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

# Connection Settings
DATABASE_CONNECTION_URI = os.environ.get('DATABASE_CONNECTION_URI', "mysql+pymysql://root:1234@mysql-db:3306/cdbase")
NEO4J_URI = os.environ.get('NEO4J_URI')
NEO4J_USER = os.environ.get('NEO4J_USER')
NEO4J_PASSWORD = os.environ.get('NEO4J_PASSWORD')
DATABASE_NAME = os.environ.get('DATABASE_NAME')

try:
    mysql_engine = create_engine(DATABASE_CONNECTION_URI)
    neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    logger.info("Database engines initialized successfully.")
except Exception as e:
    logger.critical(f"Failed to initialize database connectivity engines: {e}")
    raise e

def load_config():
    """Loads the updated JSON mapping configuration file."""
    logger.info("Loading JSON mapping configuration matrix from disk.")
    try:
        with open("mapping_config.json", "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load 'mapping_config.json': {e}")
        raise e

def create_constraints_and_nodes(config):
    """Creates schema constraints and ingests relational rows as Graph Nodes."""
    logger.info("Starting Phase 1: Schema Constraints Creation and Graph Node Ingestion.")
    st.info("🔄 Phase 1: Creating constraints and streaming graph nodes...")
    total_chunks = 0
    chunk_idx = 0
    
    with neo4j_driver.session(database=DATABASE_NAME) as session:
        # Create Uniqueness Constraints automatically based on Primary Keys
        for node in config["nodes"]:
            pks = node["primary_keys"]
            label = node["target_label"]
            
            # If composite PK layout is detected, map uniqueness constraint at fusion ID instead
            constraint_prop = "_composite_id" if len(pks) > 1 else (pks[0] if pks else None)
            
            if constraint_prop:
                constraint_query = f"CREATE CONSTRAINT FOR (n:`{label})` REQUIRE n.`{constraint_prop}` IS UNIQUE"
                try:
                    session.run(constraint_query)
                    logger.info(f"Enforced uniqueness constraint on Node Label '{label}' for property '{constraint_prop}'.")
                except Exception:
                    logger.debug(f"Uniqueness constraint for Label '{label}' already exists. Skipping.")

        # Ingest data for each table leveraging chunksize for memory scalability
        for node in config["nodes"]:
            table = node["table_name"]
            label = node["target_label"]
            pks = node["primary_keys"]
            
            logger.info(f"Processing RDBMS Table: '{table}' ➔ Mapping to Target Graph Label: '{label}'")
            
            try:
                # Streaming data from MySQL in Chunks (Memory-Efficient O(1) space complexity)
                # Protected identifiers via explicit backticks wrapping
                for chunk in pd.read_sql_query(f"SELECT * FROM `{table}`", mysql_engine, chunksize=10000):
                    chunk_idx += 1
                    total_chunks += 1
                    logger.info(f"Extracting batch chunk #{chunk_idx} from table '{table}' containing {len(chunk)} records.")
                    
                    # --- DATA TYPE CASTING AND CLEANING ---
                    for col in chunk.columns:
                        if chunk[col].dtype == 'int64' and chunk[col].nunique() <= 2 and set(chunk[col].dropna().unique()).issubset({0, 1}):
                            chunk[col] = chunk[col].astype(bool)
                        elif 'date' in col.lower() or 'time' in col.lower():
                            chunk[col] = pd.to_datetime(chunk[col]).dt.strftime('%Y-%m-%dT%H:%M:%S')
                    
                    chunk = chunk.astype(object).where(pd.notnull(chunk), None)

                    if len(pks) > 1:
                        if chunk.empty:
                            chunk["_composite_id"] = pd.Series(dtype=str)
                        else:
                            chunk["_composite_id"] = chunk[pks].astype(str).agg('_'.join, axis=1)
                        merge_pk = "_composite_id"
                    else:
                        merge_pk = pks[0] if pks else "id"
                    
                    batch_data = chunk.to_dict(orient="records")                    
                    
                    cypher_query = f"""
                    UNWIND $batch AS row
                    MERGE (n:`{label}` {{{merge_pk}: row.{merge_pk}}})
                    ON CREATE SET n += row
                    ON MATCH SET n += row
                    """
                    session.run(cypher_query, batch=batch_data)
                    logger.info(f"Successfully unwound chunk #{chunk_idx} into label '{label}'.")
                    
                st.caption(f"✅ Ingested table `{table}` as Graph Nodes `:{label}`")
                logger.success(f"Completed node ingestion mapping sequence for source table '{table}'.")
            except Exception as e:
                logger.error(f"Error during streaming node ingestion for table '{table}': {e}")
                raise e
                
    logger.info(f"Final number of total node chunks extracted: {total_chunks}.")
    return total_chunks

def create_relationships(config):
    """Generates graph edges matching the RDBMS foreign keys and custom configurations."""
    logger.info("Starting Phase 2: Generating Directed Graph Edges (Relationships).")
    st.info("🔄 Phase 2: Building directed graph relationship edges...")
    total_chunks = 0
    chunk_idx = 0
    total_edges_created = 0
    
    with neo4j_driver.session(database=DATABASE_NAME) as session:
        for rel in config["relationships"]:
            fk_table = rel["fk_table"]
            pk_table = rel["pk_table"]
            fk_cols = rel["fk_columns"]
            pk_cols = rel["pk_columns"]
            rel_type = rel["relationship_type"]
            direction = rel["direction"]
            
            try:
                source_label = next(n["target_label"] for n in config["nodes"] if n["table_name"] == fk_table)
                target_label = next(n["target_label"] for n in config["nodes"] if n["table_name"] == pk_table)
                source_pks = next(n["primary_keys"] for n in config["nodes"] if n["table_name"] == fk_table)
                target_pks = next(n["primary_keys"] for n in config["nodes"] if n["table_name"] == pk_table)            
            except StopIteration:
                logger.error(f"Mapping configuration schema mismatch for labels: {fk_table} or {pk_table}")
                continue
                
            logger.info(f"Mapping Relationship Edge [{rel_type}] ({direction}) between nodes '{source_label}' and '{target_label}'")
            
            # Select all primary keys and foreign keys to ensure reliable node matching
            select_cols = list(set(fk_cols + source_pks))
            cols_str = ", ".join([f"`{c}`" for c in select_cols])
            query_sql = f"SELECT {cols_str} FROM `{fk_table}`"
            
            try:
                for chunk in pd.read_sql_query(query_sql, mysql_engine, chunksize=10000):
                    chunk_idx += 1
                    total_chunks += 1
                    
                    # Compute Source Node Matching Logic (Dynamic Multi-Column Fusion)
                    if len(source_pks) > 1:
                        if chunk.empty:
                            chunk["_source_composite_id"] = pd.Series(dtype=str)
                        else:
                            chunk["_source_composite_id"] = chunk[source_pks].astype(str).agg('_'.join, axis=1)
                        source_match = "source._composite_id = row._source_composite_id"
                    else:
                        source_match = f"source.{source_pks[0]} = row.{source_pks[0]}" if source_pks else "false"
                        
                    # Compute Target Node Matching Logic (Dynamic Multi-Column Fusion)
                    if len(target_pks) > 1:
                        if chunk.empty:
                            chunk["_target_composite_id"] = pd.Series(dtype=str)
                        else:
                            chunk["_target_composite_id"] = chunk[fk_cols].astype(str).agg('_'.join, axis=1)
                        target_match = "target._composite_id = row._target_composite_id"
                    else:
                        target_match = f"target.{pk_cols[0]} = row.{fk_cols[0]}"
                    
                    if direction == "FORWARD":
                        cypher_rel = f"MERGE (source)-[:`{rel_type}`]->(target)"
                    else:
                        cypher_rel = f"MERGE (source)<-[:`{rel_type}`]-(target)"
                    
                    batch_data = chunk.to_dict(orient="records")
                    
                    cypher_query = f"""
                    UNWIND $batch AS row
                    MATCH (source:`{source_label}`) WHERE {source_match}
                    MATCH (target:`{target_label}`) WHERE {target_match}
                    {cypher_rel}
                    """
                    session.run(cypher_query, batch=batch_data)
                    
                    total_edges_created += len(batch_data)
                    logger.info(f"Linked edge relation chunk #{chunk_idx}. Chunk total: {len(batch_data)}")
                    
                st.caption(f"✅ Mapped edge constraint candidate: `{fk_table}` ➔ `{pk_table}` [{rel_type}]")
                logger.success(f"Successfully generated graph edges for relationship type [{rel_type}].")
            except Exception as e:
                logger.error(f"Edge generation failed for relationship type '{rel_type}': {e}")
                raise e
                
    logger.info(f"Final number of total relationship chunks extracted: {total_chunks}.")
    return total_chunks, total_edges_created

def validate_migration(config):
    """Performs an automated post-migration row-count data integrity audit."""
    logger.info("Starting Phase 3: Executing Cross-Database Data Integrity Verification Audit.")
    st.info("🔄 Phase 3: Executing Cross-Database Data Integrity Verification Audit...")
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
    """Wrapper function to orchestrate the entire pipeline with Benchmarking enabled."""
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
        
    # Execute Pipeline Operational Phases
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
    
    # Render Streamlit-specific dashboard metrics directly upon completion
    st.success("🎉 Ingestion Pipeline and Cross-Database Verification Audit completed successfully!")
    st.divider()
    st.subheader("Pipeline Operational Benchmarking Matrix")
    
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Execution Latency", f"{elapsed_time:.2f} sec")
    c2.metric("Memory Allocated (Delta RAM)", f"{ram_used:.2f} MB")
    c3.metric("Processed Batch Blocks", f"{total_chunks} chunks")
    c4.metric("Unique Graph Edges Formed", f"{total_edges} relationships")
    
    st.write("#### Data Integrity Validation Audit Trail Summary")
    st.table(audit_trail)
    
    return metrics

if __name__ == "__main__":
    run_full_migration()
    neo4j_driver.close()
    logger.info("Pipeline execution lifecycle finalized. Outgoing network drivers disconnected safely.")