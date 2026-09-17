"""
What this file does:
1. Connects Python with multiple databases, such as MySQL, PostgreSQL, SQLite.
2. Reads the metadata of the tables as well as their Foreign Keys (Schema Extraction).
3. Establishes the default structural direction for graph relationships.
4. Generates the mapping_config.json file for storing the graph configuration.
"""

import os
import json
from sqlalchemy import create_engine, inspect
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

def extract_schema_to_json(DATABASE_CONNECTION_URI, output_json_path):
    logger.info("Initializing automated relational database schema extraction.")
    
    try:
        engine = create_engine(DATABASE_CONNECTION_URI)
        inspector = inspect(engine)
        logger.debug("SQLAlchemy engine connected successfully to target RDBMS endpoint.")
    except Exception as e:
        logger.error(f"Database connection engine instantiation failed: {e}")
        raise e
    
    mapping_config = {
        "nodes": [],
        "relationships": [],
    }
    
    try:
        tables = inspector.get_table_names()
        logger.info(f"Detected {len(tables)} target RDBMS source tables: {tables}")
    except Exception as e:
        logger.error(f"Failed to fetch relational database table names from inspector metadata: {e}")
        raise e
    
    for table in tables:
        logger.debug(f"Processing structural metadata extraction for table: '{table}'")
        
        try:
            pk_constraint = inspector.get_pk_constraint(table)
            pk_columns = pk_constraint.get('constrained_columns', [])
            columns = [c['name'] for c in inspector.get_columns(table)]
            fkeys = inspector.get_foreign_keys(table)

            fk_column_names = []
            for fk in fkeys:
                fk_column_names.extend(fk["constrained_columns"])

            is_pure_junction = (
                len(pk_columns) > 1
                and len(fkeys) >= 2
                and set(pk_columns).issubset(set(fk_column_names))
            )

            if is_pure_junction:
                logger.info(f"Table '{table}' detected as PURE JUNCTION TABLE -> Converted to Graph Relationship.")

                fk1, fk2 = fkeys[0], fkeys[1]
                edge_props = [c for c in columns if c not in pk_columns]

                mapping_config["relationships"].append({
                    "type": "MANY_TO_MANY",
                    "junction_table": table,
                    "source_table": fk1["referred_table"],
                    "source_pk": fk1["referred_columns"],
                    "source_fk": fk1["constrained_columns"],
                    "target_table": fk2["referred_table"],
                    "target_pk": fk2["referred_columns"],
                    "target_fk": fk2["constrained_columns"],
                    "relationship_type": table.upper(),
                    "direction": "FORWARD",
                    "properties": edge_props,
                })
                continue
            
            if len(pk_columns) > 1:
                logger.info(
                    f"Table '{table}' detected as HYBRID ENTITY (Composite PK with own"
                    f" attributes: {pk_columns}) -> Node"
                )
            else:
                logger.info(
                    f"Table '{table}' detected as PURE ENTITY -> Node with PK"
                    f" {pk_columns}"
                )

            mapping_config["nodes"].append({
                "table_name": table,
                "target_label": table,
                "primary_keys": pk_columns,
                "properties": columns,
            })
        except Exception as e:
            logger.error(f"Error extracting columns or primary keys for table '{table}': {e}")
            continue
        
        try:            
            for fk in fkeys:
                rel_type = f"{table.upper()}_TO_{fk['referred_table'].upper()}"
                
                mapping_config["relationships"].append({
                    "type": "ONE_TO_MANY",
                    "fk_table": table,
                    "pk_table": fk['referred_table'],
                    "fk_columns": fk['constrained_columns'],
                    "pk_columns": fk['referred_columns'],
                    "relationship_type": rel_type,
                    "direction": "FORWARD"
                })
                logger.info(f"Mapped relationship constraint candidate: {table} ➔ {fk['referred_table']} [{rel_type}]")
        except Exception as e:
            logger.error(f"Error mapping relational foreign keys to graph edges for table '{table}': {e}")
            continue

    try:
        with open(output_json_path, 'w', encoding='utf-8') as f:
            json.dump(mapping_config, f, indent=4, ensure_ascii=False)
        logger.success(f"Initial configuration file '{output_json_path}' was successfully created.")
    except Exception as e:
        logger.error(f"Failed to write structural output configuration payload to '{output_json_path}': {e}")
        raise e

if __name__ == "__main__":
    DATABASE_CONNECTION = os.environ.get('DATABASE_CONNECTION')
    extract_schema_to_json(DATABASE_CONNECTION, "mapping_config.json")