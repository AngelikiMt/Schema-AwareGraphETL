"""
What this file does:
1. Connects Python with (MySQL, PostgreSQL, Oracle, SQLite) database.
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
        # Establish database connectivity engine
        engine = create_engine(DATABASE_CONNECTION_URI)
        inspector = inspect(engine)
        logger.debug("SQLAlchemy engine connected successfully to target RDBMS endpoint.")
    except Exception as e:
        logger.error(f"Database connection engine instantiation failed: {e}")
        raise e
    
    mapping_config = {
        "nodes": [],
        "relationships": []
    }
    
    try:
        # 1. Identify and log RDBMS tables as candidate Graph Nodes
        tables = inspector.get_table_names()
        logger.info(f"Detected {len(tables)} target RDBMS source tables: {tables}")
    except Exception as e:
        logger.error(f"Failed to fetch relational database table names from inspector metadata: {e}")
        raise e
    
    for table in tables:
        logger.debug(f"Processing structural metadata extraction for table: '{table}'")
        
        try:
            # SQLAlchemy 2.0+ compatible Primary Key and structural metadata extraction
            pk_constraint = inspector.get_pk_constraint(table)
            pk_columns = pk_constraint.get('constrained_columns', [])
            columns = [c['name'] for c in inspector.get_columns(table)]
            
            if len(pk_columns) == 1:
                logger.info(f"Table '{table}' -> Detected single PK: {pk_columns}")
            else:
                logger.info(f"Table '{table}' -> Detected Composite PKs: {pk_columns}")
            
            mapping_config["nodes"].append({
                "table_name": table,
                "target_label": table,
                "primary_keys": pk_columns,
                "properties": columns
            })
        except Exception as e:
            logger.error(f"Error extracting columns or primary keys for table '{table}': {e}")
            continue
        
        try:
            # 2. Identify and log Foreign Keys as candidate Graph Relationships (Forward Direction)
            fkeys = inspector.get_foreign_keys(table)
            logger.info(f"Table '{table}' holds {len(fkeys)} foreign key constraints.")
            
            for fk in fkeys:
                # Automated UPPER_CASE naming convention based on the pointed referred table
                rel_type = f"{table.upper()}_TO_{fk['referred_table'].upper()}"
                
                mapping_config["relationships"].append({
                    "fk_table": table,                     # Source Node (holds the Foreign Key)
                    "pk_table": fk['referred_table'],       # Target Node (holds the Primary Key)
                    "fk_columns": fk['constrained_columns'],
                    "pk_columns": fk['referred_columns'],
                    "relationship_type": rel_type,
                    "direction": "FORWARD"                  # Default relationship directionality
                })
                logger.info(f"Mapped relationship constraint candidate: {table} ➔ {fk['referred_table']} [{rel_type}]")
        except Exception as e:
            logger.error(f"Error mapping relational foreign keys to graph edges for table '{table}': {e}")
            continue

    # Serialize structured mapping schema metadata into the intermediate configuration JSON file
    try:
        with open(output_json_path, 'w', encoding='utf-8') as f:
            json.dump(mapping_config, f, indent=4, ensure_ascii=False)
        logger.success(f"Initial configuration file '{output_json_path}' was successfully created.")
    except Exception as e:
        logger.error(f"Failed to write structural output configuration payload to '{output_json_path}': {e}")
        raise e

# Execution Block
if __name__ == "__main__":
    DATABASE_CONNECTION = os.environ.get('DATABASE_CONNECTION')
    extract_schema_to_json(DATABASE_CONNECTION, "mapping_config.json")