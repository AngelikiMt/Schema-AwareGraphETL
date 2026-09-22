"""
The graph Ingestion and Migration UI module
What this file does:
1. Provides the UI forinitiate the batch migration process
2. Batch ETL Orchestrator. Coordinates the execution of data ingestion in chunks

This file calls the graph_importer module
"""

import streamlit as st
import os
import graph_importer
from loguru import logger

def ingest_graph():
    st.title("Database Batch Ingestion")
    st.subheader("Execute the Migration Pipeline into Neo4j")
    
    st.info("Automatic process that reads data from dataset in chunks of 10,000 rows, performs data type casting and builds nodes/edges in Neo4j.")
    logger.info("Automatic End-to-End graph migration sequence.")
    
    if os.path.exists("mapping_config.json"):
        logger.debug("Verification complete: 'mapping_config.json' configuration matrix found.")
        
        with st.spinner("Migrating data and validating integrity... Please wait."):
            try:
                logger.info("Invoking data import and integrity validation pipeline modules via graph_importer.")
                
                graph_importer.run_full_migration()
                
                st.success("🎉 Migration and Data Integrity Validation completed successfully!")
                logger.success("End-to-End graph migration process completed successfully.")
                                
            except Exception as e:
                logger.error(f"Migration pipeline execution aborted due to a critical exception: {e}")
                st.error(f"Critical Error during ingestion: {e}")
