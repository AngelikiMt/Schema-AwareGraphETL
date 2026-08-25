"""
What this file does:
1. Provides the UI component for triggering the graph database migration.
2. Orchestrates execution of the batch processing ETL pipeline via graph_importer.
3. Implements exception catching and operational status logging.
"""

import streamlit as st
import os
import graph_importer
from loguru import logger

def ingest_graph():    
    st.title("Database Batch Ingestion")
    st.subheader("Execute the Migration Pipeline into Neo4j")
    
    st.info("Automatic process that reads data from dataset in chunks of 10,000 rows, performs data type casting, and builds nodes/edges in Neo4j.")
    logger.info("Automatic End-to-End graph migration sequence.")
    
    # Verify the existence of the mapping structural configurations before starting execution
    if os.path.exists("mapping_config.json"):
        logger.debug("Verification complete: 'mapping_config.json' configuration matrix found.")
        
        with st.spinner("Migrating data and validating integrity... Please wait."):
            try:
                logger.info("Invoking data import and integrity validation pipeline modules via graph_importer.")
                
                # Call the full pipeline execution orchestration routine and capture returned metrics
                metrics = graph_importer.run_full_migration()
                
                st.success("🎉 Migration and Data Integrity Validation completed successfully! Go to the visualization page to see your graph.")
                logger.success("End-to-End graph migration process completed successfully.")
                
                # --- DISPLAY PERFORMANCE METRICS BELOW SUCCESS MESSAGE ---
                st.divider()
                st.subheader("Pipeline Execution Performance Benchmarks")
                
                col1, col2, col3, col4 = st.columns(4)
                col1.metric(label="⏱️ Execution Time", value=f"{metrics['execution_time']:.2f} sec")
                col2.metric(label="💾 RAM Consumed (Delta)", value=f"{metrics['ram_used']:.2f} MB")
                col3.metric(label="📦 Total Batches (Chunks)", value=f"{metrics['total_chunks']} chunks")
                col4.metric(label="🖇️Total Unique Relationships Created", value=f"{metrics['total_relationships']}")
                
                # Display the audit trail data integrity table
                st.write("#### Data Integrity Audit Trail")
                st.table(metrics["audit_trail"])
                
            except Exception as e:
                logger.error(f"Migration pipeline execution aborted due to a critical exception: {e}")
                st.error(f"Critical Error during ingestion: {e}")
