"""
What this file does:
1. Acts as the central UI of the application.
2. Integrates the Mapping Configuration Editor (accessible via transition button).
3. Allows triggering the Graph Ingestion Pipeline directly from the UI.
4. Embedded the Interactive PyVis Graph Visualizer.
"""

import os
import streamlit as st
import json
import sqlparse
from loguru import logger

import graph_ingestion
import graph_visualizer
import schema_extractor  # Imported directly to run natively inside Docker
from sqlalchemy import create_engine, inspect, text
from dotenv import load_dotenv

load_dotenv()

# Configure loguru to write logs once globally
logger.remove()  # Remove default handler to avoid mixing stdout
logger.add(lambda msg: print(msg, end=""), level="INFO")
logger.add("app_pipeline.log", rotation="10 MB", retention="10 days", level="DEBUG")

# Target credentials inside the Docker network layout
DATABASE_DOCKER_CONNECTION = os.environ.get('DATABASE_DOCKER_CONNECTION')
DATABASE_CONNECTION_URI = os.environ.get('DATABASE_CONNECTION_URI')

# ----------------- Orchestrator - Automation -----------------
if not os.path.exists("mapping_config.json"):
    logger.warning("Configuration file 'mapping_config.json' missing. Triggering automated schema extraction.")
    try:
        schema_extractor.extract_schema_to_json(DATABASE_DOCKER_CONNECTION, "mapping_config.json")
        logger.success("Initial schema extracted successfully via native schema_extractor module call.")
        st.toast("Initial schema extracted successfully!", icon="✅")
        st.rerun()
    except Exception as e:
        logger.error(f"Critical Error: Failed to extract schema natively from the database. Exception: {e}")
        st.error("Critical Error: Failed to extract schema from the database.")
        st.stop()

st.set_page_config(layout="wide")

# ----------------- NAVIGATION STATE MANAGEMENT -----------------
SIDEBAR_PAGES = [
    "Upload New Database",
    "Run Graph Ingestion and Visualization"
]

# Initialize state variables
if "page_selection" not in st.session_state:
    st.session_state["page_selection"] = "Upload New Database"

# Check if a custom button click requested a page transition
if st.session_state.get("skip_sidebar_override", False):
    st.session_state["skip_sidebar_override"] = False
    current_page = st.session_state["page_selection"]
else:
    current_page = st.session_state["page_selection"]

st.sidebar.title("Graph ETL Platform")

# Sync sidebar radio index safely
if current_page == "Run Graph Ingestion and Visualization":
    radio_index = 1
else:
    radio_index = 0

# Sidebar radio selection (Customization is hidden from here)
selected_sidebar_page = st.sidebar.radio(
    "Select Operation Stage",
    SIDEBAR_PAGES,
    index=radio_index,
    key="navigation_radio"
)

# Detect if the user explicitly clicked a sidebar tab
if current_page in SIDEBAR_PAGES and selected_sidebar_page != current_page:
    st.session_state["page_selection"] = selected_sidebar_page
    current_page = selected_sidebar_page

# Display a back button in sidebar ONLY when viewing the hidden Customization page
if current_page == "Relationship Customization":
    st.sidebar.markdown("---")
    if st.sidebar.button("⬅ Back to Upload Database", use_container_width=True):
        st.session_state["page_selection"] = "Upload New Database"
        st.session_state["skip_sidebar_override"] = True
        st.rerun()

page = st.session_state["page_selection"]
logger.info(f"Active App Page rendering: '{page}'")

# ----------------- PAGE 0: UPLOAD DATABASE -----------------
if page == "Upload New Database":
    st.title("Upload New Dataset")
    st.write("Supports any standard RDBMS SQL Dump (MySQL, PostgreSQL, SQLite, Oracle)")
    uploaded_file = st.file_uploader("Choose a .sql file to populate the target Relational Database", type=["sql"])

    if uploaded_file is not None:
        file_id = uploaded_file.name + "_" + str(uploaded_file.size)
        
        if "last_processed_file" not in st.session_state or st.session_state["last_processed_file"] != file_id:
            sql_script = uploaded_file.read().decode("utf-8", errors="replace")
            
            with st.spinner("SQL File detected! Executing script and populating database automatically."):
                try:
                    local_engine = create_engine(DATABASE_DOCKER_CONNECTION)
                    dialect = local_engine.dialect.name
                    logger.info(f"Target relational database dialect identified as: '{dialect}'")

                    inspector = inspect(local_engine)
                    existing_tables = inspector.get_table_names()
                    logger.info(f"Dynamically detected existing tables for truncation: {existing_tables}")

                    raw_queries = sqlparse.split(sql_script)
                    clean_queries = []      

                    for q in raw_queries:
                        trimmed = q.strip()
                        
                        if not trimmed or trimmed == ';' or trimmed.startswith('--') or trimmed.upper().startswith('USE '):
                            continue
                        if trimmed.upper().startswith('DELIMITER'):
                            continue
                        if trimmed.startswith('/*') and trimmed.endswith('*/'):
                            continue
                        if trimmed.upper().startswith('LOCK TABLES') or trimmed.upper().startswith('UNLOCK TABLES'):
                            continue
                        if trimmed.strip(';') == '':
                            continue
                            
                        clean_queries.append(trimmed)
                    
                    with local_engine.connect() as connection:
                        with connection.begin():
                            if dialect == 'mysql':
                                connection.execute(text("SET FOREIGN_KEY_CHECKS = 0"))
                                for table_name in existing_tables:
                                    connection.execute(text(f"DROP TABLE IF EXISTS `{table_name}`"))
                                    
                            elif dialect == 'postgresql':
                                connection.execute(text("SET CONSTRAINTS ALL DEFERRED"))
                                for table_name in existing_tables:
                                    connection.execute(text(f"DROP TABLE IF EXISTS \"{table_name}\" CASCADE"))
                                    
                            elif dialect == 'sqlite':
                                connection.execute(text("PRAGMA foreign_keys = OFF"))
                                for table_name in existing_tables:
                                    connection.execute(text(f"DROP TABLE IF EXISTS [{table_name}]"))
                                    
                            for query in clean_queries:
                                if not query.strip():
                                    continue
                                try:
                                    connection.execute(text(query))
                                except Exception as query_err:
                                    logger.error(f"Failed to execute sub-query: {query[:100]}... Error: {query_err}")
                                    raise query_err                                
                            
                            if dialect == 'mysql':
                                connection.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
                            elif dialect == 'sqlite':
                                connection.execute(text("PRAGMA foreign_keys = ON"))
                        
                    logger.success(f"Successfully executed dynamic migration matching engine dialect: '{dialect}'.")

                    logger.info("Triggering mandatory post-ingestion schema extraction sweep.")
                    schema_extractor.extract_schema_to_json(DATABASE_CONNECTION_URI, "mapping_config.json")
                    logger.success("mapping_config.json updated automatically with fresh metadata schema structures.")
                    st.toast("New schema mapping structural properties generated!", icon="✅")

                    st.session_state["last_processed_file"] = file_id
                    st.success(f"🎉 SQL File processed automatically! Database wiped and populated successfully matching '{dialect.upper()}' dialect guidelines!")
                except Exception as e:
                    logger.error(f"Failed to execute SQL script uploaded by user: {e}")
                    st.error(f"Failed to execute SQL script: {e}")

        # Display successful upload state and navigation button
        if st.session_state.get("last_processed_file") == file_id:
            st.write("---")
            st.success("**Database schema extracted successfully!** Proceed to customize your graph model's relationships.")
            
            # Transition button to Relationship Customization
            if st.button("🚀 Proceed to Relationship Customization ➔", type="primary"):
                st.session_state["page_selection"] = "Relationship Customization"
                st.session_state["skip_sidebar_override"] = True
                st.rerun()

# ----------------- PAGE 1: CUSTOMIZATION (HIDDEN FROM SIDEBAR) -----------------
elif page == "Relationship Customization":
    st.title("Schema-Aware Graph ETL and Migration Pipeline")
    st.subheader("Relationship Directionality")

    try:
        with open("mapping_config.json", "r", encoding="utf-8") as f:
            config = json.load(f)
        logger.debug("Successfully loaded 'mapping_config.json' for customization.")
    except FileNotFoundError:
        st.error("The file mapping_config.json was not found. Please upload a database first!")
        st.stop()

    if not config.get("relationships") or len(config["relationships"]) == 0:
        st.warning("**No Physical Foreign Keys Detected in the Source Database!**")
        st.markdown("""
        Your relational database schema does not enforce foreign key constraints. 
        Because of this, the graph cannot establish connections (Edges) automatically.
        
        **How to fix this without changing your database:**
        1. Open the generated `mapping_config.json` file in your workspace.
        2. Manually define your **Logical Relationships** in the `"relationships"` array using this format:
        ```json
        "relationships": [
            {
                "fk_table": "your_source_table",
                "pk_table": "your_target_table",
                "fk_columns": ["ForeignKeyColumn"],
                "pk_columns": ["PrimaryKeyColumn"],
                "relationship_type": "YOUR_RELATIONSHIP_LABEL",
                "direction": "FORWARD"
            }
        ]
        ```
        3. Save the file and proceed to Graph Ingestion!
        """)
        logger.warning("No physical foreign keys found in mapping_config.json.")
    else:
        st.write("#### Detected Relationships (Table with foreign key ➔ Table with Primary Key):")

        with st.form("mapping_form"):
            for idx, rel in enumerate(config["relationships"]):
                st.markdown(f"**Relationship {idx+1}:** Table `{rel['fk_table']}`(`{', '.join(rel['fk_columns'])}`) ➔ Table `{rel['pk_table']}` (`{', '.join(rel['pk_columns'])}`)")
                
                col1, col2 = st.columns(2)
                with col1:
                    new_type = st.text_input(f"Relationship Name (ID: {idx})", value=rel["relationship_type"], key=f"type_{idx}")
                    config["relationships"][idx]["relationship_type"] = new_type.upper()
                    
                with col2:
                    current_dir = 0 if rel["direction"] == "FORWARD" else 1
                    new_dir = st.selectbox(f"Direction (ID: {idx})", ["FORWARD", "REVERSE"], index=current_dir, key=f"dir_{idx}")
                    config["relationships"][idx]["direction"] = new_dir
                st.write("")

            submit_button = st.form_submit_button("💾 Save Changes to JSON")
            
        if submit_button:
            try:
                with open("mapping_config.json", "w", encoding="utf-8") as f:
                    json.dump(config, f, indent=4, ensure_ascii=False)
                st.success("mapping_config.json has been successfully updated!")
                logger.success("Successfully saved relationship customization.")
            except Exception as e:
                st.error(f"Error saving changes: {e}")

    st.write("---")
    
    col_back, col_next = st.columns([1, 4])
    with col_back:
        # Action button to navigate backward
        if st.button("⬅ Back to Upload", type="secondary"):
            st.session_state["page_selection"] = "Upload New Database"
            st.session_state["skip_sidebar_override"] = True
            st.rerun()
            
    with col_next:
        # Action button to navigate forward
        if st.button("🚀 Proceed to Graph Ingestion ➔", type="primary"):
            st.session_state["page_selection"] = "Run Graph Ingestion and Visualization"
            st.session_state["skip_sidebar_override"] = True
            st.rerun()

# ----------------- PAGE 2: INGESTION & VISUALIZATION -----------------
elif page == "Run Graph Ingestion and Visualization":
    st.title("Run Graph Ingestion Pipeline")
    logger.info("Initiating structural graph ingestion routine via graph_ingestion module.")

    with st.spinner("Executing structural graph ingestion routine..."):
        try:
            metrics_results = graph_ingestion.ingest_graph()
            st.success("Graph ingestion routine executed successfully.")
        except Exception as e:
            st.error(f"An unexpected error occurred during ingestion: {e}")

    st.divider()
    st.subheader("Interactive Schema View")
    try:
        graph_visualizer.show_graph()
        
        if os.path.exists("mapping_config.json"):
            with open("mapping_config.json", "r", encoding="utf-8") as f:
                raw_data = f.read()
            st.download_button(
                label="📄 Download Graph Topology Schema Report (JSON)",
                data=raw_data,
                file_name="graph_topology_report.json",
                mime="application/json"
            )
    except Exception as e:
        st.error(f"An error occurred while rendering the visualization: {e}")