"""
The main UI orchestrator of the Relational-to-Graph ETL Pipeline.
What this file does:
1. Upload satabase. The user ingests an.sql file
2. The system auto-extracts schema
3. Relationship Customization. Enables users configure each relationship label and direction
4. Graph Ingestiona and Visualization. The system migrates relational data to Neo4j
5. The system renders an interactive PyVis graph
6. The user is able to download the final graph topology mapping report in JSON format

This file calls graph_ingestion, graph_visualizer and schema_extractor modules
"""

import os
import streamlit as st
import json
import sqlparse
from loguru import logger

import graph_ingestion
import graph_visualizer
import schema_extractor
from sqlalchemy import create_engine, inspect, text
from dotenv import load_dotenv

load_dotenv()

DATABASE_DOCKER_CONNECTION = os.environ.get('DATABASE_DOCKER_CONNECTION')
DATABASE_CONNECTION_URI = os.environ.get('DATABASE_CONNECTION_URI')

st.set_page_config(layout="wide")

# ----------------- NAVIGATION STATE MANAGEMENT -----------------
SIDEBAR_PAGES = [
    "Upload New Database",
    "Run Graph Ingestion and Visualization"
]

if "page_selection" not in st.session_state:
    st.session_state["page_selection"] = "Upload New Database"

st.sidebar.title("Page Navigation")

if st.session_state["page_selection"] in SIDEBAR_PAGES:
    current_idx = SIDEBAR_PAGES.index(st.session_state["page_selection"])

    # Creates a radio side bar
    selected_stage = st.sidebar.radio(
        "Select Operation Stage", # Title of the radio buttons
        SIDEBAR_PAGES, # Pages that includes
        index=current_idx # Saves the index (0, 1) where the user is currently on
    )
    
    if selected_stage != st.session_state["page_selection"]:
        st.session_state["page_selection"] = selected_stage
        st.rerun() # Resuns page for loading all page content

else:
    st.sidebar.info("⚙️ Currently editing: Relationship Customization")
    if st.sidebar.button("⬅ Back to Upload Database", key="side_back_btn", use_container_width=True):
        st.session_state["page_selection"] = "Upload New Database"
        st.rerun()

page = st.session_state["page_selection"]
logger.info(f"Active App Page rendering: '{page}'")

def navigate_to(target_page: str):
    st.session_state["page_selection"] = target_page
    st.rerun()

# ----------------- PAGE 0: UPLOAD DATABASE -----------------
if page == "Upload New Database":
    st.title("Upload New Dataset")
    st.write("Supports any standard RDBMS SQL Database (MySQL, PostgreSQL and SQLite)")
    uploaded_file = st.file_uploader("Choose an .sql file to populate the target Relational Database", type=["sql"])

    if uploaded_file is not None:
        file_id = uploaded_file.name + "_" + str(uploaded_file.size)

        # Checks if file has already been processed, because streamlit reruns script everytime the user press a button
        # This way avoids rerunning the Script and deleting/recreating the db after any click.
        if "last_processed_file" not in st.session_state or st.session_state["last_processed_file"] != file_id:
            sql_script = uploaded_file.read().decode("utf-8", errors="replace") # errors="replace" replaces any uknown or non valid characters with '?' for the script to not crash
            
            with st.spinner("SQL File detected! Executing script and populating database automatically."):
                try:
                    local_engine = create_engine(DATABASE_DOCKER_CONNECTION)
                    dialect = local_engine.dialect.name
                    logger.info(f"Target relational database dialect identified as: '{dialect}'")

                    inspector = inspect(local_engine)
                    existing_tables = inspector.get_table_names()
                    logger.info(f"Dynamically detected existing tables: {existing_tables}")

                    # Splits the .sql script into smaller queries on ;
                    raw_queries = sqlparse.split(sql_script)
                    clean_queries = []      

                    for q in raw_queries:
                        # Cleans queries by removing spaces, new lines, characters like ';', or comments that starts with '--'. 
                        trimmed = q.strip()
                        
                        if not trimmed or trimmed == ';' or trimmed.startswith('--') or trimmed.upper().startswith('USE '):
                            continue
                        if trimmed.upper().startswith('DELIMITER'):
                            continue
                        if trimmed.startswith('/*') and trimmed.endswith('*/'):
                            continue
                        if trimmed.upper().startswith('LOCK TABLES') or trimmed.upper().startswith('UNLOCK TABLES'):
                            continue
                        upper_trimmed = trimmed.upper()
                        if upper_trimmed.startswith('SET @OLD_') or '=@OLD_' in upper_trimmed.replace(" ", ""):
                            continue
                        if trimmed.strip(';') == '':
                            continue
                        clean_queries.append(trimmed)
                    
                    with local_engine.connect() as connection:
                        with connection.begin():
                            if dialect == 'mysql':
                                # Db cleaning for removing any previous data to avoid errors: 'Table already exists' and 'Duplicate entry for key PRIMARY'
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
                                    logger.error(f"Failed to execute sub-query: {query}... Error: {query_err}")
                                    raise query_err                                

                            # Re-enable foreign key checks for MySQL and SQLite to restore relational integrity.
                            # PostgreSQL is excluded since 'SET CONSTRAINTS ALL DEFERRED' only applies to the active transaction.
                            # Postgres automatically re-enforces constraints upon commit when the transaction block closes.
                            if dialect == 'mysql':
                                connection.execute(text("SET FOREIGN_KEY_CHECKS = 1"))
                            elif dialect == 'sqlite':
                                connection.execute(text("PRAGMA foreign_keys = ON"))
                        
                    logger.success(f"Successfully executed dynamic migration matching engine dialect: '{dialect}'.")

                    logger.info("Triggering mandatory post-ingestion schema extraction sweep.")
                    # Calls schema_extractor module and reads the new, cleaned sql schema.
                    schema_extractor.extract_schema_to_json(DATABASE_CONNECTION_URI, "mapping_config.json")
                    logger.success("mapping_config.json updated automatically with fresh metadata schema structures.")
                    st.toast("✅ New schema mapping structural properties generated!")

                    # Sets this script as a processed file, for not to be running after any click
                    st.session_state["last_processed_file"] = file_id
                    st.success(f"🎉 SQL File processed automatically! Database wiped and populated successfully!")
                except Exception as e:
                    logger.error(f"Failed to execute SQL script uploaded by user: {e}")
                    st.error(f"Failed to execute SQL script: {e}")

        if st.session_state.get("last_processed_file") == file_id:
            st.write("---")
            st.success("**Database schema extracted successfully!** Proceed to customize your graph model's relationships.")
            
            col_c, col_i = st.columns(2)
            with col_c:
                if st.button("Relationship Customization ➔", key="goto_customization_btn", type="secondary", use_container_width=True):
                    navigate_to("Relationship Customization")
            with col_i:
                if st.button("Graph Ingestion ➔", key="goto_ingestion_btn", type="primary", use_container_width=True):
                    navigate_to("Run Graph Ingestion and Visualization")

# ----------------- PAGE 1: RELATIONSHIP CUSTOMIZATION -----------------
elif page == "Relationship Customization":
    st.title("Schema-Aware Graph ETL and Migration Pipeline")
    st.subheader("Relationship Directionality")

    try:
        with open("mapping_config.json", "r", encoding="utf-8") as f:
            config = json.load(f)
        logger.info("Successfully loaded 'mapping_config.json' for customization.")
    except FileNotFoundError:
        st.error("The file mapping_config.json was not found. Please upload a database first!")
        logger.warning(f"The file mapping_config.json was not found.")
        # Stops script execution to avoid pipeline from crashing
        st.stop()

    if not config.get("relationships") or len(config["relationships"]) == 0:
        st.warning("**No Physical Foreign Keys Detected in the Source Database!**")
        st.markdown("""
        Your relational database schema does not enforce foreign key constraints. 
        Because of this, the graph cannot establish connections (Edges) automatically.
        
        **How to fix this without changing your database:**
        1. Open the generated `mapping_config.json` file in your workspace.
        2. Manually define the **Relationships** in the `"relationships"` array using this format:
        ```json
        "relationships": [
            {
                "fk_table": "source_table",
                "pk_table": "target_table",
                "fk_columns": ["ForeignKeyColumn"],
                "pk_columns": ["PrimaryKeyColumn"],
                "relationship_type": "RELATIONSHIP_LABEL",
                "direction": "FORWARD"
            }
        ]
        ```
        3. Save the file and proceed to Graph Ingestion!
        """)
        logger.warning("No physical foreign keys found in mapping_config.json.")
    else:
        st.write("#### Detected Relationships (Table with foreign key ➔ Table with Primary Key):")

        # Creates a dynamic Streamlit form which enables users to alter relationships metadata
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

    st.divider()
    
    col_back, col_next = st.columns(2)
    with col_back:
        if st.button("⬅ Back to Upload", key="custom_back_btn", type="secondary", use_container_width=True):
            navigate_to("Upload New Database")
            
    with col_next:
        if st.button("Graph Ingestion ➔", key="custom_goto_ingestion_btn", type="primary", use_container_width=True):
            navigate_to("Run Graph Ingestion and Visualization")

# ----------------- PAGE 2: Graph INGESTION and VISUALIZATION -----------------
elif page == "Run Graph Ingestion and Visualization":
    st.title("Run Graph Migration Pipeline")
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
            st.download_button(label="Download Graph Topology Schema Report (JSON)", data=raw_data, file_name="graph_topology_report.json", mime="application/json")
    except Exception as e:
        st.error(f"An error occurred while rendering the visualization: {e}")