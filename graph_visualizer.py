"""
Interactive Graph Visualization Module
What this file does:
1. Connects to the migrated Neo4j graph database
2. Fetches nodes and relationships using a parameterized Cypher query
3. Generates an interactive, physics-based network graph using PyVis
4. Renders the interactive graph visualization directly inside the Streamlit UI
5. Injects a clear color guide/legend directly into the PyVis canvas output with multi-colored edges
"""

import streamlit as st
from neo4j import GraphDatabase
from pyvis.network import Network
import streamlit.components.v1 as components
import os
import re
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

NEO4J_URI = os.environ.get('NEO4J_URI')
NEO4J_USER = os.environ.get('NEO4J_USER')
NEO4J_PASSWORD = os.environ.get('NEO4J_PASSWORD')
DATABASE_NAME = os.environ.get('DATABASE_NAME')

NODE_COLOR_PALETTE = [
    "#97c2fc", "#fb7e81", "#7be141", "#ffc0cb", "#e6a1f0",
    "#ffff00", "#ff9900", "#00ffff", "#9900cc", "#ccff00"
]

EDGE_COLOR_PALETTE = [
    "#ff0000", "#0000ff", "#00aa00", "#ff00ff", "#7700aa",
    "#ff5500", "#00aaaa", "#555555", "#aa5500", "#0055a0"
]

def fetch_graph_data():
    """
    Queries Neo4j to retrieve nodes and their relationships, collecting all metadata
    1. runs a graph traversal query:
        - s --> source node, r --> relationship, t --> target node
    2. Iterates over records and constructs unique node entries using their internal 'element_id' to prevent duplicate rendering
    """
    logger.info("Initiating data fetch session from Neo4j target instance")
    
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        logger.success("Neo4j database driver connection established successfully.")
    except Exception as e:
        logger.error(f"Failed to instantiate Neo4j GraphDatabase driver connection: {e}")
        raise e
        
    nodes = {}
    edges = []
    node_labels_set = set()
    edge_types_set = set()
    
    query = "MATCH (s)-[r]->(t) RETURN s, r, t"
    logger.info(f"Executing Cypher query topology fetch payload: {query}")
    
    try:
        with driver.session(database=DATABASE_NAME) as session:
            result = session.run(query)
            for record in result:
                source = record["s"]
                relationship = record["r"]
                target = record["t"]

                # Unique id for ensuring that each node will be stored only once
                s_id = source.element_id
                s_label = list(source.labels)[0]
                s_name = source.get("name") or source.get("title") or (str(list(source.values())[0]) if source.values() else "Unknown")
                nodes[s_id] = {"label": f"{s_label}: {s_name}", "group": s_label}
                node_labels_set.add(s_label)
                
                t_id = target.element_id
                t_label = list(target.labels)[0]
                t_name = target.get("name") or target.get("title") or (str(list(target.values())[0]) if target.values() else "Unknown")
                nodes[t_id] = {"label": f"{t_label}: {t_name}", "group": t_label}
                node_labels_set.add(t_label)
                
                edges.append((s_id, t_id, relationship.type))
                edge_types_set.add(relationship.type)
                
        logger.success(f"Successfully extracted graph entities. Extracted {len(nodes)} nodes, {len(edges)} edges.")
    except Exception as e:
        logger.error(f"Cypher data extraction failed: {e}")
        raise e
    finally:
        driver.close()
        
    return nodes, edges, sorted(list(node_labels_set)), sorted(list(edge_types_set))

def show_graph():
    """
    enders an interactive, physics-enabled network graph with an embedded legend in Streamlit
    1. Fetches graph entities --> fetch_graph_data()
    2. Initializes a PyVis Network instance with interactive physics
    3. Dynamically maps color palettes to unique node labels and edge types  
    4. Populates the Network graph canvas with nodes and edges
    5. Cleans the disk by deleting the graph file that PyVis creates before using it to avoid leftover error 
    """
    st.title("📊 Live Graph Visualization")
    st.subheader("Interactive view of your migrated Neo4j Graph with Color Guide")
    
    try:
        nodes, edges, all_labels, all_edge_types = fetch_graph_data()

        if len(nodes) == 0:
            st.info("**Neo4j database is currently empty.** Migration needed.")
            return
    except Exception as e:
        st.error(f"Failed to fetch dataset from Neo4j engine: {e}")
        return

    net = Network(height="650px", width="100%", bgcolor="#ffffff", font_color="#000000", directed=True)
    
    label_to_color = {}
    for idx, label in enumerate(all_labels):
        color = NODE_COLOR_PALETTE[idx % len(NODE_COLOR_PALETTE)]
        label_to_color[label] = color

    edge_to_color = {}
    for idx, edge_type in enumerate(all_edge_types):
        color = EDGE_COLOR_PALETTE[idx % len(EDGE_COLOR_PALETTE)]
        edge_to_color[edge_type] = color

    for node_id, node_info in nodes.items():
        net.add_node(node_id, label=node_info["label"], color=label_to_color[node_info["group"]], title=node_info["group"])
        
    for source, target, label in edges:
        assigned_edge_color = edge_to_color[label]
        net.add_edge(source, target, label=label, color=assigned_edge_color)
        
    net.toggle_physics(True)
    
    path = "temp_graph.html"
    try:
        net.save_graph(path)
        
        with open(path, 'r', encoding='utf-8') as f:
            html_content = f.read()

        if os.path.exists("legend_template.html"):
            with open("legend_template.html", "r", encoding="utf-8") as tmpl_f:
                template = tmpl_f.read()

            node_entries = ""
            for label in all_labels:
                color = label_to_color[label]
                node_entries += f"""
                <div style="display: flex; align-items: center; margin-top: 5px;">
                    <div style="width: 14px; height: 14px; background-color: {color}; border-radius: 50%; border: 1px solid #444; margin-right: 8px;"></div>
                    <span>{label}</span>
                </div>
                """
                
            edge_entries = ""
            for edge_type in all_edge_types:
                color = edge_to_color[edge_type]
                edge_entries += f"""
                <div style="display: flex; align-items: center; margin-top: 6px;">
                    <div style="width: 22px; height: 3px; background-color: {color}; border-radius: 2px; margin-right: 8px;"></div>
                    <span style="font-family: monospace; font-size: 12px;">{edge_type}</span>
                </div>
                """
            
            legend_html = template.replace("{{NODE_ITEMS}}", node_entries).replace("{{EDGE_ITEMS}}", edge_entries)
            
            html_with_legend = re.sub(r'(</body>)', lambda m: legend_html + m.group(1), html_content)
        else:
            logger.warning("The 'legend_template.html' file was missing from workspace. Rendering graph fallback layout.")
            html_with_legend = html_content
        
        components.html(html_with_legend, height=700, scrolling=True)
    except Exception as e:
        logger.error(f"Rendering aborted during template parsing: {e}")
        st.error(f"Error rendering graph topology workspace: {e}")
    finally:
        if os.path.exists(path):
            os.remove(path)

if __name__ == "__main__":
    show_graph()
