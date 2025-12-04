# src/visualization/dashboard.py
import streamlit as st
import pickle
import networkx as nx
from pyvis.network import Network
import pandas as pd
from pathlib import Path
import streamlit.components.v1 as components

# CONFIGURACIÓN DE LA PÁGINA (ESTILO TERMINAL)
st.set_page_config(
    page_title="Institutional Radar",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="📡"
)

# Estilos CSS para simular Dark Mode profesional
st.markdown("""
    <style>
    .stApp {
        background-color: #0E1117;
        color: #FAFAFA;
    }
    </style>
    """, unsafe_allow_html=True)

# Rutas
BASE_DIR = Path(__file__).resolve().parents[2]
GRAPH_PATH = BASE_DIR / "data" / "processed" / "market_graph.gpickle"

# FUNCIONES
@st.cache_data
def load_graph():
    """Carga el grafo desde el archivo pickle (el cerebro congelado)."""
    with open(GRAPH_PATH, 'rb') as f:
        return pickle.load(f)

def build_pyvis_network(G, physics=True):
    """Convierte el grafo de NetworkX a PyVis (HTML interactivo)."""
    # Crear red PyVis con fondo oscuro
    net = Network(height="600px", width="100%", bgcolor="#222222", font_color="white")
    
    # Traducir nodos y aristas de NetworkX a PyVis
    for node, attrs in G.nodes(data=True):
        # Diferenciar colores por tipo de nodo
        color = "#00ff00" if attrs.get('type') == 'company' else "#ff4b4b" # Verde Matrix vs Rojo
        size = attrs.get('importance_score', 0.01) * 300 # El tamaño depende del PageRank
        
        # Tooltip al pasar el mouse
        title = f"{node}\nType: {attrs.get('type')}\nScore: {attrs.get('importance_score', 0):.4f}"
        
        net.add_node(node, label=node, title=title, color=color, size=size)

    for source, target, attrs in G.edges(data=True):
        # El grosor depende del valor invertido
        value = attrs.get('weight', 1)
        width = 1  # Por defecto
        if value > 1000000: width = 3
        elif value > 10000000: width = 6
        
        net.add_edge(source, target, width=width, color="#555555")

    # Físicas (para que los nodos reboten y se acomoden)
    if physics:
        net.barnes_hut(gravity=-2000, central_gravity=0.3, spring_length=200)
    
    return net

# INTERFAZ PRINCIPAL
def main():
    st.title("📡 Institutional Radar // Terminal V1")
    st.markdown("---")

    # 1. Sidebar: Controles
    with st.sidebar:
        st.header("⚙️ Configuración")
        physics_on = st.checkbox("Activar Físicas (Grafo Vivo)", value=True)
        st.info("Visualizando Ecosistema EV & Tech.\nDatos procesados de SEC 13F.")

    # 2. Cargar Datos
    if not GRAPH_PATH.exists():
        st.error("⚠️ No se encontró el archivo del grafo. Ejecuta 'src/features/build_graph.py' primero.")
        return

    G = load_graph()

    # 3. Métricas Rápidas (Top Bar)
    col1, col2, col3 = st.columns(3)
    col1.metric("Nodos (Empresas/Fondos)", len(G.nodes))
    col2.metric("Conexiones (Inversiones)", len(G.edges))
    
    # Encontrar la empresa más importante
    top_company = sorted([n for n, a in G.nodes(data=True) if a['type']=='company'], 
                         key=lambda x: G.nodes[x]['importance_score'], reverse=True)[0]
    col3.metric("Top Player (PageRank)", top_company)

    # 4. El Grafo Interactivo
    st.subheader("Grafo de Influencia Institucional")
    
    # Generar HTML temporal
    pyvis_net = build_pyvis_network(G, physics=physics_on)
    try:
        path = '/tmp'
        pyvis_net.save_graph(f'{path}/pyvis_graph.html')
        HtmlFile = open(f'{path}/pyvis_graph.html', 'r', encoding='utf-8')
        
    except:
        # Fallback para Windows o rutas locales
        pyvis_net.save_graph('pyvis_graph.html')
        HtmlFile = open('pyvis_graph.html', 'r', encoding='utf-8')

    # Renderizar en Streamlit
    components.html(HtmlFile.read(), height=610)

    # 5. Tabla de Detalles (Drill Down)
    st.markdown("---")
    st.subheader("📋 Datos del Mercado (Top 10 PageRank)")
    
    # Crear un DataFrame rápido con los datos del grafo
    data = []
    for node, attrs in G.nodes(data=True):
        if attrs.get('type') == 'company':
            data.append({
                "Ticker": node,
                "Name": attrs.get('name'),
                "Sector": attrs.get('sector'),
                "PageRank Score": attrs.get('importance_score'),
                "Institutional Backers": attrs.get('institutional_popularity')
            })
            
    df = pd.DataFrame(data).sort_values("PageRank Score", ascending=False).head(10)
    st.dataframe(df, use_container_width=True)

if __name__ == "__main__":
    main()