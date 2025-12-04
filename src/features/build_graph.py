# src/features/build_graph.py
import networkx as nx
from sqlalchemy.orm import Session
from src.database.connection import get_db
from src.database.models import Fund, Company, Holding, StockPrice
import pickle
from pathlib import Path

# Rutas
BASE_DIR = Path(__file__).resolve().parents[2]
GRAPH_OUTPUT_PATH = BASE_DIR / "data" / "processed" / "market_graph.gpickle"

def build_network():
    print("Construyendo el Grafo de Conocimiento Financiero...")
    
    db = next(get_db())
    
    # 1. Inicializar un Grafo Dirigido (Directed Graph)
    # Dirigido porque: El Fondo --> POSEE --> La Empresa (la relación tiene dirección)
    G = nx.DiGraph()
    
    # 2. Obtener Nodos (Fondos y Empresas)
    # Solo traemos empresas que tengan Ticker (las relevantes del sector EV/Tech)
    tech_companies = db.query(Company).filter(Company.ticker != None).all()
    funds = db.query(Fund).all()
    
    print(f"   🔹 Añadiendo {len(funds)} Fondos y {len(tech_companies)} Empresas clave...")
    
    # Añadir Nodos de Empresas
    for comp in tech_companies:
        G.add_node(comp.ticker, type='company', name=comp.name, sector=comp.sector)
        
    # Añadir Nodos de Fondos (Usamos su CIK o Nombre como ID)
    for fund in funds:
        # Limpiamos el nombre para que se vea bien en el grafo
        short_name = fund.name.replace(" Advisors", "").replace(" Management", "").replace(" Group", "").replace(" Inc", "")
        G.add_node(short_name, type='fund', name=fund.name, strategy=fund.strategy)

    # 3. Crear las Aristas (Relaciones de Inversión)
    print("   🔗 Conectando inversiones (esto puede tomar unos segundos)...")
    
    # Buscamos holdings SOLO de las empresas tech que filtramos
    tech_ids = [c.id for c in tech_companies]
    
    # Query optimizado: Traer holdings recientes de esas empresas
    holdings = db.query(Holding).filter(Holding.company_id.in_(tech_ids)).all()
    
    edge_count = 0
    for h in holdings:
        # Recuperar nombres para los nodos
        fund_node = h.fund.name.replace(" Advisors", "").replace(" Management", "").replace(" Group", "").replace(" Inc", "")
        company_node = h.company.ticker
        
        if fund_node in G.nodes and company_node in G.nodes:
            # Añadir arista con peso (Valor de la inversión)
            G.add_edge(fund_node, company_node, weight=h.value, shares=h.shares)
            edge_count += 1

    print(f"   ✅ Grafo construido con {len(G.nodes)} nodos y {edge_count} conexiones.")

    # Data Science
    # Calculamos métricas sobre la estructura de la red
    
    print("Calculando Métricas de Red...")
    
    # 1. Grado de Entrada (In-Degree): ¿Cuántos fondos invierten en esta empresa?
    # Indica "Popularidad Institucional"
    in_degrees = dict(G.in_degree())
    nx.set_node_attributes(G, in_degrees, 'institutional_popularity')
    
    # 2. Pagerank (Algoritmo de Google):
    # No solo cuántos te compran, sino QUÉ TAN IMPORTANTES son los que te compran.
    pagerank = nx.pagerank(G, weight='weight')
    nx.set_node_attributes(G, pagerank, 'importance_score')
    
    # Imprimir un top 5 de ejemplo
    sorted_companies = sorted(
        [n for n, attr in G.nodes(data=True) if attr['type'] == 'company'],
        key=lambda x: G.nodes[x]['importance_score'],
        reverse=True
    )
    
    print("\nTOP 5 EMPRESAS POR IMPORTANCIA SISTÉMICA (PageRank):")
    for i, ticker in enumerate(sorted_companies[:5]):
        score = G.nodes[ticker]['importance_score']
        investors = G.nodes[ticker]['institutional_popularity']
        print(f"   {i+1}. {ticker} (Score: {score:.4f}) - En carteras de {investors} fondos")

    # 4. Guardar el Grafo para la App
    # Usamos pickle para guardar el objeto Python completo
    with open(GRAPH_OUTPUT_PATH, 'wb') as f:
        pickle.dump(G, f)
    
    print(f"\nGrafo guardado en: {GRAPH_OUTPUT_PATH}")
    print("   Listo para ser visualizado en la Terminal.")

if __name__ == "__main__":
    build_network()