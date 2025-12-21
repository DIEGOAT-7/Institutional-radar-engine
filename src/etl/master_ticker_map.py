# src/etl/master_ticker_map.py
import requests
import pandas as pd
from sqlalchemy import text
from src.database.connection import get_db
from src.database.models import Company

def normalize_name(name):
    """Limpia el nombre para mejorar el match (QUITA INC, CORP, LTD, PUNTOS)."""
    if not name: return ""
    name = name.upper()
    remove_list = [" INC", " CORP", " CO", " LTD", " PLC", " AG", " SA", ".", ","]
    for word in remove_list:
        name = name.replace(word, "")
    return name.strip()

def run_master_mapping():
    db = next(get_db())
    print("Iniciando Mapeo Maestro de Tickers y S&P 500...")

    # --- 1. OBTENER DICCIONARIO OFICIAL SEC ---
    print("Descargando company_tickers.json de la SEC...", end=" ")
    headers = {'User-Agent': 'Institutional Radar Project (education@example.com)'}
    url = "https://www.sec.gov/files/company_tickers.json"
    
    try:
        r = requests.get(url, headers=headers)
        raw_data = r.json()
        # Convertir a DataFrame
        sec_df = pd.DataFrame.from_dict(raw_data, orient='index')
        # Normalizar nombres del SEC
        sec_df['clean_title'] = sec_df['title'].apply(normalize_name)
        print(f"Listo. {len(sec_df)} empresas oficiales cargadas.")
    except Exception as e:
        print(f"Error descargando SEC data: {e}")
        return

    # --- 2. OBTENER LISTA S&P 500 (Wikipedia) ---
    print("Descargando lista S&P 500 de Wikipedia...", end=" ")
    try:
        sp500_table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
        sp500_tickers = set(sp500_table['Symbol'].unique())
        print(f"Listo. {len(sp500_tickers)} componentes identificados.")
    except Exception as e:
        print(f" Error descargando S&P 500: {e}")
        sp500_tickers = set()

    # --- 3. PROCESO DE MAPEO EN BASE DE DATOS ---
    # Traemos empresas que NO tienen Ticker o NO sabemos si son SP500
    companies = db.query(Company).all()
    print(f"Analizando {len(companies)} empresas en nuestra DB...")
    
    updated_tickers = 0
    updated_sp500 = 0
    
    # Creamos un diccionario rápido para búsqueda por nombre {NOMBRE_LIMPIO: (TICKER, CIK)}
    sec_map = dict(zip(sec_df['clean_title'], zip(sec_df['ticker'], sec_df['cik_str'])))

    for c in companies:
        cambio = False
        
        # A. INTENTAR LLENAR TICKER SI FALTA
        if not c.ticker:
            my_clean_name = normalize_name(c.name)
            match = sec_map.get(my_clean_name)
            
            if match:
                c.ticker = match[0]
                c.cik = str(match[1]).zfill(10) # Guardamos el CIK de la empresa también
                updated_tickers += 1
                cambio = True
        
        # B. VERIFICAR S&P 500 (Si tiene ticker)
        if c.ticker:
            is_in_sp = c.ticker in sp500_tickers
            if c.is_sp500 != is_in_sp:
                c.is_sp500 = is_in_sp
                updated_sp500 += 1
                cambio = True
    
    db.commit()
    db.close()
    
    print("-" * 50)
    print(f" RESULTADOS:")
    print(f"   - Tickers Nuevos Asignados: {updated_tickers}")
    print(f"   - Flags S&P 500 Actualizados: {updated_sp500}")
    print("-" * 50)

if __name__ == "__main__":
    run_master_mapping()
