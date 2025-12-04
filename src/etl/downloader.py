# src/etl/downloader.py
import yaml
import sys
from pathlib import Path
from sec_edgar_downloader import Downloader

# CONFIGURACIÓN
# Definimos las rutas relativas al proyecto para que funcione en cualquier PC
BASE_DIR = Path(__file__).resolve().parents[2] # Sube 3 niveles hasta 'institutional-radar'
CONFIG_PATH = BASE_DIR / "config" / "funds.yaml"
DATA_RAW_PATH = BASE_DIR / "data" / "raw"

# TU IDENTIDAD PARA LA SEC (Cámbialo si quieres)
USER_EMAIL = "estudiante_data_science@proyecto.com"
USER_NAME = "Institutional Radar Bot"

def load_config():
    """Lee el archivo YAML con la lista de fondos."""
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Error, No encuentro el archivo de configuración en: {CONFIG_PATH}")
    
    with open(CONFIG_PATH, "r") as file:
        return yaml.safe_load(file)

def run_downloader():
    """Función principal que orquesta la descarga."""
    print(f"Directorio base del proyecto: {BASE_DIR}")
    print(f"Leyendo configuración desde: {CONFIG_PATH}")
    
    config = load_config()
    funds = config['selected_funds']
    
    print(f"Objetivo: Descargar datos de {len(funds)} fondos clave del sector EV & Tech.")
    
    # Inicializamos el descargador apuntando a data/raw
    dl = Downloader(DATA_RAW_PATH, USER_EMAIL, USER_NAME)
    
    for fund in funds:
        name = fund['name']
        cik = fund['cik']
        category = fund.get('type', 'General')
        
        print(f"\n Procesando: {name} ({category})")
        print(f"   [CIK: {cik}] - Descargando últimos reportes 13F-HR...")
        
        try:
            # Descargamos los últimos 4 trimestres (1 año) para probar rápido.
            # Luego podemos subir esto a 12 (3 años).
            num_filings = dl.get("13F-HR", cik, limit=4)
            print(f"   ✅ Éxito. Archivos guardados en data/raw")
            
        except Exception as e:
            print(f"   ❌ Error con {name}: {e}")

    print("\n" + "="*50)
    print("ETL DE DESCARGA FINALIZADO")
    print(f"Revisa tus archivos XML en: {DATA_RAW_PATH}")
    print("="*50)

if __name__ == "__main__":
    run_downloader()