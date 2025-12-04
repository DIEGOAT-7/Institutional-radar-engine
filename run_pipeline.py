# run_pipeline.py
import subprocess
import sys
import time
from pathlib import Path

def run_command(command, description):
    print(f"\n{description}")
    try:
        # Ejecuta el comando en la terminal
        subprocess.check_call(command, shell=True)
        print(f"{description} completado con éxito.")
    except subprocess.CalledProcessError as e:
        print(f"Error ejecutando: {description}")
        print(f"   Detalle: {e}")
        sys.exit(1)

def main():
    print("===================================================")
    print("   INSTITUTIONAL DATA ENGINE - MASTER PIPELINE")
    print("===================================================")
    print("Esto reconstruirá la Base de Datos desde cero.")
    print("Asegúrate de tener los archivos RAW en 'data/raw'.\n")
    
    # 1. Limpieza (Opcional, comentada por seguridad)
    # db_path = Path("data/institutional_radar.db")
    # if db_path.exists():
    #     print("Borrando DB antigua...")
    #     db_path.unlink()

    # 2. Inicialización
    run_command("python3 init_project_db.py", "Creando Estructura SQL")
    
    # 3. Semilla de Fondos
    run_command("python3 populate_funds.py", "Registrando Top 38 Fondos")
    
    # 4. Ingesta (Descarga)
    # Nota: Si ya tienes los datos, esto será rápido.
    run_command("python3 src/etl/downloader.py", "Descargando Datos SEC (Extract)")
    
    # 5. Procesamiento Core
    run_command("python3 -m src.etl.parser", "Procesando XMLs y Derivados (Transform & Load)")
    
    # 6. Enriquecimiento Maestro (Tickers)
    run_command("python3 -m src.etl.master_ticker_map", "Mapeando Tickers Oficiales")
    
    # 7. Metadatos
    run_command("python3 -m src.etl.fill_metadata", "Descargando Sectores e Industrias")
    
    # 8. Mercado
    run_command("python3 -m src.etl.market_data", "Descargando Precios Históricos")

    print("\n===================================================")
    print("PIPELINE FINALIZADO. DATA WAREHOUSE LISTO.")
    print("===================================================")

if __name__ == "__main__":
    main()