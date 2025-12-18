# src/etl/fetch_executives.py
import yfinance as yf
from src.database.connection import get_db
from src.database.models import Company, KeyExecutive
import time

def fetch_executives():
    db = next(get_db())
    
    # 1. Obtener empresas que tienen Ticker pero NO tienen ejecutivos
    # (Hacemos un LEFT JOIN o chequeo simple para no repetir trabajo)
    # Por simplicidad, traemos todas con Ticker y verificamos en el loop
    companies = db.query(Company).filter(Company.ticker != None).all()
    
    print(f"Buscando ejecutivos para {len(companies)} empresas...")
    
    for c in companies:
        # Verificar si ya tiene ejecutivos
        if len(c.key_people) > 0:
            continue
            
        print(f"   {c.name} ({c.ticker})...", end=" ")
        
        try:
            ticker = yf.Ticker(c.ticker)
            officers = ticker.info.get('companyOfficers', [])
            
            new_execs = []
            for off in officers:
                name = off.get('name')
                title = off.get('title')
                
                if name and title:
                    # Detectar si es importante
                    is_insider = any(role in title.upper() for role in ['CEO', 'CFO', 'PRESIDENT', 'CHAIRMAN', 'COO'])
                    
                    new_execs.append(KeyExecutive(
                        company_id=c.id,
                        name=name,
                        role=title,
                        is_insider=is_insider
                    ))
            
            if new_execs:
                db.bulk_save_objects(new_execs)
                db.commit()
                print(f" {len(new_execs)} agregados.")
            else:
                print(" Sin datos.")
                
        except Exception as e:
            print(f" Error: {e}")
        
        # Respetar límites de API
        # time.sleep(0.2) 

    db.close()
    print("Tabla 'key_executives' actualizada.")

if __name__ == "__main__":
    fetch_executives()
