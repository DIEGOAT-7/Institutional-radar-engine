# src/etl/fill_metadata.py
import yfinance as yf
from src.database.connection import get_db
from src.database.models import Company
import time

def fill_gaps():
    db = next(get_db())
    
    # Buscamos empresas que tienen Ticker PERO les falta Sector o País
    targets = db.query(Company).filter(
        Company.ticker != None,
        (Company.sector == None) | (Company.sector == "Unknown") | (Company.country == None)
    ).all()
    
    print(f"🛠️  Rellenando metadatos para {len(targets)} empresas...")
    
    count = 0
    for c in targets:
        try:
            print(f"    {c.ticker}...", end=" ")
            info = yf.Ticker(c.ticker).info
            
            # Solo actualizamos si Yahoo nos da algo útil
            if 'sector' in info:
                c.sector = info.get('sector')
                c.industry = info.get('industry')
                c.country = info.get('country')
                c.description = info.get('longBusinessSummary', '')[:500] # Primeros 500 chars
                count += 1
                print("✅")
            else:
                print(" Sin datos en Yahoo")
                
            # Guardamos cada 10 para no perder progreso
            if count % 10 == 0:
                db.commit()
                
            time.sleep(0.1) # Pausa técnica
            
        except Exception as e:
            print(f"❌ {e}")

    db.commit()
    db.close()
    print(f" Proceso terminado. {count} empresas enriquecidas.")

if __name__ == "__main__":
    fill_gaps()
