# src/etl/market_data.py (Versión Final: Precios + Metadatos)
import yfinance as yf
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from src.database.connection import get_db
from src.database.models import Company, StockPrice
import time

def fetch_market_data():
    db = next(get_db())
    
    # Traer empresas con Ticker
    companies = db.query(Company).filter(Company.ticker != None).all()
    print(f" Descargando precios históricos para {len(companies)} empresas...")
    
    # Rango: Últimos 3 años (coincide con nuestros datos 13F)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365*3)
    
    for c in companies:
        print(f"   ⬇️ {c.ticker}...", end=" ")
        
        try:
            # 1. Metadatos (Sector/Industria) si faltan
            if not c.sector or c.sector == "Unknown":
                tick_info = yf.Ticker(c.ticker).info
                c.sector = tick_info.get('sector', 'Unknown')
                c.industry = tick_info.get('industry', 'Unknown')
                c.country = tick_info.get('country', 'Unknown')
                c.description = tick_info.get('longBusinessSummary', '')[:500]
                db.commit()
                print(f"[Info Updated]", end=" ")

            # 2. Precios
            # Verificamos si ya tiene precios recientes para no bajar todo de nuevo
            last_price = db.query(StockPrice).filter(StockPrice.company_id == c.id).order_by(StockPrice.date.desc()).first()
            
            if last_price:
                current_start = last_price.date + timedelta(days=1)
                if current_start >= end_date.date():
                    print("✅ Actualizado.")
                    continue
            else:
                current_start = start_date

            # Descarga
            df = yf.download(c.ticker, start=current_start, end=end_date, progress=False, multi_level_index=False)
            
            if df.empty:
                print("⚠️ Sin datos nuevos.")
                continue
                
            prices_batch = []
            for dt, row in df.iterrows():
                # Manejo de escalares de pandas
                close = row['Close'].item() if hasattr(row['Close'], 'item') else row['Close']
                vol = row['Volume'].item() if hasattr(row['Volume'], 'item') else row['Volume']
                
                prices_batch.append(StockPrice(
                    company_id=c.id,
                    date=dt.date(),
                    close_price=float(close),
                    volume=float(vol)
                ))
            
            db.bulk_save_objects(prices_batch)
            db.commit()
            print(f"✅ +{len(prices_batch)} días.")
            
        except Exception as e:
            print(f"❌ {e}")
            
    db.close()
    print("🏁 Tabla 'stock_prices' lista.")

if __name__ == "__main__":
    fetch_market_data()