# src/etl/map_tickers.py
from sqlalchemy import text
from src.database.connection import get_db

# Diccionario de emergencia para las Top 30 empresas que mueven el mercado
# Esto cubre probablemente el 70% del valor de los portafolios
MANUAL_MAP = {
    # Tech Giants
    "APPLE INC": "AAPL", "MICROSOFT CORP": "MSFT", "NVIDIA CORP": "NVDA",
    "AMAZON COM INC": "AMZN", "META PLATFORMS INC": "META", "ALPHABET INC": "GOOGL",
    "TESLA INC": "TSLA", "BROADCOM INC": "AVGO", "NETFLIX INC": "NFLX",
    "AMD": "AMD", "INTEL CORP": "INTC", "QUALCOMM INC": "QCOM",
    
    # Finance
    "BERKSHIRE HATHAWAY INC DEL": "BRK-B", "JPMORGAN CHASE & CO": "JPM",
    "VISA INC": "V", "MASTERCARD INC": "MA", "BANK OF AMERICA CORP": "BAC",
    
    # Pharma & Health
    "LILLY ELI & CO": "LLY", "UNITEDHEALTH GROUP INC": "UNH", "JOHNSON & JOHNSON": "JNJ",
    "MERCK & CO INC": "MRK", "ABBVIE INC": "ABBV", "PFIZER INC": "PFE",
    
    # Retail & Consumer
    "WALMART INC": "WMT", "COSTCO WHSL CORP": "COST", "PROCTER & GAMBLE CO": "PG",
    "HOME DEPOT INC": "HD", "COCA COLA CO": "KO", "PEPSICO INC": "PEP",
    
    # Energy
    "EXXON MOBIL CORP": "XOM", "CHEVRON CORP": "CVX"
}

def map_tickers():
    db = next(get_db())
    print("Asignando Tickers a las empresas Top...")
    
    count = 0
    # Recorremos el diccionario y actualizamos la DB
    for name_pattern, ticker in MANUAL_MAP.items():
        # Usamos LIKE para encontrar "APPLE INC" o "APPLE INC."
        query = text("UPDATE companies SET ticker = :t WHERE name LIKE :n AND ticker IS NULL")
        result = db.execute(query, {"t": ticker, "n": f"%{name_pattern}%"})
        
        if result.rowcount > 0:
            print(f"   {name_pattern} -> {ticker} ({result.rowcount} registros actualizados)")
            count += result.rowcount
            
    db.commit()
    db.close()
    print(f"Mapeo terminado. {count} empresas ahora tienen Ticker y son rastreables.")

if __name__ == "__main__":
    map_tickers()
