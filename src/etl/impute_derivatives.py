# src/etl/impute_derivatives.py
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime
from sqlalchemy import text
from src.database.connection import get_db
from src.database.models import Fund, Company, DerivativePosition

# Configuración
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_RAW_PATH = BASE_DIR / "data" / "raw"

NAMESPACES = {
    'ns1': 'http://www.sec.gov/edgar/document/thirteenf/informationtable',
    'ns2': 'http://www.sec.gov/edgar/thirteenf/informationtable'
}

def get_text(elem, tag):
    for ns in NAMESPACES.values():
        res = elem.find(f'{{{ns}}}{tag}')
        if res is not None: return res.text
    return elem.find(tag).text if elem.find(tag) is not None else None

def impute_derivatives():
    print("INICIANDO IMPUTACIÓN QUIRÚRGICA DE DERIVADOS...")
    db = next(get_db())

    # LIMPIEZA PREVIA: Borrar solo la tabla de derivados para evitar duplicados
    print("🧹 Limpiando tabla 'derivatives' antigua...")
    db.execute(text("DELETE FROM derivatives"))
    db.commit()
    print("   Tabla vacía y lista.")

    # CACHÉ EN MEMORIA (Para velocidad extrema)
    print("⏳ Cargando mapas de identidad (Funds/Companies)...")
    # Diccionario: {CIK_string: ID_int}
    fund_map = {f.cik: f.id for f in db.query(Fund).all()} 
    # Diccionario: {CUSIP_string: ID_int}
    comp_map = {c.cusip: c.id for c in db.query(Company).all()}
    
    print(f"   Memorizados {len(fund_map)} Fondos y {len(comp_map)} Empresas.")

    # BUSCAR ARCHIVOS
    # Buscamos recursivamente en raw (donde sea que estén)
    all_files = list(DATA_RAW_PATH.rglob("*.txt"))
    print(f"📂 Escaneando {len(all_files)} archivos de reportes...")

    total_derivs = 0

    for file_path in all_files:
        # Extraer CIK de la ruta del archivo (la carpeta padre suele ser el CIK)
        # Truco: Buscamos qué parte del path es un número de 10 dígitos (o parecido)
        cik_found = None
        for part in file_path.parts:
            if part.isdigit() and len(part) > 4: # Asumimos que es el CIK folder
                cik_found = part
                break
        
        if not cik_found: continue

        # Buscar ID del fondo
        fund_id = fund_map.get(cik_found)
        if not fund_id: fund_id = fund_map.get(cik_found.lstrip("0"))
        
        if not fund_id: continue # Si no tenemos el fondo registrado, saltamos

        # Leer XML
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            match = re.search(r'<informationTable.*?>.*?</informationTable>', content, re.DOTALL | re.IGNORECASE)
            if not match: continue
            
            root = ET.fromstring(match.group(0))
        except: continue

        report_date = datetime.fromtimestamp(file_path.stat().st_mtime).date()
        
        # Buscar filas
        rows = root.findall('.//{http://www.sec.gov/edgar/document/thirteenf/informationtable}infoTable')
        if not rows:
            rows = root.findall('.//{http://www.sec.gov/edgar/thirteenf/informationtable}infoTable')

        batch = []
        for row in rows:
            # AQUÍ ESTÁ EL FILTRO
            raw_pc = get_text(row, 'putCall')
            # Si no tiene etiqueta putCall, IGNORAR (Ahorramos 90% de tiempo)
            if not raw_pc: continue
            
            # Normalizar: "Put" -> "PUT"
            put_call = raw_pc.strip().upper()
            
            if put_call not in ['PUT', 'CALL']: continue

            # Si llegamos aquí, ES UN DERIVADO. Lo procesamos.
            cusip = get_text(row, 'cusip')
            val_str = get_text(row, 'value')
            
            # Buscar Company ID en memoria
            comp_id = comp_map.get(cusip)
            
            # Si la empresa no existe (raro, pero posible), la saltamos para no complicar el script rápido
            if not comp_id: continue

            # Obtener shares
            shrs_node = None
            for ns in NAMESPACES.values():
                n = row.find(f'{{{ns}}}shrsOrPrnAmt')
                if n is not None: shrs_node = n; break
            if shrs_node is None: shrs_node = row.find('shrsOrPrnAmt')
            shrs_str = get_text(shrs_node, 'sshPrnamt') if shrs_node is not None else "0"

            batch.append(DerivativePosition(
                fund_id=fund_id,
                company_id=comp_id,
                report_date=report_date,
                derivative_type=put_call,
                value=float(val_str) * 1000,
                shares_underlying=float(shrs_str)
            ))

        if batch:
            db.bulk_save_objects(batch)
            total_derivs += len(batch)
            print(f"   Injectando {len(batch)} derivados para CIK {cik_found}")

    db.commit()
    db.close()
    print("="*50)
    print(f"✅ OPERACIÓN COMPLETADA. Total derivados rescatados: {total_derivs}")
    print("="*50)

if __name__ == "__main__":
    impute_derivatives()