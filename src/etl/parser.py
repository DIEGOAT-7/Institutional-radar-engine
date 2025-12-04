# src/etl/parser.py (Versión 2.0 - Con soporte para Derivados)
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime
from sqlalchemy.orm import Session
from src.database.connection import get_db
from src.database.models import Fund, Company, Holding, DerivativePosition

# CONFIGURACIÓN
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_RAW_PATH = BASE_DIR / "data" / "raw"

NAMESPACES = {
    'ns1': 'http://www.sec.gov/edgar/document/thirteenf/informationtable',
    'ns2': 'http://www.sec.gov/edgar/thirteenf/informationtable'
}

def extract_xml_from_text(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        match = re.search(r'<informationTable.*?>.*?</informationTable>', content, re.DOTALL | re.IGNORECASE)
        if match: return match.group(0)
        return None
    except Exception as e:
        print(f"      ❌ Error leyendo archivo {file_path.name}: {e}")
        return None

def parse_13f_filing(file_path, cik, db: Session):
    print(f"  Procesando: {file_path.name}")
    
    xml_content = extract_xml_from_text(file_path)
    if not xml_content: return

    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError: return

    # Buscar el Fondo en la BD (Lógica Robusta)
    # 1. Intento Exacto
    fund = db.query(Fund).filter(Fund.cik == cik).first()
    
    # 2. Intento "Sin Ceros" (Si en la carpeta es 000123 y en la DB es 123)
    if not fund:
        cik_no_zeros = cik.lstrip("0")
        fund = db.query(Fund).filter(Fund.cik == cik_no_zeros).first()

    if not fund:
        print(f"      ⚠️ CIK {cik} (ni {cik.lstrip('0')}) encontrado en tabla 'funds'. Ejecuta populate_funds.py")
        return

    report_date = datetime.fromtimestamp(file_path.stat().st_mtime).date()
    
    # Búsqueda de filas
    rows = root.findall('.//{http://www.sec.gov/edgar/document/thirteenf/informationtable}infoTable')
    if not rows:
        rows = root.findall('.//{http://www.sec.gov/edgar/thirteenf/informationtable}infoTable')
    
    count_stock = 0
    count_deriv = 0
    
    for row in rows:
        def get_text(elem, tag):
            for ns in NAMESPACES.values():
                res = elem.find(f'{{{ns}}}{tag}')
                if res is not None: return res.text
            return elem.find(tag).text if elem.find(tag) is not None else None

        name = get_text(row, 'nameOfIssuer')
        cusip = get_text(row, 'cusip')
        val_str = get_text(row, 'value')
        put_call = get_text(row, 'putCall') # <--- AQUÍ ESTÁ LA MAGIA (PUT/CALL/None)
        
        # Extraer shares
        shrs_node = None
        for ns in NAMESPACES.values():
            n = row.find(f'{{{ns}}}shrsOrPrnAmt')
            if n is not None: 
                shrs_node = n; break
        if shrs_node is None: shrs_node = row.find('shrsOrPrnAmt')
        shrs_str = get_text(shrs_node, 'sshPrnamt') if shrs_node is not None else "0"

        if not cusip or not val_str: continue

        # 1. Gestionar Empresa
        company = db.query(Company).filter(Company.cusip == cusip).first()
        if not company:
            company = Company(name=name, cusip=cusip, sector="Unknown")
            db.add(company)
            db.flush()

        # 2. Lógica de Derivados vs Acciones
        if put_call in ['PUT', 'CALL']:
            # Es un derivado
            exists = db.query(DerivativePosition.id).filter(
                DerivativePosition.fund_id == fund.id,
                DerivativePosition.company_id == company.id,
                DerivativePosition.report_date == report_date,
                DerivativePosition.derivative_type == put_call
            ).first()

            if not exists:
                deriv = DerivativePosition(
                    fund_id=fund.id,
                    company_id=company.id,
                    report_date=report_date,
                    derivative_type=put_call,
                    value=float(val_str) * 1000,
                    shares_underlying=float(shrs_str)
                )
                db.add(deriv)
                count_deriv += 1
        else:
            # Es una acción común
            exists = db.query(Holding.id).filter(
                Holding.fund_id == fund.id,
                Holding.company_id == company.id,
                Holding.report_date == report_date
            ).first()

            if not exists:
                holding = Holding(
                    fund_id=fund.id,
                    company_id=company.id,
                    report_date=report_date,
                    value=float(val_str) * 1000,
                    shares=float(shrs_str)
                )
                db.add(holding)
                count_stock += 1
            
    db.commit()
    if count_stock > 0 or count_deriv > 0:
        print(f"      ✅ Guardadas: {count_stock} Acciones | {count_deriv} Derivados (Puts/Calls)")

def run_parser():
    print("Iniciando Parser V2 (Soporte Derivados)...")
    db = next(get_db())
    
    # Búsqueda recursiva mejorada
    all_folders = list(DATA_RAW_PATH.rglob("*"))
    cik_folders = list(set([f for f in all_folders if f.is_dir() and f.name.isdigit() and len(f.name) > 4]))
    
    if not cik_folders:
        print("❌ ERROR: No encontré carpetas de CIK en data/raw")
        return

    print(f"Procesando {len(cik_folders)} Fondos...")
    for fund_folder in cik_folders:
        cik = fund_folder.name
        filing_files = list(fund_folder.rglob("*.txt"))
        if filing_files:
            print(f"\n Fondo CIK: {cik}")
            for f in filing_files: parse_13f_filing(f, cik, db)
    
    db.close()
    print("\nETL Completado.")

if __name__ == "__main__":
    run_parser()