from src.database.connection import get_db
from src.database.models import Fund

db = next(get_db())
funds = db.query(Fund).all()

print(f"TOTAL FONDOS EN DB: {len(funds)}")
print("-" * 30)
if len(funds) > 0:
    print("Ejemplos de CIKs guardados:")
    for f in funds[:10]:
        print(f"ID: {f.id} | CIK: '{f.cik}' | Nombre: {f.name}")
else:
    print("LA BASE DE DATOS ESTÁ VACÍA. 'populate_funds.py' no guardó nada.")