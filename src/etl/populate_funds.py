# populate_funds.py (Versión: Guardado Individual)
import yaml
from pathlib import Path
from sqlalchemy.exc import IntegrityError
from src.database.connection import get_db
from src.database.models import Fund

def populate():
    # 1. Cargar Configuración
    config_path = Path("config/funds.yaml")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    
    db = next(get_db())
    funds_list = config['selected_funds']
    
    print(f" Intentando registrar {len(funds_list)} fondos...")
    
    registrados = 0
    ya_existian = 0
    errores = 0
    
    for item in funds_list:
        try:
            # Verificar si existe
            exists = db.query(Fund).filter(Fund.cik == item['cik']).first()
            
            if not exists:
                new_fund = Fund(
                    cik=item['cik'],
                    name=item['name'],
                    strategy=item.get('type', 'General')
                )
                db.add(new_fund)
                
                # --- LA CLAVE ESTÁ AQUÍ ---
                # Guardamos INMEDIATAMENTE. Si este falla, no afecta a los demás.
                db.commit() 
                
                print(f"   Guardado en DB: {item['name']}")
                registrados += 1
            else:
                print(f"    Ya estaba en DB: {item['name']}")
                ya_existian += 1
                
        except IntegrityError:
            db.rollback() # Limpiamos el error solo de este fondo
            print(f"    Duplicado/Error detectado en: {item['name']} (Saltando...)")
            errores += 1
        except Exception as e:
            db.rollback()
            print(f"   Error desconocido en {item['name']}: {e}")
            errores += 1
            
    db.close()
    print("="*50)
    print(f"RESULTADO FINAL: {registrados} Nuevos | {ya_existian} Viejos | {errores} Errores")

if __name__ == "__main__":
    populate()
