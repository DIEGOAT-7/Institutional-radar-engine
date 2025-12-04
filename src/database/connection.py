# src/database/connection.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pathlib import Path
from .models import Base

# CONFIGURACIÓN DE RUTAS
# Esto asegura que la base de datos se cree en la carpeta 'data'
BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "data" / "institutional_radar.db"
DATABASE_URL = f"sqlite:///{DB_PATH}"

# CREACIÓN DEL MOTOR
# echo=False para que no llene la consola de texto, ponlo en True si quieres ver el SQL crudo
engine = create_engine(DATABASE_URL, echo=False)

# Fábrica de sesiones (usaremos esto cada vez que queramos guardar/leer datos)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    """Crea las tablas en la base de datos si no existen."""
    print(f"Conectando a base de datos en: {DB_PATH}")
    # Esta línea mágica convierte las clases de Python en tablas SQL reales
    Base.metadata.create_all(bind=engine)
    print("✅ Tablas creadas exitosamente (Schema Loaded).")

def get_db():
    """Dependencia para obtener una sesión de base de datos."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()