from src.database.connection import init_db

if __name__ == "__main__":
    print("Iniciando configuración de Base de Datos SQL...")
    init_db()
    print("Sistema listo, Archivo .db generado en la carpeta 'data'.")
