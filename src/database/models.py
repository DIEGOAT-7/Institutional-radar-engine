# src/database/models.py
from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey, DateTime, Text, Boolean
from sqlalchemy.orm import DeclarativeBase, relationship
from datetime import datetime

class Base(DeclarativeBase):
    pass

# 1. LOS FONDOS (Inversores)
class Fund(Base):
    __tablename__ = 'funds'

    id = Column(Integer, primary_key=True, autoincrement=True)
    cik = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=False)
    strategy = Column(String, nullable=True) # Ej: Quant, Value, Activist
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relaciones
    holdings = relationship("Holding", back_populates="fund")
    derivatives = relationship("DerivativePosition", back_populates="fund")

    def __repr__(self):
        return f"<Fund(name='{self.name}', cik='{self.cik}')>"

# 2. LAS EMPRESAS (Activos)
class Company(Base):
    __tablename__ = 'companies'

    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Identificadores
    name = Column(String, nullable=False)
    ticker = Column(String, nullable=True) # Símbolo bursátil (TSLA)
    cusip = Column(String, unique=True, nullable=False) # ID Oficial
    cik = Column(String, nullable=True) # ID en la SEC (si logramos cruzarlo)
    
    # Clasificación (Para rellenar NULLs)
    sector = Column(String, nullable=True)   # Ej: Technology
    industry = Column(String, nullable=True) # Ej: Auto Manufacturers
    country = Column(String, nullable=True)  # Ej: USA, China
    
    # Datos extra para la "Terminal"
    description = Column(Text, nullable=True) # Descripción breve de qué hace la empresa
    is_sp500 = Column(Boolean, default=False) # ¿Está en el índice principal?

    # Relaciones
    prices = relationship("StockPrice", back_populates="company")
    held_by = relationship("Holding", back_populates="company")
    
    # Relación compleja: Supply Chain (Proveedores y Clientes)
    # Definimos las relaciones salientes (yo proveo a...) y entrantes (me proveen...)
    suppliers = relationship(
        "SupplyChainRelationship",
        foreign_keys="[SupplyChainRelationship.customer_id]",
        back_populates="customer"
    )
    customers = relationship(
        "SupplyChainRelationship",
        foreign_keys="[SupplyChainRelationship.supplier_id]",
        back_populates="supplier"
    )
    
    key_people = relationship("KeyExecutive", back_populates="company")
    derivative_positions = relationship("DerivativePosition", back_populates="company")

    def __repr__(self):
        return f"<Company(ticker='{self.ticker}', name='{self.name}')>"

# 3. HOLDINGS (Acciones Comunes)
class Holding(Base):
    __tablename__ = 'holdings'

    id = Column(Integer, primary_key=True, autoincrement=True)
    fund_id = Column(Integer, ForeignKey('funds.id'), nullable=False)
    company_id = Column(Integer, ForeignKey('companies.id'), nullable=False)
    report_date = Column(Date, nullable=False)
    shares = Column(Float, nullable=False)
    value = Column(Float, nullable=False) # Valor reportado en USD
    
    fund = relationship("Fund", back_populates="holdings")
    company = relationship("Company", back_populates="held_by")

# --- 4. PRECIOS (Series de Tiempo) ---
class StockPrice(Base):
    __tablename__ = 'stock_prices'

    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey('companies.id'), nullable=False)
    date = Column(Date, nullable=False)
    close_price = Column(Float, nullable=False)
    volume = Column(Float, nullable=True)

    company = relationship("Company", back_populates="prices")


# 5. DERIVADOS (Puts & Calls) 
# Aquí detectamos si un fondo apuesta en contra o se cubre
class DerivativePosition(Base):
    __tablename__ = 'derivatives'

    id = Column(Integer, primary_key=True, autoincrement=True)
    fund_id = Column(Integer, ForeignKey('funds.id'), nullable=False)
    company_id = Column(Integer, ForeignKey('companies.id'), nullable=False)
    report_date = Column(Date, nullable=False)
    
    derivative_type = Column(String, nullable=False) # 'PUT' o 'CALL'
    shares_underlying = Column(Float, nullable=False) # Cantidad de acciones equivalentes
    value = Column(Float, nullable=False) # Valor nocional
    
    fund = relationship("Fund", back_populates="derivatives")
    company = relationship("Company", back_populates="derivative_positions")

# 6. CADENA DE SUMINISTRO (El Grafo Industrial)
# Tabla de relación Muchos-a-Muchos entre compañías
class SupplyChainRelationship(Base):
    __tablename__ = 'supply_chain'

    id = Column(Integer, primary_key=True, autoincrement=True)
    
    supplier_id = Column(Integer, ForeignKey('companies.id'), nullable=False)
    customer_id = Column(Integer, ForeignKey('companies.id'), nullable=False)
    
    component_type = Column(String, nullable=True) # Ej: "Lithium Batteries", "Semiconductors"
    revenue_dependency = Column(Float, nullable=True) # % de ingresos que depende de este cliente (Riesgo)
    
    supplier = relationship("Company", foreign_keys=[supplier_id], back_populates="customers")
    customer = relationship("Company", foreign_keys=[customer_id], back_populates="suppliers")

# 7. EJECUTIVOS Y JUNTA DIRECTIVA (El Grafo Humano)
class KeyExecutive(Base):
    __tablename__ = 'key_executives'

    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey('companies.id'), nullable=False)
    
    name = Column(String, nullable=False) # Ej: "Elon Musk"
    role = Column(String, nullable=False) # Ej: "CEO", "CFO", "Board Member"
    is_insider = Column(Boolean, default=False) # ¿Es considerado 'Insider' por la SEC?
    
    company = relationship("Company", back_populates="key_people")