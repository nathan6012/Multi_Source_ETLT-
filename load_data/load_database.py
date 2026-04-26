import sys
import os
import logging 

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from sqlalchemy.ext.asyncio import create_async_engine
import asyncio
from sqlalchemy import text,select
from sqlalchemy import Text,Float

from sqlalchemy.dialects.postgresql import insert as upsert 
from sqlalchemy import UniqueConstraint 
from sqlalchemy import(Table,Column,Integer,String,MetaData,Index,Numeric,func)
from sqlalchemy import DateTime
from dotenv import load_dotenv


logging.basicConfig(level=logging.INFO)
load_dotenv()


# To change to Big Query 
async def load_db_data_database(data):
  """Loads The Transformed Data to Posgres Db"""
  
  db_url = os.getenv("DATABASE_URL").strip()
  
  engine = create_async_engine(db_url,echo=False,
  pool_pre_ping=True,
  pool_size=5,
  max_overflow=10,)
  
  
  metadata = MetaData()
  
  production = Table(
    "production",
    metadata,
    Column("production_id", Integer, primary_key=True),
    Column("product_name", String),
    Column("category", String),
    Column("region", String, nullable=True),
    Column("production_date", DateTime),
    Column("units_produced", Integer),
    Column("defective_units", Integer),
    Column("revenue", Numeric(18, 2)),
    Column("cost", Numeric(18, 2)),
    Column("analyst_score", Numeric(10, 2), nullable=True),
    # NEW: auto-update timestamp
    Column("updated_on", DateTime, server_default=func.now(), onupdate=func.now()),)
 
  
  async with engine.begin() as conn:
    await conn.run_sync(metadata.create_all)
    
    
  
  async with engine.begin() as conn:
    stmt = upsert(production).values(data)

    stmt = stmt.on_conflict_do_update(
        index_elements=["production_id"],  # conflict key
        set_={
            "product_name": stmt.excluded.product_name,
            "category": stmt.excluded.category,
            "region": stmt.excluded.region,
            "production_date": stmt.excluded.production_date,
            "units_produced": stmt.excluded.units_produced,
            "defective_units": stmt.excluded.defective_units,
            "revenue": stmt.excluded.revenue,
            "cost": stmt.excluded.cost,
            "analyst_score": stmt.excluded.analyst_score,

            # always refresh timestamp on update
            "updated_on": func.now(),
        },
    )
  
   

    await conn.execute(stmt)  
    
  logging.info("Data update in Database from DB src")   
  await engine.dispose()  