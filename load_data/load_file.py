import sys
import os
import logging 

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from sqlalchemy.ext.asyncio import create_async_engine
import asyncio
from sqlalchemy import text,select,update 

from sqlalchemy import Text,Float

from sqlalchemy.dialects.postgresql import insert as upsert 
from sqlalchemy import UniqueConstraint 
from sqlalchemy import(Table,Column,Integer,String,MetaData,Index,Numeric,func)
from sqlalchemy import DateTime
from dotenv import load_dotenv


logging.basicConfig(level=logging.INFO)
load_dotenv()


async def load_file_data_database(data):
  """Loads The Transformed Data to Posgres Db"""
  
  db_url = os.getenv("DATABASE_URL").strip()
  
  engine = create_async_engine(db_url,echo=False,
  pool_pre_ping=True,
  pool_size=5,
  max_overflow=10,)
  
  metadata = MetaData()
  
  full_orders = Table(
    "full_orders",
    metadata,

    Column("order_id", Integer, primary_key=True),
    Column("customer", String),
    Column("region", String),
    Column("product", String),
    Column("quantity", Integer),
    Column("unit_price", Integer),
    Column("cost_per_unit", Float),
    Column("order_date", DateTime),)
  
  async with engine.begin() as conn:
    await conn.run_sync(metadata.create_all)  


  async with engine.begin() as conn:
    stmt = upsert(full_orders).values(data)

    stmt = stmt.on_conflict_do_update(
    index_elements=["order_id"],
    set_={
    "customer": stmt.excluded.customer,
     "region": stmt.excluded.region,
     "product": stmt.excluded.product,
    "quantity": stmt.excluded.quantity,
     "unit_price": stmt.excluded.unit_price,
    "cost_per_unit": stmt.excluded.cost_per_unit,
    "order_date": stmt.excluded.order_date,
    },)

    await conn.execute(stmt)  
  
  logging.info("Execel Data into Db/updated")  
  await engine.dispose()
  
