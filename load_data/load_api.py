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
from sqlalchemy import(Table,Column,Integer,String,MetaData,Index,Numeric)
from sqlalchemy import DateTime
from dotenv import load_dotenv


logging.basicConfig(level=logging.INFO)
load_dotenv()



async def load_api_data_database(data):
  """Loads The Transformed Data to Posgres Db"""
  
  db_url = os.getenv("DATABASE_URL").strip()
  
  engine = create_async_engine(db_url,echo=False,
  pool_pre_ping=True,
  pool_size=5,
  max_overflow=10,)
  
  
  metadata = MetaData()
  
  payments = Table(
    "payments",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),

    Column("external_id",String,unique=True),
    Column("amount", Integer),
    Column("amount_received", Integer),
    Column("currency", String),
    Column("status", String),
    Column("created", Integer),
    Column("order_id", String),
    Column("product", String),
    Column("channel", String),
    Column("region", String),
    Column("customer_type", String),)

  Index("idx_payments", payments.c.id)


  async with engine.begin() as conn:
    await conn.run_sync(metadata.create_all)


  async with engine.begin() as conn:
    stmt = upsert(payments).values(data)

    stmt = stmt.on_conflict_do_update(
        index_elements=["external_id"],  
        set_={
      "id": stmt.excluded.id,
     "amount": stmt.excluded.amount,
     "amount_received": stmt.excluded.amount_received,
     "currency": stmt.excluded.currency,
     "status": stmt.excluded.status,
     "created": stmt.excluded.created,
     "order_id": stmt.excluded.order_id,
     "product": stmt.excluded.product,
     "channel": stmt.excluded.channel,
     "region": stmt.excluded.region,
     "customer_type": stmt.excluded.customer_type,
        },
    )

    await conn.execute(stmt)
  logging.info("Data in updated to Db")  
    



  await asyncio.sleep(0.3)
  
  await engine.dispose()