import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))



from sqlalchemy.ext.asyncio import create_async_engine, AsyncConnection
from sqlalchemy import MetaData,inspect
from sqlalchemy import select,Table
import asyncio 
from dotenv import load_dotenv
import logging 
#Logging 
logging.getLogger().setLevel(logging.INFO)
#Helper Load 

load_dotenv()


# Inspector Object 
async def get_table(conn: AsyncConnection, table_name: str, metadata: MetaData) -> Table | None:
  """Return a Table object if it exists in the database, else None."""
  
  tables = await conn.run_sync(lambda sync_conn: inspect(sync_conn).get_table_names())
  
  if table_name in tables:
    return await conn.run_sync(lambda sync_conn:Table(table_name, metadata, autoload_with=sync_conn))
  return None



async def fetch_table_data(conn: AsyncConnection, table: Table) -> list[dict]:
  """Fetch all rows from a table and return as list of dicts."""
  
  result = await conn.execute(select(table))
  rows = result.fetchall()
  return [dict(row._mapping) for row in rows]






async def extract_from_db(db_url):
  """Connect to DB and extract data"""

  db_url = db_url.strip()

  engine = create_async_engine(
        db_url,
        echo=False,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
    )

  metadata = MetaData()

  async with engine.connect() as conn:
    production_analytics_fusion = await get_table(
            conn, "production_analytics_fusion", metadata
        )

    production_analytics_fusion_data = (
            await fetch_table_data(conn, production_analytics_fusion)
            if production_analytics_fusion is not None
            else []
        )


  await engine.dispose()

  logging.info(
        f"Data Extracted From Database: {len(production_analytics_fusion_data)}"
    )

  return production_analytics_fusion_data 
# Remove after test   
  
  
  
  
  
  
  
  
  
  



