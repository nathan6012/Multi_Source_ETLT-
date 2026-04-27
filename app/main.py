import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from prefect import flow, task
import asyncio
import logging 

# Import all the other Files from Folders Here

# Connectors
from extract.api_connect import run_pipeline

from extract.database_connect import extract_from_db

from extract.file_connect import extract_xl_file



#Save 
from save_raw.save_database_file import save_raw_db_data

from save_raw.save_file_file import save_raw_execel_data

from save_raw.save_raw_api import save_raw_api_data


#models 
from validate.models import SalesData,ProductsData,PaymentData



# validation 
from validate.api_normalize import normalize_data


from validate.validate_api import validate_api

from validate.validate_database import validate_database_data


from validate.validate_file import validate_file_data



#Transform
from transform.transform_api_data import transform_api_data

from transform.transform_db import transform_database_data

from transform.transform_file import transform_excel_data



# Load_data imports 
from load_data.load_api import load_api_data_database

from load_data.load_database import big_Query_client

from load_data.load_file import load_file_data_database

from app.file_sys import files_management





logging.basicConfig(level=logging.INFO)


#SAAS API 

@task(name="API_Extract")
async def api_extract_task():
  x = await run_pipeline()
  return x
  
@task(name =",Save_to_dlake")
def save_raw_api_task(x):
  return save_raw_api_data(x)
  
@task(name="normalize_api_data")
def normalize_api_data_task(k):
  return normalize_data(k)

@task(name="validate_api_data")
def validate_api_data_task(k,model):
  return validate_api(k,model)
  
@task(name="transform_api_data")
def transform_api_data_task(q,l):
  return transform_api_data(q,l)
  
@task(name="load_api_data_database")
async def load_api_data_database_task(z):
  return await load_api_data_database(z)




# DataBase 

@task(name="extract_from_db",retries = 3)
async def extract_from_db_task():
  y = await extract_from_db()
  return y 

@task(name="save_db_raw_csv")
def save_raw_db_data_task(y):
  return save_raw_db_data(y)

@task(name="validate_database_data")
def validate_database_data_task(y,model):
  return validate_database_data(y,model)
  
@task(name="transform_database_data")  
def transform_database_data_task(f,g):
  return transform_database_data(f,g)
  
@task(name = "load_db_data_Big_Query")
def load_db_data_big_query_task(m):
  return big_Query_client(m)
  
  
# Excel Files 
@task(name="Excel Extract")
def extract_xl_file_task():
  return extract_xl_file()
 
@task(name="save_raw_execel_data")
def save_raw_execel_data_task(d):
  return save_raw_execel_data(d)
  
@task(name="validate_file_data")
def validate_file_data_task(d,model):
  return validate_file_data(d,model)

@task(name="transform_excel_data")
def transform_excel_data_task(t,r):
  return transform_excel_data(t,r)
    
@task(name="load_file_data_database")
async def load_file_data_database_task(h):
  await load_file_data_database(h)

  # Check Data Lake files 
@task(name="Files files_management")
def files_management_task():
  return files_management()
  



#Apis main 
@flow(name="API_flow", log_prints=True)
async def main_flow_api():
  " Main Prefect  Flow That Combine Everything "
  raw_api = await api_extract_task()
  
  if raw_api:
    save_raw_api_task(raw_api)
    
    normal = normalize_api_data_task(raw_api)
  
    clean,unclean = validate_api_data_task(normal,PaymentData)
    
    records1 = transform_api_data_task(clean,unclean)
  
    await load_api_data_database_task(records1)
  else:
    print("No New API Data To Extract")
    
  

# Databases 
flow(name="DB_flow", log_prints=True)
async def main_flow_db():
  """ From DB To DB/Warehouse """ 
  
  db_data = await extract_from_db_task()
  
  save_raw_db_data_task(db_data)
  
  clean1,unclean1 = validate_database_data_task(db_data,ProductsData)
  
  records0 = transform_database_data_task(clean1,unclean1)
  
  load_db_data_big_query_task(records0)
  




#Excel 
flow(name="excel_flow", log_prints=True)
async def main_flow_excel():
  """from Excel to Database/cv/warehouse"""
  # Excel files
  xl_data = extract_xl_file_task()
  if xl_data:
    save_raw_execel_data_task(xl_data)
    
    clean0,unclean0 = validate_file_data_task(xl_data,SalesData)
  
    records2 = transform_excel_data_task(clean0,unclean0)
    await load_file_data_database_task(records2)
  else:
    print("No Excel Files scanned")
    
  
 
@flow(name="etl_orchestrator")
async def etl_orchestrator():
  """ Master Main of mains """

  await main_flow_api()
  await main_flow_db()
  await main_flow_excel()
  files_management_task()
  print("Services for local files")
 
  
if __name__ =="__main__":
  asyncio.run(etl_orchestrator())
  
  
  
  
  
  