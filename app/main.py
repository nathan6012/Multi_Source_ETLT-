import sys
import os
import asyncio
import logging
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from prefect import flow, task
from prefect.blocks.system import Secret
#from prefect.blocks.system import String 
import requests

# Connectors
from extract.api_connect import run_pipeline
from extract.database_connect import extract_from_db

# Save
from save_raw.save_database_file import save_raw_db_data

from save_raw.save_raw_api import save_raw_api_data

# Models
from validate.models import  ProductsData, PaymentData

# Validation
from validate.api_normalize import normalize_data
from validate.validate_api import validate_api


# Transform
from transform.transform_api_data import transform_api_data
from transform.transform_db import run_etl_pipeline

# Load
from load_data.load_api import load_api_data_database
from load_data.load_database import big_Query_client


from utils.file_sys import files_management


logging.basicConfig(level=logging.INFO)

async def send_slack(message: str):
  webhook = await Secret.load("slack-webhook")
  webhook = webhook.get()

  if webhook:
    requests.post(webhook, json={"text": message})
        

@task 
def files_management_task(endpoint, access_key, secret_key):
  return files_management





# ------------------- API TASKS -------------------

@task
async def api_extract_task(api_key):
  return await run_pipeline(api_key)

@task
def save_raw_api_task(x, endpoint, access_key, secret_key):
  return save_raw_api_data(x, endpoint, access_key, secret_key)

@task
def normalize_api_data_task(k):
  return normalize_data(k)

@task
def validate_api_data_task(k, model):
  return validate_api(k, model)

@task
def transform_api_data_task(q, l):
  return transform_api_data(q, l)

@task
async def load_api_data_database_task(data, db_url):
  return await load_api_data_database(data, db_url)


# ------------------- DB TASKS -------------------

@task(retries=3)
async def extract_from_db_task(db_url):
  return await extract_from_db(db_url)

@task
def save_raw_db_data_task(y, endpoint, access_key, secret_key):
  return save_raw_db_data(y, endpoint, access_key, secret_key)

@task
def transform_database_data_task():
  return run_etl_pipeline()

@task
def load_db_data_big_query_task(data, gcp_creds):
  return big_Query_client(data, gcp_creds)


# -- -------------------


# ------------------- FLOWS -------------------

@flow(log_prints=True)
async def main_flow_api(api_key, db_url, endpoint, access_key, secret_key):
  """ API data Extract ETL Flow """
  
  raw_api = await api_extract_task(api_key)
  print(len(raw_api))
  

  if raw_api:
    save_raw_api_task(raw_api, endpoint, access_key, secret_key)

    normal = normalize_api_data_task(raw_api)
    print(len(normal))

    clean, unclean = validate_api_data_task(normal, PaymentData)

    records = transform_api_data_task(clean, unclean)
    print(len(records))
    

    await load_api_data_database_task(records, db_url)
  else:
    print("No API Data")

#______
#________




@flow(log_prints=True)
async def main_flow_db(db_url, gcp_creds, endpoint, access_key, secret_key):
  """  postgres To Big Query Migration """ 
  
  print("Data To Big query Migration Etl logic")
  db_data = await extract_from_db_task(db_url)
  print(len(db_data))

  save_raw_db_data_task(db_data, endpoint, access_key, secret_key)
  
# change validation logic  
  records  = validate_database_data_task(db_data, ProductsData)

  data =  load_db_data_big_query_task(records, gcp_creds)
  
  transform_database_data_task()
  
  
  



# ------------------- ORCHESTRATOR -------------------

@flow(name="etl_orchestrator", log_prints=True)
async def etl_orchestrator():

    # Load secrets
  db_url = (await Secret.load("database-url")).get()
  
  api_key = (await Secret.load("api-key")).get()
  
  gcp_creds = (await Secret.load("gcp-credentials")).get()

  endpoint = (await Secret.load("aws-endpoint-url")).get()
  
  access_key = (await Secret.load("aws-access-key-id")).get()
  
  secret_key = (await Secret.load("aws-secret-access-key")).get()

  try:
    await main_flow_api(api_key, db_url, endpoint, access_key, secret_key)

    await main_flow_db(db_url, gcp_creds, endpoint, access_key, secret_key)
    
    files_management_task(endpoint, access_key, secret_key)
    
    

    await send_slack(
            "✅ ETL SUCCESS: etl_orchestrator\n"
            "────────────────────────────\n"
            "📥 API pipeline: completed\n"
            "🗄️ DB pipeline: completed\n"
            "📊 Excel pipeline: completed\n"
            "🚀 All data loaded successfully"
        )

  except Exception as e:
    await send_slack(
            "❌ ETL FAILED: etl_orchestrator\n"
            "────────────────────────────\n"
            "⚠️ One or more pipelines failed\n"
            "📥 Check API / DB / Excel stages in Prefect logs\n"
        )
    raise e  
  
  
  
