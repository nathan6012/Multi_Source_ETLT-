import sys
import os
import asyncio
import logging
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from prefect import flow, task
from prefect.blocks.system import Secret

# Connectors
from extract.api_connect import run_pipeline
from extract.database_connect import extract_from_db
from extract.file_connect import extract_xl_file

# Save
from save_raw.save_database_file import save_raw_db_data
from save_raw.save_file_file import save_raw_execel_data
from save_raw.save_raw_api import save_raw_api_data

# Models
from validate.models import SalesData, ProductsData, PaymentData

# Validation
from validate.api_normalize import normalize_data
from validate.validate_api import validate_api
from validate.validate_database import validate_database_data
from validate.validate_file import validate_file_data

# Transform
from transform.transform_api_data import transform_api_data
from transform.transform_db import transform_database_data
from transform.transform_file import transform_excel_data

# Load
from load_data.load_api import load_api_data_database
from load_data.load_database import big_Query_client
from load_data.load_file import load_file_data_database

from app.file_sys import files_management

logging.basicConfig(level=logging.INFO)

# ------------------- API TASKS -------------------

@task
async def api_extract_task(api_key):
  return await run_pipeline(api_key)

@task
def save_raw_api_task(x):
  return save_raw_api_data(x)

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
def save_raw_db_data_task(y):
  return save_raw_db_data(y)

@task
def validate_database_data_task(y, model):
  return validate_database_data(y, model)

@task
def transform_database_data_task(f, g):
  return transform_database_data(f, g)

@task
def load_db_data_big_query_task(data, gcp_creds):
  return big_Query_client(data, gcp_creds)


# ------------------- FILE TASKS -------------------

@task
def extract_xl_file_task():
  return extract_xl_file()

@task
def save_raw_execel_data_task(d):
  return save_raw_execel_data(d)

@task
def validate_file_data_task(d, model):
  return validate_file_data(d, model)

@task
def transform_excel_data_task(t, r):
  return transform_excel_data(t, r)

@task
async def load_file_data_database_task(data, db_url):
  return await load_file_data_database(data, db_url)

@task
def files_management_task():
  return files_management()


# ------------------- FLOWS -------------------

@flow
async def main_flow_api(api_key, db_url):
  raw_api = await api_extract_task(api_key)

  if raw_api:
    save_raw_api_task(raw_api)

    normal = normalize_api_data_task(raw_api)

    clean, unclean = validate_api_data_task(normal, PaymentData)

    records = transform_api_data_task(clean, unclean)

    await load_api_data_database_task(records, db_url)
  else:
    print("No API Data")


@flow
async def main_flow_db(db_url, gcp_creds):
  db_data = await extract_from_db_task(db_url)

  save_raw_db_data_task(db_data)

  clean, unclean = validate_database_data_task(db_data, ProductsData)

  records = transform_database_data_task(clean, unclean)

  load_db_data_big_query_task(records, gcp_creds)


@flow
async def main_flow_excel(db_url):
  xl_data = extract_xl_file_task()

  if xl_data:
    save_raw_execel_data_task(xl_data)

    clean, unclean = validate_file_data_task(xl_data, SalesData)

    records = transform_excel_data_task(clean, unclean)

    await load_file_data_database_task(records, db_url)
  else:
    print("No Excel Data")


# ------------------- ORCHESTRATOR -------------------

@flow(name="etl_orchestrator")
async def etl_orchestrator():

    #LOAD SECRETS CORRECTLY
  db_url = (await Secret.load("database-url")).get()
  api_key = (await Secret.load("api-key")).get()
  
  gcp_creds = (await Secret.load("gcp-credentials")).get()

    # ruN FLOWS
  await main_flow_api(api_key, db_url)
  await main_flow_db(db_url, gcp_creds)
  await main_flow_excel(db_url)

  files_management_task()
  
  
  
  
  
  
  
