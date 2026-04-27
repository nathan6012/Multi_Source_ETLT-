import sys
import os
import logging 

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from google.cloud import bigquery
from pathlib import Path
import pandas as pd
import os

logging.basicConfig(level=logging.INFO)


# For Postgres to Big Query in BIG ETL etl0 


def big_Query_client(data):
  """ Loads CSV transformed Data to Big Query Data Warehosue """ 
  cwd = Path(__file__).resolve().parent
  root_dir = cwd.parent
  sub_folder = root_dir/"datalake"
  sub_folder.mkdir(parents=True, exist_ok=True)
  
 # export GOOGLE_APPLICATION_CREDENTIALS=credentials/calm-sky-419511-38ce7f0f9910.json
  
  cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
  if not cred_path:
    raise ValueError("GOOGLE_APPLICATION_CREDENTIALS not set!")


  client = bigquery.Client()
  
  
  df = pd.DataFrame(data)
  
  csv_path = sub_folder / "production.csv"
  df.to_csv(csv_path, index=False)
  
  logging.info(f"Data staged at: {csv_path}")


  print("CSV loaded successfully")

  
  table_id = "calm-sky-419511.Nathanetl_sales.you.production"

  job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
  
    

  job = client.load_table_from_dataframe(
    df,
    table_id,
    job_config=job_config )
    

  job.result()

  logging.info(f" CSV dataset successfully loaded into BigQuery table: {table_id}")




