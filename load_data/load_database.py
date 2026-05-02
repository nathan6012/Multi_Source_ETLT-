import sys
import os
import logging 

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from google.cloud import bigquery
from google.oauth2 import service_account
from pathlib import Path
import pandas as pd

logging.basicConfig(level=logging.INFO)


# For Postgres to Big Query in BIG ETL etl0 



def big_Query_client(data, gcp_creds):
  """Loads data to BigQuery"""

  if not data:
    raise ValueError("❌ Empty dataset - skipping BigQuery load")

  credentials = service_account.Credentials.from_service_account_info(gcp_creds)

  client = bigquery.Client(
        credentials=credentials,
        project=gcp_creds["project_id"]
    )

  logging.info("Running BigQuery Load")

  df = pd.DataFrame(data)
  logging.info(f"data schemas: {df.dtypes}")

    # optional temp storage (you can remove later)
  cwd = Path(__file__).resolve().parent
  root_dir = cwd.parent
  sub_folder = root_dir / "local"
  sub_folder.mkdir(parents=True, exist_ok=True)

  csv_path = sub_folder / "production.csv"
  df.to_csv(csv_path, index=False)

  logging.info(f"Data staged at: {csv_path}")

  table_id = "calm-sky-419511.Nathanelt_sales.production"

  job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

  job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config
    )

  job.result()

  logging.info(f"Loaded into BigQuery table: {table_id}")

