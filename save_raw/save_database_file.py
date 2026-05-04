import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
import logging 
from datetime import datetime
import pandas as pd

import boto3
from io import StringIO


logging.getLogger().setLevel(logging.INFO)

def save_raw_db_data(data,endpoint, access_key, secret_key):
  
  """Just saves our data to csv for any emergency can be persisted to S3/R2"""
  
  bucket = "nathan-elt-buck"
  s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto"
    )
  df = pd.DataFrame(data)
  
  buffer = StringIO()
  df.to_csv(buffer, index=False)  # CSV instead o
  buffer.seek(0)
  
  s3.put_object(
        Bucket=bucket,
        Key="multi-src/csv/production.csv",  # CSV
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv"
    )
  try:
    s3.head_object(Bucket=bucket, Key=key)
    logging.info("✅ Upload confirmed in R2")
  except Exception as e:
    logging.warning("❌ Upload not found in R2:", e)  

  
  
  

