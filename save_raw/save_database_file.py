import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
import logging 
from datetime import datetime
import pandas as pd

import boto3
import io
from io import StringIO



logging.getLogger().setLevel(logging.INFO)


def save_raw_db_data(data, endpoint, access_key, secret_key):
  bucket = "nathan-elt-buck"
  s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto"
    )
    
  df = pd.DataFrame(data)
  logging.info(f"Uploading {len(df)} rows...") # If this is 0, the bucket stays empty.

# ... (inside your function)

  buffer = io.BytesIO()
  df.to_csv(buffer, index=False, encoding='utf-8')

# Use .getvalue() to send the actual data content, not the stream object
  s3.put_object(
    Bucket=bucket,
    Key="multi-src/csv/production.csv",
    Body=buffer.getvalue(),  # This sends the raw bytes directly
    ContentType="text/csv")
    

   
    
