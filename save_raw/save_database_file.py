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
    
    # Create a binary buffer
  buffer = io.BytesIO()
    
    # Write the CSV to the buffer
  df.to_csv(buffer, index=False, encoding='utf-8')
    
    # --- THE FIX ---
    # Move the pointer back to the start of the buffer so boto3 can read the content
  buffer.seek(0)
    # ----------------
    
  s3.put_object(
        Bucket=bucket,
        Key="multi-src/csv/production.csv",
        Body=buffer,  # boto3 will now read from the start of the buffer
        ContentType="text/csv"
    )
   
   
    
