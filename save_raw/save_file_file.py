import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
import logging 
from datetime import datetime
import pandas as pd
from pathlib import Path
import boto3

# from io import BytesIO for streaming in memory rather than saving first 




logging.getLogger().setLevel(logging.INFO)

def save_raw_execel_data(data,endpoint, access_key, secret_key):
  """Just saves our data to json for any emergency can be persisted to S3/R2"""
    
  s3 = boto3.client(
   "s3",
  endpoint_url=endpoint,
  aws_access_key_id=access_key,
  aws_secret_access_key=secret_key,
  region_name="auto"
    )

  bucket = "nathan-elt-buck"

  df = pd.DataFrame(data)

    # convert to JSON string in memory (no file)
  json_str = df.to_json(orient="records", lines=True)

  s3.put_object(
        Bucket=bucket,
        Key="multi-src/json/full_orders.json",
        Body=json_str.encode("utf-8"),
        ContentType="application/json"
    )

  logging.info("JSON uploaded to R2")  

  

  
  