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
  dir_url = Path(__file__).resolve().parent
  root_dir = dir_url.parent
  sub_folder = root_dir/"datalake"
  sub_folder.mkdir(parents=True, exist_ok=True)
  
  file_path = sub_folder/"full_orders.json"
  
  bucket = "nathan-elt-buck"
  s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto"
    )
  
  
  df = pd.DataFrame(data)

    # save locally
  df.to_json(file_path, orient="records", lines=True)

    # upload to R2
  with open(file_path, "rb") as f:
    s3.put_object(
      Bucket=bucket,
      Key="multi-src/json/full_orders.json",
      Body=f,
      ContentType="application/json"
        )

  logging.info("JSON data loaded to R2 data lake")
    
    
    
    

  

  
  