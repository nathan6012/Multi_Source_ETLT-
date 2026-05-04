import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
import logging 
from datetime import datetime
from pathlib import Path
import pandas as pd

import boto3



logging.getLogger().setLevel(logging.INFO)


def save_raw_db_data(data, endpoint, access_key, secret_key):
  
  dir_url = Path(__file__).resolve().parent
  root_dir = dir_url.parent
  sub_folder = root_dir/"datalake"
  sub_folder.mkdir(parents=True, exist_ok=True)
  
  bucket = "nathan-elt-buck"
  
  s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto"
    )
    
  

  file_path = sub_folder/"production.csv"
  df = pd.DataFrame(data)

  df.to_csv(file_path, index=False, encoding='utf-8')
  
  with open(file_path, "rb") as f:
    s3.put_object(
      Bucket=bucket,
      Key="multi-src/csv/production.csv",
      Body=f,
      ContentType="text/csv")
      
  logging.info("Data Loaded To R2")          