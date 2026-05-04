import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json

from pathlib import Path

import logging

from datetime import datetime

import pandas as pd

import boto3
from io import StringIO


logging.getLogger().setLevel(logging.INFO)

def save_raw_api_data(data,endpoint,access_key,secret_key):
  """Just saves our data to json for any emergency can be persisted to S3/R2"""
  
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
  df.to_json(buffer, orient="records", lines=True)
  buffer.seek(0)
  s3.put_object(
        Bucket=bucket,
        Key="multi-src/json/payments.json",
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="application/json"
    )
  logging.info("File loaded to Datalake")  




  
  
 
