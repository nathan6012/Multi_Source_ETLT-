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
  
  s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto"
    )

  bucket = "nathan-elt-buck"

  df = pd.DataFrame(data)

    # convert directly to bytes (no StringIO)
  csv_bytes = df.to_csv(index=False).encode("utf-8")

  s3.put_object(
        Bucket=bucket,
        Key="multi-src/csv/production.csv",
        Body=csv_bytes,
        ContentType="text/csv"
    )

  logging.info("Upload successful")