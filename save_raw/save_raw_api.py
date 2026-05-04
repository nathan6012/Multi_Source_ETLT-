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


def save_raw_api_data(data, endpoint, access_key, secret_key):
  """Save API data directly to R2 (no local files)"""

  s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto"
    )

  bucket = "nathan-elt-buck"

  df = pd.DataFrame(data)

    # convert directly to JSON string in memory
  json_str = df.to_json(orient="records", lines=True)

  s3.put_object(
        Bucket=bucket,
        Key="multi-src/json/payments.json",
        Body=json_str.encode("utf-8"),
        ContentType="application/json"
    )

  logging.info("Payments JSON uploaded to R2 data lake")
  