import sys 
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import time 
import logging 
from datetime import datetime, timezone, timedelta
import boto3
logging.basicConfig(level=logging.INFO)


def files_management(endpoint, access_key, secret_key):
  """ Tracks and deletes System Files(in R2 datalake) after 5 days """
  
  bucket = "nathan-elt-buck"
  
  s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto"
    )

    # List all objects
  response = s3.list_objects_v2(Bucket=bucket)

  if "Contents" not in response:
    logging.info("Bucket is already empty")
    return

  objects = [{"Key": obj["Key"]} for obj in response["Contents"]]

    # S3 delete supports batch delete (max 1000 objects)
  s3.delete_objects(
        Bucket=bucket,
        Delete={"Objects": objects}
    )

  logging.info(f"Deleted {len(objects)} objects from bucket {bucket}")
  
  
  