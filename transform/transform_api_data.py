import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


import pandas as pd
from pandas import json_normalize
#import re 
import json
import logging 
#from pathlib import Path 

logging.getLogger().setLevel(logging.INFO)


# takes two inputs 
def transform_api_data(clean_a, unclean_b):
  """Transform / clean Data for Staging"""

  clean = pd.json_normalize(clean_a)
  unclean = pd.json_normalize(unclean_b)

    # Replace NaNs properly
  clean = clean.where(pd.notnull(clean), None)

    # Drop unnecessary columns safely
  clean = clean.drop(columns=["idx"], errors="ignore")

    # Remove prefix
  clean.columns = clean.columns.str.replace("data.", "", regex=True)

    # Enforce dtypes BEFORE any formatting
  clean = clean.astype({
        "amount": "int32",
        "amount_received": "int32",
        "created": "int32"
    }, errors="ignore")

    # Remove meta prefix
  clean.columns = clean.columns.str.replace("^meta", "", regex=True)
  
  clean = clean.rename(columns={"id": "external_id"})

    # Drop id safely
  #clean = clean.drop(columns=["id"], errors="ignore")
  
  clean = clean.drop(columns=["object"],errors="ignore")
  
  

  
  print(clean.dtypes)
  
  clean = clean.reset_index(drop=True)
  
  dataset = clean.to_dict(orient="records")

  logging.info(f"API Data Has Been Clean: {len(dataset)} records")

  return dataset
  
  

  
  
  