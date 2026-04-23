import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


import pandas as pd 
import json 
from pandas import json_normalize

import logging 
logging.getLogger().setLevel(logging.INFO)

#from pathlib import Path

#cwd = Path(__file__).resolve().parent
#root_dir = cwd.parent
#sub_folder = root_dir/"datalake"
#file = sub_folder/"api_data_2026-04-20_14-33-27.json"



def normalize_data(raw):
  """ Normalizes Api Json Response and Extracts Needed Fields only """ 
  
 # pd.set_option("display.max_rows", None)
 # pd.set_option("display.max_columns", None)

  df = pd.json_normalize(raw)
  
  
  selected = df[[
    "id",
    "object",
    "amount",
    "amount_received",
    "currency",
    "status",
    "created",
    "metadata.order_id",
    "metadata.product",
    "metadata.channel",
    "metadata.region",
    "metadata.customer_type"]]
    
  selected.columns = selected.columns.str.replace(".", "_") 
  
 # print(selected)
  
  records = selected.to_dict(orient="records")
  
  logging.info("API data normalized and Extracted Required fields")
  
  
  return records
