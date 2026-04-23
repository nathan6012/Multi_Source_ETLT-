import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
from pathlib import Path
import logging 
from datetime import datetime
import pandas as pd

logging.getLogger().setLevel(logging.INFO)

def save_raw_execel_data(data):
  """Just saves our data to json for any emergency can be persisted to S3/R2"""
  
  cwd = Path(__file__).resolve().parent
  root_dir = cwd.parent
  sub_folder = root_dir/"datalake"
  sub_folder.mkdir(parents=True, exist_ok=True)
  
  ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
  filename = f"excel_{ts}.json"
  
  
  file = sub_folder/ filename
  
  df = pd.DataFrame(data)
  raw_data = df.to_json(file,orient="records", indent=5)
 
  logging.info("File uploaded to data_lake as json")

  
  
  