import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
from pathlib import Path
import logging 
from datetime import datetime
import pandas as pd

logging.getLogger().setLevel(logging.INFO)

def save_raw_db_data(data):
  """Just saves our data to csv for any emergency can be persisted to S3/R2"""
  
  cwd = Path(__file__).resolve().parent
  root_dir = cwd.parent
  sub_folder = root_dir/"datalake"

  sub_folder.mkdir(parents=True, exist_ok=True)
  
  # Data tracking for Backfils 
  ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
  
  filename = f"database_data_{ts}.csv"
  
  
  file = sub_folder/ filename
  
  df = pd.DataFrame(data)
  raw_data = df.to_csv(file,index=False)
  
  logging.info("File uploaded to data_lake as csv")
  

