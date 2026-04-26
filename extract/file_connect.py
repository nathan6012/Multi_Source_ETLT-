import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import openpyxl
from pathlib import Path
import pandas as pd 
import zipfile
import logging 
#Logging 
logging.getLogger().setLevel(logging.INFO)
#Helper Load 

# main Extract 
def extract_xl_file(): # add Param
  """ Scan folder for excel files if read the file if exists and convert/ return  to list[dict]
  for processing """ 

  folder_dir = Path(__file__).resolve().parent
  root_dir = folder_dir.parent
  storage = root_dir / "local"

  if not storage.exists():
    logging.info("Storage folder does not exist")
    return []
  # scaned file 
  xl_files = list(storage.glob("*.xlsx"))

  if not xl_files:
    logging.info(f"No execel file found in storage ")
    
    return []

    # pick latests file 
  target_file_path = max(xl_files, key=lambda f: f.stat().st_mtime)
  
  
  all_data = [] 
  
  try:
    sheets_dict = pd.read_excel(target_file_path, sheet_name=None, engine="openpyxl")
      
  except zipfile.BadZipFile:
    logging.warning(f"Bad Excel: {target_file_path} is invalid excel file")
  
  except Exception as e:
    logging.error(f"Failed to read Excel file {target_file_path}: {e}")
    sheets_dict = {}  
  
  #If valid Run 
  for sheet_name, df in sheets_dict.items():
    records = df.to_dict(orient="records")
    for r in records:
      r["sheet_name"] = sheet_name  # optional but useful for BI/KPIs
    all_data.extend(records)
    
  logging.info(f" Extracted Data from Excel file: {len(all_data)} ")
  
  

 # target_file_path -- > filename 
  
  return all_data 

  
def main():
  extract_xl_file()
if __name__ =="__main__":
  main()
  
