import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


import pandas as pd
import json
import logging 
#from pathlib import Path 
logging.getLogger().setLevel(logging.INFO)





def transform_database_data(clean_x,unclean_y):

  clean = pd.json_normalize(clean_x)
  
  unclean = pd.json_normalize(unclean_y)
  
  
  pd.set_option("display.max_rows", None)
  pd.set_option("display.max_columns", None)
  
  
  
  clean.columns = clean.columns.str.replace("data.",'',regex=True)
  
  col_list = [
  "category"         
  "region"              
  "production_date"      
  "units_produced"   
  "defective_units"   
  "revenue"              
  "cost"              
  "analyst_score" ]
  

#  clean["production_date"] = pd.to_datetime(clean["production_date"], utc=True)
  
  clean["production_date"] = pd.to_datetime(clean["production_date"]).dt.tz_localize(None)
  
  
  clean = clean.astype({
   "defective_units": "int32",
   "revenue":"float32",
   "cost": "float32"})
  
  clean["analyst_score"] = pd.to_numeric(clean["analyst_score"], errors="coerce").astype("float32")
  
  clean = clean.dropna(subset=["product_name"])
  
 # clean = clean.dropna(subset=["product_id"]) 
  #if "product_id" in clean.columns:
   # clean = clean.dropna(subset=["product_id"])
    
  for col in col_list:
    if col in clean.columns:
        clean[col] = clean[col].replace({pd.NA: None})
  clean = clean.drop(columns=["idx"])
  

  dataset = clean.to_dict(orient="records")
  
  logging.info(f"Database Data Cleaned: {len(dataset)} records")
  
  return dataset
  
  

  
  
  #print(clean.isna().sum())
  
  

