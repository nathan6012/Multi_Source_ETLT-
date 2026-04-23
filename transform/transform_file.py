import sys
import os


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from pandas import json_normalize
import json 
import logging 

#from pathlib import Path 
logging.getLogger().setLevel(logging.INFO)





def transform_excel_data(clean_n, unclean_p):

  clean = pd.json_normalize(clean_n)
  unclean = pd.json_normalize(unclean_p)

    # Improve debug visibility (optional in production)
  pd.set_option("display.max_rows", None)
  pd.set_option("display.max_columns", None)

    # Clean column names
  clean.columns = clean.columns.str.replace("data.", "", regex=True)

    # Drop unnecessary column safely
  clean = clean.drop(columns=["sheet_name"], errors="ignore")

    # Type casting
  clean = clean.astype({
        "order_id": "int32",
        "customer": "string",
        "region": "string",
        "product": "string",
        "quantity": "int32",
        "unit_price": "int32",
        "cost_per_unit": "float32",
    })

    # Parse datetime
#  clean["order_date"] = pd.to_datetime(clean["order_date"], utc=True)
  
  clean["order_date"] = pd.to_datetime(clean["order_date"]).dt.tz_localize(None)

    # Required columns cleanup list
  col_list = [
        "order_id",
        "region",
        "product",
        "quantity",
        "unit_price",
        "cost_per_unit",
    ]

    # Remove null customers early (data quality gate)
  clean = clean.dropna(subset=["customer"])

    # Normalize missing values
  for col in col_list:
    if col in clean.columns:
      clean[col] = clean[col].replace({pd.NA: None})

    # Drop index column safely
  clean = clean.drop(columns=["idx"], errors="ignore")
  
  print(clean.dtypes)

    # Convert to records
  dataset = clean.to_dict(orient="records")

  logging.info(f"Excel Data has been Cleaned: {len(dataset)} records")

  return dataset  
  






