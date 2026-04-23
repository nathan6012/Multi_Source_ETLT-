import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pydantic import BaseModel, ValidationError
from datetime import datetime
import json
import logging 

logging.getLogger().setLevel(logging.INFO)

def validate_database_data(data,Model):
  clean = []
  unclean = []
  for idx,records in enumerate(data):
    #Our clean Data 
    try:
      valid = Model(**records)
      clean.append({
        "idx":idx,
        "data":valid.model_dump(mode="json")})
    #Our Not clean Data Logic     
        
    except ValidationError as e:
      unclean.append({
        "idx":idx,
        "data":records,
          "errors": e.errors()
    
      })
  if  unclean:
    logging.warning("Some DB Data has Been Pushed to Dqueue/Invalid failed Validatedion check and clean fields ")
    
    
  
        
    
  return clean,unclean
  



