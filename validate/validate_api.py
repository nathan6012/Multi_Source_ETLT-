import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from pydantic import BaseModel, ValidationError
from datetime import datetime
import json 
import logging 

logging.getLogger().setLevel(logging.INFO)



def validate_api(data,Model):
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
  
  if len(unclean) > 0:
    logging.warning("Some API Data has Been Pushed to Dead-Letter queue/Invalid failed Validatedion")
    
    logging.info(f"Invalid data: {len(unclean)}  check and clean fields  while clean: {len(clean)}")
    
    
  # Remove aftet Cleaning   
  with open("valid_api.json","w") as f:
    json.dump(clean,f,indent=6)
      
    
  
  return clean,unclean


  