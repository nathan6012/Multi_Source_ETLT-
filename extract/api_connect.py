import asyncio
import logging
import json
from pathlib import Path
import os 
from dotenv import load_dotenv

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential
from aiolimiter import AsyncLimiter

logging.getLogger().setLevel(logging.INFO)


load_dotenv()
API_KEY = os.getenv("API_KEY")
limiter = AsyncLimiter(5, 1)

# -----------------------------

cwd = Path(__file__).resolve().parent
root = cwd.parent
sub_folder = root/"local"
# state checker Saved in local 
# rest or use startung before for okd data 
CHECKPOINT_FILE = sub_folder/"checkpoint.json"




# Helper 1
def load_checkpoint():
  """ cursor state file to check previous Load """
  
  if not CHECKPOINT_FILE.exists():
    return None

  try:
    data = json.loads(CHECKPOINT_FILE.read_text())
    return data.get("starting_after")
    
  except Exception as e:
    logging.warning(f"Checkpoint corrupted, restarting fresh: {e}")
    return None



# Helper 2 
def save_checkpoint(starting_after):
  """ saves/updates the cursor for next load """
  # i doubt this surfix might cause isues 
  
  print("Filess")
  temp_file = CHECKPOINT_FILE.with_name("checkpoint.tmp")

  temp_file.write_text(
   json.dumps({"starting_after": starting_after})
    )
  temp_file.replace(CHECKPOINT_FILE)
  


#Fetch logic 
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10)
)
async def fetch(client, starting_after=None):
    async with limiter:
      params = {"limit": 100}

      if starting_after:
        params["starting_after"] = starting_after
        
        
      resp = await client.get(
        # Change url and endpoint 
            "https://api.stripe.com/v1/payment_intents",
            params=params
        )

      resp.raise_for_status()
      return resp.json()


#Pipeline 
async def run_pipeline():
  logging.info("Starting Stripe ETL job")

  all_data = [] # container 

    # restore state if exists
  starting_after = load_checkpoint()

  headers = {
        "Authorization": f"Bearer {API_KEY}"}

  async with httpx.AsyncClient(timeout=10, headers=headers) as client:
    while True:
      logging.info(f"Fetching starting_after={starting_after}")

      data = await fetch(client, starting_after)

      items = data.get("data", [])

      if items:
        all_data.extend(items)
        starting_after = items[-1]["id"]
        save_checkpoint(starting_after)

  # stop condition
      if not data.get("has_more"):
        break

  logging.info(f"Finished. Total api records: {len(all_data)}")

  return all_data


# Testsing 


  