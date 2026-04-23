import sys 
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pathlib 
from pathlib import Path
import time 
import logging 

logging.basicConfig(level=logging.INFO)


def files_management():
  """ Tracks and deletes System Files(in datalake) after 7 days """
  days = 7
  pwd = Path(__file__).resolve().parent
  root_dir = pwd.parent
  sub_folder = root_dir/"datalake"
  
  now = time.time()
  cutoff = now - (days * 86400)
  
  
  for file_path in sub_folder.iterdir():
    if file_path.suffix in {"json","csv"} and file_path.is_file():
      file_mtime = file_path.stat().st_mtime
      
      if file_mtime < cutoff:
        try:
          file_path.unlink()
          logging.info(f"Deleted: {file_path}")
        except Exception as e:
            logging.warning(f"Error deleting {file_path}: {e}")

  
