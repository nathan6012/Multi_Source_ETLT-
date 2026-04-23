import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pydantic import BaseModel,Field,field_validator, ConfigDict
from decimal import Decimal
from datetime import datetime
from typing import Optional
from datetime import datetime 





class SalesData(BaseModel):
  
  model_config = ConfigDict(extra="forbid")
 # File model 
  order_id: int 
  customer: str
  region: Optional[str] = None 
  product: str 
  quantity: int                     
  unit_price: Decimal
  cost_per_unit: Decimal
  order_date: datetime
  
  sheet_name: Optional[str] = None 
  
class ProductsData(BaseModel):
  
  model_config = ConfigDict(extra="forbid")
  # Database Data 
  production_id: int 
  product_name: str 
  category: str 
  region: Optional[str] = None 
  production_date: datetime
  units_produced: int 
  defective_units: int
  revenue: Decimal
  cost: Decimal
  analyst_score: Optional[Decimal] = None 
  
  

class PaymentData(BaseModel):
  
  model_config = ConfigDict(extra="forbid")
  
  #api Data 
  id: str 
  object: str 
  amount: Decimal
  amount_received: Decimal
  currency: str 
  status: str 
  created: int 
  metadata_order_id: Optional[str] = None
  metadata_product: Optional[str] = None 
  metadata_channel: Optional[str] = None
  metadata_region: Optional[str] = None 
  metadata_customer_type: Optional[str] = None
  
 # metadata_order_id: str | None = None
  