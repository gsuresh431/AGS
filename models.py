from pydantic import BaseModel
from datetime import date

class Product(BaseModel):
    CustomerId: str
    ProductName: str
    Domain: str
    StartDate: date
    DurationMonths: int