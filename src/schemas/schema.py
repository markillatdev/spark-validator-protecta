from typing import List
from pydantic import BaseModel
from utils.constants import Constants

class responseSchema(BaseModel):
    msg: str
    
    class Config:
        shema_extra = {
            "ejemplo": {
                "msg": "success",
            }
        }

class JwtSchema(BaseModel):
    access_token: str

    class Config:
        schema_extra = {
            "ejemplo": {
                "access_token": "************"
            }
        }

class ApiKeySchema(BaseModel):
    apikey: str

    class Config:
        schema_extra = {
            "ejemplo": {
                "apikey": "************"
            }
        }

class InvoiceSchema(BaseModel):
    invoiceIds: List[int]

    class Config:
        schema_extra = {
            "ejemplo": {
                "invoiceIds": [1, 2, 3, 4, 5, 6]
            }
        }


class DataFrameSchema(BaseModel):
    years: List[int]
    origen: str

    class Config:
        schema_extra = {
            "ejemplo": {
                "years": [2019, 2020],
                "origen": [Constants.SYSTEM_SILUX_SABSA, Constants.SYSTEM_SILUX_COBERTURA, Constants.SYSTEM_UNIX_SABSA, Constants.SYSTEM_UNIX_COBERTURA]
            }
        }