from typing import List, Optional
from pydantic import BaseModel
from utils.constants import Constants
from pydantic import validator


class responseSchema(BaseModel):
    msg: str
    success: bool
    total: Optional[int] = None
    
    class Config:
        shema_extra = {
            "ejemplo": {
                "msg": "success",
                "success": True,
                "total": 12
            }
        }


class JwtSchema(BaseModel):
    access_token: str
    expires_in: int

    class Config:
        schema_extra = {
            "ejemplo": {
                "access_token": "************",
                "expires_in": 1800
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

    @validator('invoiceIds')
    def check_not_empty(cls, v):
        if not v:
            raise ValueError('invoiceIds list cannot be empty')
        return v

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