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

class responseBasicSchema(BaseModel):
    msg: str
    success: bool
    
    class Config:
        shema_extra = {
            "ejemplo": {
                "msg": "success",
                "success": True
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
                "origen": [Constants.SYSTEM_SILUX_SEMEFA, Constants.SYSTEM_SOLBEN_SEMEFA]
            }
        }

class DestroyDataFrameSchema(BaseModel):
    origen: str

    class Config:
        schema_extra = {
            "ejemplo": {
                "origen": [Constants.SYSTEM_SILUX_SEMEFA, Constants.SYSTEM_SOLBEN_SEMEFA]
            }
        }

class GetRecordsSchema(BaseModel):
    repository: str

    class Config:
        schema_extra = {
            "ejemplo": {
                "repository": ["unix_sabsa", "unix_cobertura"],
            }
        }
