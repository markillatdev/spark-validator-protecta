from typing import List, Optional
from pydantic import BaseModel, ConfigDict, field_validator
from utils.constants import Constants


class responseSchema(BaseModel):
    msg: str
    success: bool
    total: Optional[int] = None

    model_config = ConfigDict(
        json_schema_extra={
            "ejemplo": {
                "msg": "success",
                "success": True,
                "total": 12
            }
        }
    )

class responseBasicSchema(BaseModel):
    msg: str
    success: bool

    model_config = ConfigDict(
        json_schema_extra={
            "ejemplo": {
                "msg": "success",
                "success": True
            }
        }
    )

class JwtSchema(BaseModel):
    access_token: str
    expires_in: int

    model_config = ConfigDict(
        json_schema_extra={
            "ejemplo": {
                "access_token": "************",
                "expires_in": 1800
            }
        }
    )


class ApiKeySchema(BaseModel):
    apikey: str

    model_config = ConfigDict(
        json_schema_extra={
            "ejemplo": {
                "apikey": "************"
            }
        }
    )


class InvoiceSchema(BaseModel):
    invoiceIds: List[int]

    @field_validator('invoiceIds')
    @classmethod
    def check_not_empty(cls, v):
        if not v:
            raise ValueError('invoiceIds list cannot be empty')
        return v

    model_config = ConfigDict(
        json_schema_extra={
            "ejemplo": {
                "invoiceIds": [1, 2, 3, 4, 5, 6]
            }
        }
    )


class DataFrameSchema(BaseModel):
    years: List[int]
    origen: str

    model_config = ConfigDict(
        json_schema_extra={
            "ejemplo": {
                "years": [2019, 2020],
                "origen": [Constants.SYSTEM_SILUX_PROTECTA, Constants.SYSTEM_SOLBEN_PROTECTA]
            }
        }
    )

class DestroyDataFrameSchema(BaseModel):
    origen: str

    model_config = ConfigDict(
        json_schema_extra={
            "ejemplo": {
                "origen": [Constants.SYSTEM_SILUX_PROTECTA, Constants.SYSTEM_SOLBEN_PROTECTA]
            }
        }
    )

class GetRecordsSchema(BaseModel):
    repository: str

    model_config = ConfigDict(
        json_schema_extra={
            "ejemplo": {
                "repository": ["silux_protecta", "solben_protecta"],
            }
        }
    )
