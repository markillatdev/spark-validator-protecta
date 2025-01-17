from pydantic import BaseModel

class responseSchema(BaseModel):
    msg: str
    
    class Config:
        shema_extra = {
            "ejemplo": {
                "msg": "success",
            }
        }

class JwtEsquema(BaseModel):
    access_token: str

    class Config:
        schema_extra = {
            "ejemplo": {
                "access_token": "************"
            }
        }

class ApiKeyEsquema(BaseModel):
    apikey: str

    class Config:
        schema_extra = {
            "ejemplo": {
                "apikey": "************"
            }
        }