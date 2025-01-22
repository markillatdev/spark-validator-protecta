from fastapi import HTTPException, status, Request
from jose import jwt
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
load_dotenv()

API_KEY = os.getenv("API_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = "HS256"

def create_access_token(data: dict):
    to_encode = data.copy()
    token_expires_at = datetime.utcnow() + timedelta(minutes=30)
    to_encode.update({"exp": token_expires_at})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def get_access_token(apiKey: str):
    if apiKey != API_KEY:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unathorized")
    access_token = create_access_token(data={"service": "microservice"})
    return {"access_token": access_token, "expires_in": 1800}

def verify_token(request: Request):
    authorization = request.headers.get("Authorization")
    if authorization is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="No autenticado")
    else:
        try:
            token = authorization.split(" ")[1]
            return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        except:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="No autenticado")