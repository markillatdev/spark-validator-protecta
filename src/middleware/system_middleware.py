from fastapi import Request, HTTPException, status
from fastapi.responses import JSONResponse
from utils.constants import Constants

class SystemMiddleware:
    def __init__(self):
        self.allowed_systems = [
            Constants.SYSTEM_SABSA,
            Constants.SYSTEM_COBERTURA
        ]

    async def __call__(self, request: Request, call_next):
        try:
            system = request.headers.get("system")

            if system not in self.allowed_systems:
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Sistema no autorizado")
            
            response = await call_next(request)    
            return response
        
        except HTTPException as http_ex:
            return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content={"msg": http_ex.detail})
