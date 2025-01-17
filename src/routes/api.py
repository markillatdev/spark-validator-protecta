from fastapi import APIRouter, status, Request, Depends
from schemas.schema import *
from services.system_service import SystemService
from services.jwt_service import *

router = APIRouter()

@router.get("/auth/token", status_code=status.HTTP_200_OK, response_model=JwtEsquema)
async def generateToken(data: ApiKeyEsquema):
    return get_access_token(data.apikey)
    
@router.post("/validate-invoice-duplicate", status_code=status.HTTP_200_OK, response_model=responseSchema)
async def validateInternalInvoiceDuplicate(request: Request, token: str = Depends(verify_token)):
    service = SystemService(request.headers.get("system"))
    return service.operations_invoices()

@router.post("/validate-attention-import-duplicate", status_code=status.HTTP_200_OK, response_model=responseSchema)
async def validateInternalAttentionDuplicateImport(request: Request, token: str = Depends(verify_token)):
    service = SystemService(request.headers.get("system"))
    return service.operations_attention()
