from fastapi import APIRouter, status, Request, Depends
from schemas.schema import *
from services.system_service import SystemService
from services.jwt_service import *

router = APIRouter()

@router.get("/auth/token", status_code=status.HTTP_200_OK, response_model=JwtSchema)
async def generateToken(data: ApiKeySchema):
    return get_access_token(data.apikey)
    
@router.post("/validate-invoice-duplicate", status_code=status.HTTP_200_OK, response_model=responseSchema)
async def validateInternalInvoiceDuplicate(schema: InvoiceSchema, request: Request, token: str = Depends(verify_token)):
    service = SystemService(request.headers.get("system"))
    return service.operations_invoices(schema.invoiceIds)

@router.post("/validate-attention-import-duplicate", status_code=status.HTTP_200_OK, response_model=responseSchema)
async def validateInternalAttentionDuplicateImport(schema: InvoiceSchema,request: Request, token: str = Depends(verify_token)):
    service = SystemService(request.headers.get("system"))
    return service.operations_attention(schema.invoiceIds)

@router.post("/update-invoices-unique", status_code=status.HTTP_200_OK, response_model=responseSchema)
async def updateInvoiceUnique(schema: InvoiceSchema, request: Request, token: str = Depends(verify_token)):
    service = SystemService(request.headers.get("system"))
    return service.operations_update_invoices(schema.invoiceIds)