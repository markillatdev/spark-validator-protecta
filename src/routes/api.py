from fastapi import APIRouter, status, Request, Depends
from schemas.schema import *
from services.loader.count_dataframe_service import CountDataframe
from services.system_service import SystemService
from services.loader.data_loader_service import DataFrameLoader
from services.jwt_service import *
from services.validate.test_dataframe_service import testDataframeService

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

@router.post("/validate-taxtypes-import-duplicate", status_code=status.HTTP_200_OK, response_model=responseSchema)
async def validateInternalTaxTypeDuplicateImport(schema: InvoiceSchema,request: Request, token: str = Depends(verify_token)):
    service = SystemService(request.headers.get("system"))
    return service.operations_taxtypes(schema.invoiceIds)

@router.post("/validate-amounts-import-duplicate", status_code=status.HTTP_200_OK, response_model=responseSchema)
async def validateInternalAmountDuplicateImport(schema: InvoiceSchema,request: Request, token: str = Depends(verify_token)):
    service = SystemService(request.headers.get("system"))
    return service.operations_amounts(schema.invoiceIds)

@router.post("/update-invoices-unique", status_code=status.HTTP_200_OK, response_model=responseSchema)
async def updateInvoiceUnique(schema: InvoiceSchema, request: Request, token: str = Depends(verify_token)):
    service = SystemService(request.headers.get("system"))
    return service.operations_update_invoices(schema.invoiceIds)

@router.put("/update-reset-invoices", status_code=status.HTTP_200_OK, response_model=responseSchema)
async def updateResetInvoice(schema: InvoiceSchema, request: Request, token: str = Depends(verify_token)):
    service = SystemService(request.headers.get("system"))
    return service.operations_update_reset_invoices(schema.invoiceIds)

@router.post("/load-dataframe-to-database", status_code=status.HTTP_200_OK, response_model=responseBasicSchema)
async def loadDataFrameToDatabase(schema: DataFrameSchema, request: Request, token: str = Depends(verify_token)):
    service = DataFrameLoader(request.headers.get("system"))
    return service.load_data(schema.years, schema.origen)

@router.delete("/destroy-dataframe", status_code=status.HTTP_200_OK, response_model=responseBasicSchema)
async def deleteDataframe(schema: DestroyDataFrameSchema, request: Request, token: str = Depends(verify_token)):
    service = DataFrameLoader(request.headers.get("system"))
    return service.destroy_dataframe(schema.origen)

@router.post("/count-records-dataframes", status_code=status.HTTP_200_OK, response_model=responseBasicSchema)
async def getCountRecordsDataframes(schema: GetRecordsSchema, token: str = Depends(verify_token)):
    service = CountDataframe()
    return service.count_merged_records(schema.repository)

@router.post("/test-dataframes", status_code=status.HTTP_200_OK, response_model=responseBasicSchema)
async def testDataFrames(schema: InvoiceSchema, request: Request):
    service = testDataframeService(schema.invoiceIds, request.headers.get("system"))
    return service.res()