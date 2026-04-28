from fastapi import APIRouter, status, Request, Depends
from schemas.schema import *
from services.loader.count_dataframe_service import CountDataframe
from services.loader.data_loader_service import DataFrameLoader
from services.jwt_service import *
from services.validate.test_dataframe_service import testDataframeService
from core.celery_app import celery_app
from workers.tasks import validate_duplicate_task

router = APIRouter()

@router.get("/auth/token", status_code=status.HTTP_200_OK, response_model=JwtSchema)
async def generateToken(data: ApiKeySchema):
    return get_access_token(data.apikey)

@router.post("/validate-invoices-duplicate", status_code=status.HTTP_202_ACCEPTED, response_model=TaskResponseSchema)
async def validateInvoiceDuplicate(schema: InvoiceSchema, request: Request, token: str = Depends(verify_token)):
    system = request.headers.get("system")
    task = validate_duplicate_task.delay(schema.invoiceIds, system)
    return {"task_id": task.id, "status": "PENDING", "msg": "Tarea de validación enviada"}

@router.get("/task-status/{task_id}", status_code=status.HTTP_200_OK, response_model=TaskStatusSchema)
async def getTaskStatus(task_id: str):
    task = celery_app.AsyncResult(task_id)
    result = None
    error = None
    
    if task.state == 'SUCCESS':
        result = task.result
    elif task.state == 'FAILURE':
        error = str(task.info)
    elif task.state == 'PROGRESS':
        result = task.info
    
    return {"task_id": task_id, "status": task.state, "result": result, "error": error}

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