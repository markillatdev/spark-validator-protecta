from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from middleware.system_middleware import SystemMiddleware
from routes.api import router
from config.spark_config import get_spark_session, stop_spark_session
import atexit

app = FastAPI()

origins = [
    "http://localhost",
    "http://localhost:8080",
]

app.add_middleware(BaseHTTPMiddleware, dispatch=SystemMiddleware())
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

app.include_router(router, prefix="/api")

@app.on_event("startup")
async def startup_event():
    # Inicializar SparkSession al arrancar la aplicación
    get_spark_session()
    print("Aplicación iniciada - SparkSession lista")

@app.on_event("shutdown")
async def shutdown_event():
    # Detener SparkSession al cerrar la aplicación
    stop_spark_session()
    print("Aplicación cerrada - SparkSession detenida")

# Registrar cleanup al salir del proceso
atexit.register(stop_spark_session)
