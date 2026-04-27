import os
from dotenv import load_dotenv
from  utils.constants import Constants
load_dotenv()

DATABASE_CONFIG = {
    Constants.SYSTEM_SILUX_PROTECTA: {
        "url": os.getenv("DB_URL_SILUX_PROTECTA"),
        "host": os.getenv("DB_HOST_SILUX_PROTECTA"),
        "port": int(os.getenv("DB_PORT_SILUX_PROTECTA")),
        "user": os.getenv("DB_USER_SILUX_PROTECTA"),
        "password": os.getenv("DB_PASSWORD_SILUX_PROTECTA"),
        "database": os.getenv("DB_DATABASE_SILUX_PROTECTA")
    },
    Constants.SYSTEM_SOLBEN_PROTECTA: {
        "url": os.getenv("DB_URL_SOLBEN_PROTECTA"),
        "user": os.getenv("DB_USER_SOLBEN_PROTECTA"),
        "password": os.getenv("DB_PASSWORD_SOLBEN_PROTECTA")
    }
}

PARQUET_ATTENTIONS_PATHS = {
    Constants.SYSTEM_SOLBEN_PROTECTA: f'{os.getenv("STORAGE_PATH")}/attentions/{Constants.SYSTEM_SOLBEN_PROTECTA}/*.parquet',
    Constants.SYSTEM_SILUX_PROTECTA: f'{os.getenv("STORAGE_PATH")}/attentions/{Constants.SYSTEM_SILUX_PROTECTA}/*.parquet',
}

PARQUET_INVOICES_PATHS = {
    Constants.SYSTEM_SOLBEN_PROTECTA: f'{os.getenv("STORAGE_PATH")}/invoices/{Constants.SYSTEM_SOLBEN_PROTECTA}/*.parquet',
    Constants.SYSTEM_SILUX_PROTECTA: f'{os.getenv("STORAGE_PATH")}/invoices/{Constants.SYSTEM_SILUX_PROTECTA}/*.parquet',
}

PARQUET_TAXTYPE_PATHS = {
    Constants.SYSTEM_SOLBEN_PROTECTA: f'{os.getenv("STORAGE_PATH")}/taxtypes/{Constants.SYSTEM_SOLBEN_PROTECTA}/*.parquet',
    Constants.SYSTEM_SILUX_PROTECTA: f'{os.getenv("STORAGE_PATH")}/taxtypes/{Constants.SYSTEM_SILUX_PROTECTA}/*.parquet',
}

PARQUET_AMOUNT_PATHS = {
    Constants.SYSTEM_SOLBEN_PROTECTA: f'{os.getenv("STORAGE_PATH")}/amounts/{Constants.SYSTEM_SOLBEN_PROTECTA}/*.parquet',
    Constants.SYSTEM_SILUX_PROTECTA: f'{os.getenv("STORAGE_PATH")}/amounts/{Constants.SYSTEM_SILUX_PROTECTA}/*.parquet',
}