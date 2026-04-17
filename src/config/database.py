import os
from dotenv import load_dotenv
from  utils.constants import Constants
load_dotenv()

DATABASE_CONFIG = {
    Constants.SYSTEM_SILUX_SEMEFA: {
        "url": os.getenv("DB_URL_SILUX_SEMEFA"),
        "host": os.getenv("DB_HOST_SILUX_SEMEFA"),
        "port": int(os.getenv("DB_PORT_SILUX_SEMEFA")),
        "user": os.getenv("DB_USER_SILUX_SEMEFA"),
        "password": os.getenv("DB_PASSWORD_SILUX_SEMEFA"),
        "database": os.getenv("DB_DATABASE_SILUX_SEMEFA")
    },
    Constants.SYSTEM_SOLBEN_SEMEFA: {
        "url": os.getenv("DB_URL_SOLBEN_SEMEFA"),
        "user": os.getenv("DB_USER_SOLBEN_SEMEFA"),
        "password": os.getenv("DB_PASSWORD_SOLBEN_SEMEFA")
    }
}

PARQUET_ATTENTIONS_PATHS = {
    Constants.SYSTEM_SOLBEN_SEMEFA: f'{os.getenv("STORAGE_PATH")}/attentions/{Constants.SYSTEM_SOLBEN_SEMEFA}/*.parquet',
    Constants.SYSTEM_SILUX_SEMEFA: f'{os.getenv("STORAGE_PATH")}/attentions/{Constants.SYSTEM_SILUX_SEMEFA}/*.parquet',
}

PARQUET_INVOICES_PATHS = {
    Constants.SYSTEM_SOLBEN_SEMEFA: f'{os.getenv("STORAGE_PATH")}/invoices/{Constants.SYSTEM_SOLBEN_SEMEFA}/*.parquet',
    Constants.SYSTEM_SILUX_SEMEFA: f'{os.getenv("STORAGE_PATH")}/invoices/{Constants.SYSTEM_SILUX_SEMEFA}/*.parquet',
}

PARQUET_TAXTYPE_PATHS = {
    Constants.SYSTEM_SOLBEN_SEMEFA: f'{os.getenv("STORAGE_PATH")}/taxtypes/{Constants.SYSTEM_SOLBEN_SEMEFA}/*.parquet',
    Constants.SYSTEM_SILUX_SEMEFA: f'{os.getenv("STORAGE_PATH")}/taxtypes/{Constants.SYSTEM_SILUX_SEMEFA}/*.parquet',
}

PARQUET_AMOUNT_PATHS = {
    Constants.SYSTEM_SOLBEN_SEMEFA: f'{os.getenv("STORAGE_PATH")}/amounts/{Constants.SYSTEM_SOLBEN_SEMEFA}/*.parquet',
    Constants.SYSTEM_SILUX_SEMEFA: f'{os.getenv("STORAGE_PATH")}/amounts/{Constants.SYSTEM_SILUX_SEMEFA}/*.parquet',
}