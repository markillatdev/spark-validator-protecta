import os
from dotenv import load_dotenv
from  utils.constants import Constants
load_dotenv()

DATABASE_CONFIG = {
    Constants.SYSTEM_SILUX_SABSA: {
        "url": os.getenv("DB_URL_SILUX_SABSA"),
        "host": os.getenv("DB_HOST_SILUX_SABSA"),
        "port": int(os.getenv("DB_PORT_SILUX_SABSA")),
        "user": os.getenv("DB_USER_SILUX_SABSA"),
        "password": os.getenv("DB_PASSWORD_SILUX_SABSA"),
        "database": os.getenv("DB_DATABASE_SILUX_SABSA")
    },
    Constants.SYSTEM_SILUX_COBERTURA: {
        "url": os.getenv("DB_URL_SILUX_CM"),
        "host": os.getenv("DB_HOST_SILUX_CM"),
        "port": int(os.getenv("DB_PORT_SILUX_CM")),
        "user": os.getenv("DB_USER_SILUX_CM"),
        "password": os.getenv("DB_PASSWORD_SILUX_CM"),
        "database": os.getenv("DB_DATABASE_SILUX_CM")
    },
    Constants.SYSTEM_SOLBEN_SABSA: {
        "url": os.getenv("DB_URL_SOLBEN_SABSA"),
        "user": os.getenv("DB_USER_SOLBEN_SABSA"),
        "password": os.getenv("DB_PASSWORD_SOLBEN_SABSA")
    },
    Constants.SYSTEM_SOLBEN_COBERTURA: {
        "url": os.getenv("DB_URL_SOLBEN_CM"),
        "user": os.getenv("DB_USER_SOLBEN_CM"),
        "password": os.getenv("DB_PASSWORD_SOLBEN_CM")
    }
}

PARQUET_ATTENTIONS_PATHS = {
    Constants.SYSTEM_UNIX_SABSA: f'{os.getenv("STORAGE_PATH")}/attentions/unix_sabsa/*.parquet',
    Constants.SYSTEM_UNIX_COBERTURA: f'{os.getenv("STORAGE_PATH")}/attentions/unix_cobertura/*.parquet',
}

PARQUET_INVOICES_PATHS = {
    Constants.SYSTEM_UNIX_SABSA: f'{os.getenv("STORAGE_PATH")}/invoices/unix_sabsa/*.parquet',
    Constants.SYSTEM_UNIX_COBERTURA: f'{os.getenv("STORAGE_PATH")}/invoices/unix_cobertura/*.parquet',
}
