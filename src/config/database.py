import os
from dotenv import load_dotenv
load_dotenv()

DATABASE_CONFIG = {
    "silux_sabsa": {
        "url": os.getenv("DB_URL_SILUX_SABSA"),
        "host": os.getenv("DB_HOST_SILUX_SABSA"),
        "port": int(os.getenv("DB_PORT_SILUX_SABSA")),
        "user": os.getenv("DB_USER_SILUX_SABSA"),
        "password": os.getenv("DB_PASSWORD_SILUX_SABSA"),
        "database": os.getenv("DB_DATABASE_SILUX_SABSA")
    },
    "silux_cobertura": {
        "url": os.getenv("DB_URL_SILUX_CM"),
        "host": os.getenv("DB_HOST_SILUX_CM"),
        "port": int(os.getenv("DB_PORT_SILUX_CM")),
        "user": os.getenv("DB_USER_SILUX_CM"),
        "password": os.getenv("DB_PASSWORD_SILUX_CM"),
        "database": os.getenv("DB_DATABASE_SILUX_CM")
    },
    "sabsa_solben": {
        "url": os.getenv("DB_URL_SOLBEN_SABSA"),
        "user": os.getenv("DB_USER_SOLBEN_SABSA"),
        "password": os.getenv("DB_PASSWORD_SOLBEN_SABSA")
    },
    "sabsa_cobertura": {
        "url": os.getenv("DB_URL_SOLBEN_CM"),
        "user": os.getenv("DB_USER_SOLBEN_CM"),
        "password": os.getenv("DB_PASSWORD_SOLBEN_CM")
    }
}

PARQUET_ATTENTIONS_PATHS = {
    "unix_sabsa": f'{os.getenv("STORAGE_PATH")}/attentions/unix_sabsa/*.parquet',
    "unix_cobertura": f'{os.getenv("STORAGE_PATH")}/attentions/unix_cobertura/*.parquet',
}

PARQUET_INVOICES_PATHS = {
    "unix_sabsa": f'{os.getenv("STORAGE_PATH")}/invoices/unix_sabsa/*.parquet',
    "unix_cobertura": f'{os.getenv("STORAGE_PATH")}/invoices/unix_cobertura/*.parquet',
}
