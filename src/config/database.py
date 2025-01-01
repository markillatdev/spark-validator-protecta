import os
from dotenv import load_dotenv
load_dotenv()

DATABASE_CONFIG = {
    "silux_sabsa": {
        "url": os.getenv("DB_URL_IUNIX_SABSA"),
        "host": os.getenv("DB_HOST_SABSA"),
        "port": os.getenv("DB_PORT_SABSA"),
        "user": os.getenv("DB_USER_IUNIX_SABSA"),
        "password": os.getenv("DB_PASSWORD_IUNIX_SABSA"),
        "database": os.getenv("DB_DATABASE_SABSA")
    },
    "silux_cobertura": {
        "url": os.getenv("DB_URL_IUNIX_CM"),
        "host": os.getenv("DB_HOST_CM"),
        "port": os.getenv("DB_PORT_CM"),
        "user": os.getenv("DB_USER_IUNIX_CM"),
        "password": os.getenv("DB_PASSWORD_IUNIX_CM"),
        "database": os.getenv("DB_DATABASE_CM")
    },
    "sabsa_dev": {
        #"url": os.getenv("DB_URL", "jdbc:mysql://localhost:3307/liquidaciones_test_pro_sabsa"),
        "url": os.getenv("DB_URL", "jdbc:mysql://localhost:3306/db_liquidaciones_test_2025"),
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", 3306),
        "user": os.getenv("DB_USER", "root"),
        "password": os.getenv("DB_PASSWORD", ""),
        #"database": os.getenv("DB_NAME", "liquidaciones_test_pro_sabsa"),
        "database": os.getenv("DB_NAME", "db_liquidaciones_test_2025")
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
