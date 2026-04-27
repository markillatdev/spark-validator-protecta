import pytest
import os
import sys
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    assert os.getenv("DB_URL_SILUX_PROTECTA"), "DB_URL_SILUX_PROTECTA not set"
    assert os.getenv("DB_URL_SOLBEN_PROTECTA"), "DB_URL_SOLBEN_PROTECTA not set"
    yield
