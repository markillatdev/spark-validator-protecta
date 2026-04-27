import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
import sys
import os
from jose import jwt

from main import app
from utils.constants import Constants


client = TestClient(app)


def get_test_token():
    from services.jwt_service import SECRET_KEY, ALGORITHM
    token = jwt.encode({"service": "microservice"}, SECRET_KEY, algorithm=ALGORITHM)
    return token


class TestLoadDataFrameToDatabase:
    def test_load_dataframe_invalid_system(self):
        token = get_test_token()
        response = client.post(
            "/api/load-dataframe-to-database",
            json={"years": [2019], "origen": "invalid_system"},
            headers={"Authorization": f"Bearer {token}", "system": Constants.SYSTEM_SILUX_PROTECTA}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is False
        assert "No existe el sistema" in data["msg"]

    @patch('services.loader.data_loader_service.DataFrameLoader.querys')
    @patch('services.loader.data_loader_service.DataFrameLoader.load_attention')
    @patch('services.loader.data_loader_service.DataFrameLoader.load_invoices')
    @patch('services.loader.data_loader_service.DataFrameLoader.load_tax_type')
    @patch('services.loader.data_loader_service.DataFrameLoader.load_amount')
    def test_load_dataframe_success_mocked(
        self, mock_amount, mock_tax, mock_invoices, mock_attention, mock_querys
    ):
        token = get_test_token()
        mock_querys.return_value = ("q1", "q2", "q3", "q4")
        
        response = client.post(
            "/api/load-dataframe-to-database",
            json={"years": [2019], "origen": Constants.SYSTEM_SILUX_PROTECTA},
            headers={"Authorization": f"Bearer {token}", "system": Constants.SYSTEM_SILUX_PROTECTA}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["msg"] == "Datos cargados exitosamente"

    @patch('services.loader.data_loader_service.DataFrameLoader.querys')
    def test_load_dataframe_querys_return_none(self, mock_querys):
        token = get_test_token()
        mock_querys.return_value = (None, None, None, None)
        
        response = client.post(
            "/api/load-dataframe-to-database",
            json={"years": [2019], "origen": "invalid_system"},
            headers={"Authorization": f"Bearer {token}", "system": Constants.SYSTEM_SILUX_PROTECTA}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is False

    def test_load_dataframe_without_token(self):
        response = client.post(
            "/api/load-dataframe-to-database",
            json={"years": [2019], "origen": Constants.SYSTEM_SILUX_PROTECTA}
        )
        assert response.status_code == 401

    def test_load_dataframe_invalid_years_type(self):
        token = get_test_token()
        response = client.post(
            "/api/load-dataframe-to-database",
            json={"years": "2019", "origen": Constants.SYSTEM_SILUX_PROTECTA},
            headers={"Authorization": f"Bearer {token}", "system": Constants.SYSTEM_SILUX_PROTECTA}
        )
        assert response.status_code == 422

    def test_load_dataframe_missing_origen(self):
        token = get_test_token()
        response = client.post(
            "/api/load-dataframe-to-database",
            json={"years": [2019]},
            headers={"Authorization": f"Bearer {token}", "system": Constants.SYSTEM_SILUX_PROTECTA}
        )
        assert response.status_code == 422
