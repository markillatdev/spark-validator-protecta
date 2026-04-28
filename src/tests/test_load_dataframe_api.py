import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from jose import jwt

from main import app
from utils.constants import Constants
from services.jwt_service import SECRET_KEY, ALGORITHM

client = TestClient(app)


def get_test_token():
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
        assert "task_id" in data
        assert data["status"] == "PENDING"

    @patch('routes.api.load_dataframe_to_database_task.delay')
    def test_load_dataframe_success_mocked(self, mock_task):
        token = get_test_token()
        mock_task.return_value = MagicMock(id="test-task-id")
        
        response = client.post(
            "/api/load-dataframe-to-database",
            json={"years": [2019], "origen": Constants.SYSTEM_SILUX_PROTECTA},
            headers={"Authorization": f"Bearer {token}", "system": Constants.SYSTEM_SILUX_PROTECTA}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "task_id" in data
        assert data["status"] == "PENDING"

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

    def test_load_dataframe_empty_years(self):
        token = get_test_token()
        response = client.post(
            "/api/load-dataframe-to-database",
            json={"years": [], "origen": Constants.SYSTEM_SILUX_PROTECTA},
            headers={"Authorization": f"Bearer {token}", "system": Constants.SYSTEM_SILUX_PROTECTA}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "PENDING"

    @patch('routes.api.load_dataframe_to_database_task.delay')
    def test_load_dataframe_service_exception(self, mock_task):
        token = get_test_token()
        mock_task.side_effect = Exception("Service error")
        
        response = client.post(
            "/api/load-dataframe-to-database",
            json={"years": [2019], "origen": Constants.SYSTEM_SILUX_PROTECTA},
            headers={"Authorization": f"Bearer {token}", "system": Constants.SYSTEM_SILUX_PROTECTA}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "task_id" in data
