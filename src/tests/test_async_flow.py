import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from fastapi import HTTPException
from jose import jwt
from main import app
from utils.constants import Constants
from services.jwt_service import SECRET_KEY, ALGORITHM


client = TestClient(app)


def get_test_token():
    token = jwt.encode({"service": "microservice"}, SECRET_KEY, algorithm=ALGORITHM)
    return token


class TestAuthFlow:
    def test_get_token_success(self):
        with patch('routes.api.get_access_token', return_value={"access_token": "mock_token", "expires_in": 1800}):
            response = client.request(
                "GET",
                "/api/auth/token",
                headers={"Content-Type": "application/json", "system": Constants.SYSTEM_SILUX_PROTECTA},
                json={"apikey": "dBGj6XLWfyK0xkScUpJTJjRx8z4vZ4bB"}
            )
            assert response.status_code == 200
            data = response.json()
            assert "access_token" in data

    def test_get_token_invalid_key(self):
        with patch('routes.api.get_access_token', side_effect=HTTPException(status_code=401, detail="Unathorized")):
            response = client.request(
                "GET",
                "/api/auth/token",
                headers={"Content-Type": "application/json", "system": Constants.SYSTEM_SILUX_PROTECTA},
                json={"apikey": "invalid_key"}
            )
            assert response.status_code == 401


class TestValidationFlow:
    @patch('routes.api.validate_duplicate_task.delay')
    def test_submit_validation_task(self, mock_task):
        mock_task.return_value = MagicMock(id="test-task-id")
        token = get_test_token()
        
        response = client.post(
            "/api/validate-invoices-duplicate",
            headers={
                "Content-Type": "application/json",
                "system": Constants.SYSTEM_SILUX_PROTECTA,
                "Authorization": f"Bearer {token}"
            },
            json={"invoiceIds": [1, 2, 3]}
        )
        
        assert response.status_code == 202
        data = response.json()
        assert "task_id" in data
        mock_task.assert_called_once_with([1, 2, 3], Constants.SYSTEM_SILUX_PROTECTA)

    @patch('routes.api.validate_duplicate_task.delay')
    def test_submit_validation_task_invalid_invoices(self, mock_task):
        token = get_test_token()
        
        response = client.post(
            "/api/validate-invoices-duplicate",
            headers={
                "Content-Type": "application/json",
                "system": Constants.SYSTEM_SILUX_PROTECTA,
                "Authorization": f"Bearer {token}"
            },
            json={"invoiceIds": []}
        )
        
        assert response.status_code == 422


class TestTaskStatus:
    @patch('routes.api.celery_app.AsyncResult')
    def test_get_task_status_pending(self, mock_async_result):
        mock_result = MagicMock()
        mock_result.state = "PENDING"
        mock_result.result = None
        mock_async_result.return_value = mock_result
        
        response = client.get("/api/task-status/test-task-id")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "PENDING"

    @patch('routes.api.celery_app.AsyncResult')
    def test_get_task_status_success(self, mock_async_result):
        mock_result = MagicMock()
        mock_result.state = "SUCCESS"
        mock_result.result = {"success": True, "msg": "Completed", "total": 3}
        mock_async_result.return_value = mock_result
        
        response = client.get("/api/task-status/test-task-id")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "SUCCESS"
        assert data["result"]["success"] is True

    @patch('routes.api.celery_app.AsyncResult')
    def test_get_task_status_failure(self, mock_async_result):
        mock_result = MagicMock()
        mock_result.state = "FAILURE"
        mock_result.result = "Task failed"
        mock_async_result.return_value = mock_result
        
        response = client.get("/api/task-status/test-task-id")
        
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "FAILURE"


class TestResetInvoicesFlow:
    @patch('routes.api.update_reset_invoices_task.delay')
    def test_reset_invoices(self, mock_task):
        mock_task.return_value = MagicMock(id="reset-task-id")
        token = get_test_token()
        
        response = client.put(
            "/api/update-reset-invoices",
            headers={
                "Content-Type": "application/json",
                "system": Constants.SYSTEM_SILUX_PROTECTA,
                "Authorization": f"Bearer {token}"
            },
            json={"invoiceIds": [1, 2, 3]}
        )
        
        assert response.status_code == 202
        data = response.json()
        assert "task_id" in data
        mock_task.assert_called_once_with([1, 2, 3], Constants.SYSTEM_SILUX_PROTECTA)
