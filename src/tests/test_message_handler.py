import pytest
from unittest.mock import MagicMock
from utils.message_handler import MessageHandler


class MockValue:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class TestMessageHandler:

    def test_message_case_1_with_duplicate(self):
        message = "Duplicado encontrado"
        value = MockValue(
            codigo_iafa="IAFA001",
            ruc_proveedor="12345678901",
            nro_factu="F001-00001",
            codigo_afiliado="Afiliado123",
            fch_atencion="2024-01-15",
            nro_solben="SOL001",
            monto=100.50
        )
        system = "silux_semefa"

        result = MessageHandler.message_case_1(message, value, system)

        assert "Duplicado encontrado" in result
        assert "IAFA001" in result
        assert "12345678901" in result
        assert "F001-00001" in result
        assert "Afiliado123" in result
        assert "2024-01-15" in result
        assert "SOL001" in result
        assert "100.5" in result
        assert "silux_semefa" in result

    def test_message_case_2_with_duplicate(self):
        message = "Duplicado encontrado"
        value = MockValue(
            codigo_iafa="IAFA002",
            ruc_proveedor="12345678902",
            codigo_afiliado="Afiliado456",
            fch_atencion="2024-02-20",
            nro_solben="SOL002",
            monto=200.75
        )
        system = "solben_semefa"

        result = MessageHandler.message_case_2(message, value, system)

        assert "Duplicado encontrado" in result
        assert "IAFA002" in result
        assert "Afiliado456" in result
        assert "200.75" in result
        assert "solben_semefa" in result

    def test_message_case_3_with_duplicate(self):
        message = "Duplicado encontrado"
        value = MockValue(
            codigo_iafa="IAFA003",
            ruc_proveedor="12345678903",
            codigo_afiliado="Afiliado789",
            fch_atencion="2024-03-25",
            nro_solben="SOL003",
            tipo_impuesto="IGV"
        )
        system = "silux_semefa"

        result = MessageHandler.message_case_3(message, value, system)

        assert "Duplicado encontrado" in result
        assert "IGV" in result

    def test_message_case_4_with_duplicate(self):
        message = "Duplicado encontrado"
        value = MockValue(
            codigo_iafa="IAFA004",
            ruc_proveedor="12345678904",
            codigo_afiliado="Afiliado101",
            fch_atencion="2024-04-30",
            monto=300.00,
            tipo_impuesto="ISC"
        )
        system = "solben_semefa"

        result = MessageHandler.message_case_4(message, value, system)

        assert "Duplicado encontrado" in result
        assert "300.0" in result
        assert "ISC" in result

    def test_message_case_1_no_attribute_error(self):
        message = "Duplicado encontrado"
        value = MockValue(
            codigo_iafa="IAFA001",
            ruc_proveedor="12345678901",
            nro_factu="F001-00001",
            codigo_afiliado="Afiliado123",
            fch_atencion="2024-01-15",
            nro_solben="SOL001",
            monto=100.50
        )
        system = "silux_semefa"

        try:
            result = MessageHandler.message_case_1(message, value, system)
            assert result is not None
            assert len(result) > 0
        except AttributeError as e:
            pytest.fail(f"AttributeError should not be raised: {e}")

    def test_message_case_2_no_attribute_error(self):
        message = "Atencion duplicada"
        value = MockValue(
            codigo_iafa="IAFA002",
            ruc_proveedor="12345678902",
            codigo_afiliado="Afiliado456",
            fch_atencion="2024-02-20",
            nro_solben="SOL002",
            monto=200.75
        )
        system = "solben_semefa"

        try:
            result = MessageHandler.message_case_2(message, value, system)
            assert result is not None
            assert len(result) > 0
        except AttributeError as e:
            pytest.fail(f"AttributeError should not be raised: {e}")
