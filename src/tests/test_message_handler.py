import pytest
from unittest.mock import MagicMock
from utils.message_handler import MessageHandler


class MockValue:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class TestMessageHandlerCase1:
    def test_with_all_fields(self):
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
        system = "silux_protecta"

        result = MessageHandler.message_case_1(message, value, system)

        assert "Duplicado encontrado" in result
        assert "IAFA001" in result
        assert "12345678901" in result
        assert "F001-00001" in result
        assert "Afiliado123" in result
        assert "SOL001" in result
        assert "100.5" in result

    def test_with_missing_optional_field(self):
        message = "Duplicado"
        value = MockValue(
            codigo_iafa="IAFA001",
            ruc_proveedor="12345678901",
            nro_factu="F001-00001",
            codigo_afiliado="Afiliado123",
            fch_atencion="2024-01-15",
            nro_solben="SOL001"
        )
        system = "silux_protecta"

        result = MessageHandler.message_case_1(message, value, system)
        
        assert result is not None
        assert len(result) > 0


class TestMessageHandlerCase2:
    def test_with_all_fields(self):
        message = "Atencion duplicada"
        value = MockValue(
            codigo_iafa="IAFA002",
            ruc_proveedor="12345678902",
            codigo_afiliado="Afiliado456",
            fch_atencion="2024-02-20",
            nro_solben="SOL002",
            monto=200.75
        )
        system = "solben_protecta"

        result = MessageHandler.message_case_2(message, value, system)

        assert "Atencion duplicada" in result
        assert "IAFA002" in result
        assert "Afiliado456" in result
        assert "200.75" in result

    def test_with_zero_monto(self):
        message = "Duplicado"
        value = MockValue(
            codigo_iafa="IAFA003",
            ruc_proveedor="12345678903",
            codigo_afiliado="Afiliado789",
            fch_atencion="2024-03-25",
            nro_solben="SOL003",
            monto=0.0
        )
        system = "silux_protecta"

        result = MessageHandler.message_case_2(message, value, system)
        
        assert "0.0" in result


class TestMessageHandlerCase3:
    def test_with_tax_type(self):
        message = "Duplicado encontrado"
        value = MockValue(
            codigo_iafa="IAFA003",
            ruc_proveedor="12345678903",
            codigo_afiliado="Afiliado789",
            fch_atencion="2024-03-25",
            nro_solben="SOL003",
            tipo_impuesto="IGV"
        )
        system = "silux_protecta"

        result = MessageHandler.message_case_3(message, value, system)

        assert "Duplicado encontrado" in result
        assert "IGV" in result


class TestMessageHandlerCase4:
    def test_with_amount_and_tax_type(self):
        message = "Duplicado encontrado"
        value = MockValue(
            codigo_iafa="IAFA004",
            ruc_proveedor="12345678904",
            codigo_afiliado="Afiliado101",
            fch_atencion="2024-04-30",
            monto=300.00,
            tipo_impuesto="ISC"
        )
        system = "solben_protecta"

        result = MessageHandler.message_case_4(message, value, system)

        assert "Duplicado encontrado" in result
        assert "300.0" in result
        assert "ISC" in result


class TestMessageHandlerEdgeCases:
    def test_empty_message(self):
        value = MockValue(codigo_iafa="TEST")
        result = MessageHandler.message_case_1("", value, "system")
        assert result is not None

    def test_none_values_handling(self):
        value = MockValue(codigo_iafa=None, ruc_proveedor=None)
        result = MessageHandler.message_case_1("Test", value, "system")
        assert result is not None

    def test_different_systems(self):
        message = "Test"
        value = MockValue(codigo_iafa="IAFA001")
        
        for system in ["silux_protecta", "solben_protecta", "unknown_system"]:
            result = MessageHandler.message_case_1(message, value, system)
            assert system in result
