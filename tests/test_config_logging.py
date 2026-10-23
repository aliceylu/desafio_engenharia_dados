from datetime import date
from unittest.mock import Mock

import pytest

from src.config import get_config
from src.utils.logging import log_metricas


def test_config_caminhos_unity_catalog():
    cfg = get_config(data_referencia="2025-03-02")
    assert cfg.data_referencia == date(2025, 3, 2)
    assert cfg.caminho_transacoes() == "/Volumes/novarota/bronze/raw/transacoes_cartao_20250302.csv"
    assert cfg.tabela("prata", "transacoes") == "novarota.prata.transacoes"
    assert cfg.tabela("ouro", "gold_resumo_cliente_mes") == "novarota.ouro.gold_resumo_cliente_mes"


def test_config_data_invalida():
    with pytest.raises(ValueError):
        get_config(data_referencia="2025-13-77")


def test_log_metricas_alerta_quarentena():
    logger = Mock()
    resultado = log_metricas(logger, "prata_clientes", lidos=40, rejeitados=4)
    assert resultado == 4
    logger.warning.assert_called_once()
