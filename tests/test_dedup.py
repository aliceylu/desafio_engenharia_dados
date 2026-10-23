import pytest
pytest.importorskip("pyspark")

from src.quality.rules import deduplicar


def test_cliente_seleciona_versao_mais_recente(spark):
    df = spark.createDataFrame([
        ("CLI001", "cidade_antiga", "2025-02-01 00:00:00", "2025-03-01", "clientes.csv", "h1"),
        ("CLI001", "cidade_nova", "2025-02-28 00:00:00", "2025-03-01", "clientes.csv", "h2"),
        ("CLI002", "cidade_outra", "2025-02-15 00:00:00", "2025-03-01", "clientes.csv", "h3"),
    ], ["id_cliente", "cidade", "data_atualizacao", "data_referencia", "arquivo_origem", "hash_linha"])
    resultado = deduplicar(df, "id_cliente", "data_atualizacao")
    assert resultado.count() == 2
    assert resultado.where("id_cliente = 'CLI001'").first()["cidade"] == "cidade_nova"


def test_transacao_duplicada_prefere_arquivo_mais_novo(spark):
    df = spark.createDataFrame([
        ("TRX001", "100.00", "2025-03-01", "transacoes_cartao_20250301.csv", "h1"),
        ("TRX001", "101.50", "2025-03-02", "transacoes_cartao_20250302.csv", "h2"),
    ], ["id_transacao", "valor", "data_referencia", "arquivo_origem", "hash_linha"])
    assert deduplicar(df, "id_transacao").first()["valor"] == "101.50"
