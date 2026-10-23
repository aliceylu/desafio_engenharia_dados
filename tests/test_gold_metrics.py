import pytest
pytest.importorskip("pyspark")

from datetime import date, datetime
from decimal import Decimal

from src.transformations.gold import montar_gold_transacoes, montar_resumo_cliente_mes


def test_estorno_nao_entra_nos_indicadores(spark):
    transacoes = spark.createDataFrame([
        ("T1", "CAR1", datetime(2025, 3, 1), Decimal("100.00"), False,
         "Mercado", "5411", "Curitiba", "arquivo.csv", date(2025, 3, 1)),
        ("T2", "CAR1", datetime(2025, 3, 2), Decimal("80.00"), True,
         "Mercado", "5411", "Curitiba", "arquivo.csv", date(2025, 3, 2)),
    ], ["id_transacao", "id_cartao", "data_transacao", "valor", "estornada",
        "nome_estabelecimento", "mcc", "cidade_estabelecimento", "arquivo_origem", "data_referencia"])
    cartoes = spark.createDataFrame([("CAR1", "CTA1", "VISA", "CREDITO", "CANCELADO")],
                                     ["id_cartao", "id_conta", "bandeira", "tipo_cartao", "status_cartao"])
    contas = spark.createDataFrame([("CTA1", "CLI1", "CORRENTE", "ATIVA")],
                                    ["id_conta", "id_cliente", "tipo_conta", "status_conta"])
    clientes = spark.createDataFrame([("CLI1", "Ana", "Curitiba", "PR")],
                                      ["id_cliente", "nome", "cidade", "estado"])
    gold = montar_gold_transacoes(transacoes, cartoes, contas, clientes)
    resumo = montar_resumo_cliente_mes(gold).first()
    assert gold.count() == 1
    assert resumo["qtd_transacoes"] == 1
    assert resumo["valor_total"] == Decimal("100.00")
