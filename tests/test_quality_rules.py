import pytest
pytest.importorskip("pyspark")

from pyspark.sql import functions as F

from src.quality.rules import cpf_valido
from src.transformations.silver import tratar_transacoes


def test_cpf_digitos_verificadores_e_repetidos(spark):
    df = spark.createDataFrame([
        ("52998224725",),  # CPF de exemplo usado em testes
        ("11111111111",),
        ("52998224726",),
        ("123456789",),
    ], ["cpf"])
    saida = df.withColumn("valido", cpf_valido(F.col("cpf"))).collect()
    assert [bool(linha["valido"]) for linha in saida] == [True, False, False, False]


def test_transacoes_qualidade_e_integridade_referencial(spark):
    raw = spark.createDataFrame([
        ("T1", "CAR1", "2025-03-01 11:30:00", "100.00", "Loja", "5411", "Curitiba", "l1"),
        ("T2", "CAR1", "2025-03-01 12:00:00", "-1.00", "Loja", "5411", "Curitiba", "l2"),
        ("T3", "CAR1", "2025-13-45 10:00:00", "25.00", "Loja", "5411", "Curitiba", "l3"),
        ("T4", "CAR9999", "2025-03-01 12:20:00", "100.00", "Loja", "5411", "Curitiba", "l4"),
    ], ["id_transacao", "id_cartao", "data_transacao", "valor", "nome_estabelecimento",
        "mcc", "cidade_estabelecimento", "hash_linha"])
    raw = (raw.withColumn("data_referencia", F.lit("2025-03-01").cast("date"))
           .withColumn("arquivo_origem", F.lit("transacoes_cartao_20250301.csv")))
    cartoes = spark.createDataFrame([("CAR1",)], ["id_cartao"])
    validas, rejeitadas = tratar_transacoes(raw, cartoes)
    assert validas.count() == 1
    erros = [r["motivo_erro"] for frame in rejeitadas for r in frame.collect()]
    assert any("VALOR_INVALIDO" in e for e in erros)
    assert any("DATA_TRANSACAO_INVALIDA" in e for e in erros)
    assert any("CARTAO_INEXISTENTE_OU_INVALIDO" in e for e in erros)


from src.transformations.silver import tratar_estornos


def test_estorno_anterior_transacao_vai_quarentena(spark):
    """
    Valida três situações:
    - Estorno posterior à transação: válido.
    - Estorno anterior à transação: rejeitado.
    - Estorno sem transação correspondente: rejeitado.
    """

    raw = spark.createDataFrame(
        [
            ("EST1", "TRX1", "2025-03-03", "CANCELAMENTO", "h1"),
            ("EST2", "TRX1", "2025-03-01", "FRAUDE", "h2"),
            ("EST3", "TRX999", "2025-03-03", "FRAUDE", "h3")
        ],
        [
            "id_estorno",
            "id_transacao",
            "data_estorno",
            "motivo_estorno",
            "hash_linha"
        ]
    )

    raw = (raw.withColumn("data_referencia", F.lit("2025-03-03").cast("date")).withColumn("arquivo_origem", F.lit("estornos.csv")))

    transacoes = spark.createDataFrame(
        [
            ("TRX1", "2025-03-02 10:00:00")
        ],
        ["id_transacao", "data_transacao"]
    )

    validos, rejeitados = tratar_estornos(raw, transacoes)

    ids_validos = {r["id_estorno"] for r in validos.select("id_estorno").collect()}

    motivos = {
        r["id_estorno"]: r["motivo_erro"]
        for df in rejeitados
        for r in df.select("id_estorno", "motivo_erro").collect()
    }

    assert ids_validos == {"EST1"}

    assert ("ESTORNO_ANTERIOR_TRANSACAO" in motivos["EST2"])

    assert ("TRANSACAO_INEXISTENTE_OU_INVALIDA"in motivos["EST3"])
