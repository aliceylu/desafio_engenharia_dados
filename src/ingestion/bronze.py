"""Bronze fiel ao CSV: strings, duplicatas preservadas, metadados e replay por arquivo."""
from __future__ import annotations

import hashlib

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.utils.delta import existe_tabela
from src.utils.logging import log_etapa, log_metricas
from uuid import uuid4

COLUNAS = {
    "clientes": ("id_cliente", "nome", "cpf", "data_nascimento", "cidade", "estado", "data_atualizacao"),
    "contas": ("id_conta", "id_cliente", "tipo_conta", "status_conta", "data_abertura"),
    "cartoes": ("id_cartao", "id_conta", "bandeira", "tipo_cartao", "status_cartao", "data_emissao", "data_atualizacao"),
    "transacoes": ("id_transacao", "id_cartao", "data_transacao", "valor", "nome_estabelecimento", "mcc", "cidade_estabelecimento"),
    "estornos": ("id_estorno", "id_transacao", "data_estorno", "motivo_estorno"),
}


def checksum_arquivo(caminho: str) -> str:
    digest = hashlib.sha256()
    with open(caminho, "rb") as arquivo:
        for bloco in iter(lambda: arquivo.read(1024 * 1024), b""):
            digest.update(bloco)
    return digest.hexdigest()


def ler_csv_raw(spark, caminho: str, colunas: tuple[str, ...]):
    schema = StructType([StructField(c, StringType(), True) for c in colunas])
    return (spark.read.option("header", "true")
            .option("mode", "FAILFAST")
            .option("enforceSchema", "false")
            .schema(schema).csv(caminho))


def ingerir_arquivo(spark, cfg, logger, entidade: str, nome_arquivo: str) -> None:
    """Ingere uma nova versão apenas se o conteúdo mudou.

    A Bronze mantém todas as linhas do CSV e todos os lotes recebidos.
    A tabela é criada na primeira carga; cargas seguintes usam append.
    """
    tabela = cfg.tabela("bronze", entidade)
    caminho = cfg.caminho_raw(nome_arquivo)

    with log_etapa(logger, f"bronze_{entidade}_{nome_arquivo}"):
        checksum = checksum_arquivo(caminho)
        tabela_existe = existe_tabela(spark, tabela)

        if tabela_existe:
            anterior = (
                spark.table(tabela)
                .where(F.col("arquivo_origem") == nome_arquivo)
                .orderBy(F.col("timestamp_ingestao").desc_nulls_last())
                .select("arquivo_checksum")
                .first()
            )
            if anterior and anterior["arquivo_checksum"] == checksum:
                logger.info(
                    "ARQUIVO_SEM_ALTERACAO arquivo=%s tabela=%s",
                    nome_arquivo,
                    tabela,
                )
                return

        # Fora de 'if tabela_existe': deve funcionar na PRIMEIRA carga.
        base = ler_csv_raw(spark, caminho, COLUNAS[entidade])
        id_ingestao = uuid4().hex
        dados = (
            base
            .withColumn("arquivo_origem", F.lit(nome_arquivo))
            .withColumn("arquivo_checksum", F.lit(checksum))
            .withColumn("id_ingestao", F.lit(id_ingestao))
            .withColumn("data_referencia", F.lit(cfg.data_referencia.isoformat()).cast("date"))
            .withColumn("timestamp_ingestao", F.current_timestamp())
            .withColumn(
                "hash_linha",
                F.sha2(
                    F.to_json(
                        F.struct(*[F.col(c) for c in COLUNAS[entidade]]),
                        {"ignoreNullFields": "false"},
                    ),
                    256,
                ),
            )
        )

        qtd = dados.count()
        writer = dados.write.format("delta")
        if tabela_existe:
            writer.mode("append").option("mergeSchema", "true").saveAsTable(tabela)
        else:
            # Primeira criação: ainda não existem registros a preservar.
            writer.mode("overwrite").saveAsTable(tabela)

        log_metricas(
            logger,
            f"bronze_{entidade}",
            lidos=qtd,
            gravados=qtd,
            rejeitados=0,
        )


def executar_bronze(spark, cfg, logger):
    arquivos = (
        ("clientes", "clientes.csv"),
        ("contas", "contas.csv"),
        ("cartoes", "cartoes.csv"),
        ("transacoes", cfg.arquivo_transacoes()),
        ("estornos", "estornos.csv"),
    )
    for entidade, arquivo in arquivos:
        ingerir_arquivo(spark, cfg, logger, entidade, arquivo)
