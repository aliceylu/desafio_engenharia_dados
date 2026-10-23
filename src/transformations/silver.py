"""Prata: normalização, tipagem, escolha de versão, FK, estorno e quarentena."""
from __future__ import annotations

from functools import reduce

from pyspark.sql import functions as F

from src.utils.versoes import selecionar_versoes_vigentes
from src.quality.rules import aplicar_regras, cpf_valido, deduplicar, sem_referencia, texto_vazio
from src.utils.delta import sincronizar_snapshot, substituir_snapshot
from src.utils.logging import log_etapa, log_metricas

CHAVES = {
    "clientes": "id_cliente", "contas": "id_conta", "cartoes": "id_cartao",
    "transacoes": "id_transacao", "estornos": "id_estorno",
}
COLUNAS_TECNICAS = {"arquivo_origem", "data_referencia", "timestamp_ingestao",
                    "hash_linha", "arquivo_checksum"}


def preparar_entidade(df, entidade: str):
    """Retira somente chaves vazias; mantém versões no histórico Bronze."""
    chave = CHAVES[entidade]
    sem_chave = df.filter(texto_vazio(chave)).withColumn("motivo_erro", F.lit("CHAVE_OBRIGATORIA"))
    com_chave = df.filter(~texto_vazio(chave))
    data_col = "data_atualizacao" if entidade in ("clientes", "cartoes") else None
    return deduplicar(com_chave, chave, data_col), sem_chave


def criar_quarentena(df, entidade: str):
    chave = CHAVES[entidade]
    colunas_negocio = [c for c in df.columns if c not in COLUNAS_TECNICAS | {"motivo_erro", "_sem_referencia"}]
    return df.select(
        F.lit(entidade).alias("entidade"),
        F.col(chave).cast("string").alias("chave_registro"),
        "motivo_erro", "arquivo_origem", "data_referencia", "hash_linha",
        F.to_json(F.struct(*[F.col(c) for c in colunas_negocio]),
                  {"ignoreNullFields": "false"}).alias("dados_registro"),
    )


def _registrar_resultado(logger, nome: str, df_bronze, df_valido, rejeitados: int):
    lidos, gravados = df_bronze.count(), df_valido.count()
    log_metricas(logger, f"prata_{nome}", lidos=lidos, gravados=gravados, rejeitados=rejeitados, descartados_por_versao=max(0, lidos - gravados - rejeitados))


def tratar_clientes(raw):
    df, chaves_vazias = preparar_entidade(raw, "clientes")
    df = (df.withColumn("data_nascimento", F.expr("try_cast(data_nascimento as date)"))
          .withColumn("data_atualizacao", F.expr("try_cast(data_atualizacao as timestamp)"))
          .withColumn("cpf", F.trim(F.col("cpf"))))
    regras = [
        ("NOME_OBRIGATORIO", texto_vazio("nome")),
        ("CPF_INVALIDO", ~F.coalesce(cpf_valido(F.col("cpf")), F.lit(False))),
        ("DATA_NASCIMENTO_INVALIDA", F.col("data_nascimento").isNull()),
        ("DATA_ATUALIZACAO_INVALIDA", F.col("data_atualizacao").isNull()),
    ]
    validos, erros = aplicar_regras(df, regras)
    return validos, [erros, chaves_vazias]


def tratar_contas(raw, clientes):
    df, chaves_vazias = preparar_entidade(raw, "contas")
    df = (df.withColumn("status_conta", F.upper(F.trim(F.col("status_conta"))))
          .withColumn("tipo_conta", F.upper(F.trim(F.col("tipo_conta"))))
          .withColumn("data_abertura", F.expr("try_cast(data_abertura as date)")))
    df = sem_referencia(df, "id_cliente", clientes)
    regras = [
        ("TIPO_CONTA_INVALIDO", ~F.coalesce(F.col("tipo_conta").isin("CORRENTE", "POUPANCA"), F.lit(False))),
        ("STATUS_CONTA_INVALIDO", ~F.coalesce(F.col("status_conta").isin("ATIVA", "BLOQUEADA", "ENCERRADA"), F.lit(False))),
        ("DATA_ABERTURA_INVALIDA", F.col("data_abertura").isNull() | (F.col("data_abertura") > F.col("data_referencia"))),
        ("CLIENTE_INEXISTENTE_OU_INVALIDO", F.col("_sem_referencia")),
    ]
    validos, erros = aplicar_regras(df, regras)
    return validos.drop("_sem_referencia"), [erros, chaves_vazias]


def tratar_cartoes(raw, contas):
    df, chaves_vazias = preparar_entidade(raw, "cartoes")
    df = (df.withColumn("status_cartao", F.upper(F.trim(F.col("status_cartao"))))
          .withColumn("tipo_cartao", F.upper(F.trim(F.col("tipo_cartao"))))
          .withColumn("data_emissao", F.expr("try_cast(data_emissao as date)"))
          .withColumn("data_atualizacao", F.expr("try_cast(data_atualizacao as timestamp)")))
    df = sem_referencia(df, "id_conta", contas)
    regras = [
        ("STATUS_CARTAO_INVALIDO", ~F.coalesce(F.col("status_cartao").isin("ATIVO", "BLOQUEADO", "CANCELADO"), F.lit(False))),
        ("TIPO_CARTAO_INVALIDO", ~F.coalesce(F.col("tipo_cartao").isin("CREDITO", "DEBITO"), F.lit(False))),
        ("DATA_EMISSAO_INVALIDA", F.col("data_emissao").isNull()),
        ("DATA_ATUALIZACAO_INVALIDA", F.col("data_atualizacao").isNull()),
        ("CONTA_INEXISTENTE_OU_INVALIDA", F.col("_sem_referencia")),
    ]
    validos, erros = aplicar_regras(df, regras)
    return validos.drop("_sem_referencia"), [erros, chaves_vazias]


def tratar_transacoes(raw, cartoes):
    df, chaves_vazias = preparar_entidade(raw, "transacoes")
    df = (df.withColumn("data_transacao", F.expr("try_cast(data_transacao as timestamp)"))
          .withColumn("valor", F.expr("try_cast(valor as decimal(18,2))"))
          .withColumn("mcc", F.trim(F.col("mcc"))))
    df = sem_referencia(df, "id_cartao", cartoes)
    regras = [
        ("DATA_TRANSACAO_INVALIDA", F.col("data_transacao").isNull()),
        ("VALOR_INVALIDO", F.col("valor").isNull() | (F.col("valor") <= F.lit(0))),
        ("ESTABELECIMENTO_OBRIGATORIO", texto_vazio("nome_estabelecimento")),
        ("MCC_INVALIDO", ~F.coalesce(F.col("mcc").rlike(r"^[0-9]{4}$"), F.lit(False))),
        ("CARTAO_INEXISTENTE_OU_INVALIDO", F.col("_sem_referencia")),
    ]
    validos, erros = aplicar_regras(df, regras)
    return validos.drop("_sem_referencia"), [erros, chaves_vazias]



def tratar_estornos(raw, transacoes):
    df, chaves_vazias = preparar_entidade(raw, "estornos")

    df = df.withColumn("data_estorno", F.expr("try_cast(data_estorno as date)"))

    df = sem_referencia(df, "id_transacao", transacoes)

    datas_transacoes = transacoes.select("id_transacao", F.to_date("data_transacao").alias("_data_transacao"))

    df = df.join(datas_transacoes, on="id_transacao", how="left")

    regras = [
        ("DATA_ESTORNO_INVALIDA",F.col("data_estorno").isNull()), ("MOTIVO_ESTORNO_OBRIGATORIO", texto_vazio("motivo_estorno")),
        ("TRANSACAO_INEXISTENTE_OU_INVALIDA",F.col("_sem_referencia")),
        ("ESTORNO_ANTERIOR_TRANSACAO",F.col("data_estorno").isNotNull() & F.col("_data_transacao").isNotNull() & (F.col("data_estorno") < F.col("_data_transacao")))
    ]

    validos, erros = aplicar_regras(df, regras)

    return (validos.drop("_sem_referencia", "_data_transacao"), [erros.drop("_data_transacao"), chaves_vazias])


def executar_prata(spark, cfg, logger):
    validados = {}
    quarentenas = []
    for nome in ("clientes", "contas", "cartoes", "transacoes", "estornos"):
        with log_etapa(logger, f"prata_{nome}"):
            raw = selecionar_versoes_vigentes(
                spark.table(cfg.tabela("bronze", nome))
            ).drop("id_ingestao")
            if nome == "clientes":
                bons, ruins = tratar_clientes(raw)
            elif nome == "contas":
                bons, ruins = tratar_contas(raw, validados["clientes"])
            elif nome == "cartoes":
                bons, ruins = tratar_cartoes(raw, validados["contas"])
            elif nome == "transacoes":
                bons, ruins = tratar_transacoes(raw, validados["cartoes"])
            else:
                bons, ruins = tratar_estornos(raw, validados["transacoes"])

            rejeitados = sum(df.count() for df in ruins)
            q_entidade = reduce(lambda a, b: a.unionByName(b), [criar_quarentena(df, nome) for df in ruins])
            quarentenas.append(q_entidade)
            validados[nome] = bons
            _registrar_resultado(logger, nome, raw, bons, rejeitados)

    estornadas = (validados["estornos"].select("id_transacao").distinct().withColumn("estornada", F.lit(True)))

    validados["transacoes"] = (validados["transacoes"].join(estornadas, on="id_transacao", how="left").withColumn("estornada", F.coalesce(F.col("estornada"), F.lit(False))))

    for nome in ("clientes", "contas", "cartoes", "transacoes", "estornos"):
        with log_etapa(logger, f"merge_prata_{nome}"):
            sincronizar_snapshot(spark, validados[nome], cfg.tabela("prata", nome), CHAVES[nome])

    quarentena = reduce(lambda a, b: a.unionByName(b), quarentenas)
    with log_etapa(logger, "atualizar_quarentena"):
        substituir_snapshot(quarentena, cfg.tabela("prata", "quarentena"))
        log_metricas(logger, "prata_quarentena", rejeitados=quarentena.count())
