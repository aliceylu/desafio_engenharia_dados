"""Marts analíticos recomputados como snapshots sobre a Prata consolidada."""
from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql import Window

from src.utils.delta import substituir_snapshot
from src.utils.logging import log_etapa, log_metricas


def montar_gold_transacoes(transacoes, cartoes, contas, clientes):
    """Somente transações validadas e não estornadas entram na visão financeira."""
    return (transacoes.alias("t")
            .filter(~F.col("t.estornada"))
            .join(cartoes.alias("ca"), F.col("t.id_cartao") == F.col("ca.id_cartao"))
            .join(contas.alias("co"), F.col("ca.id_conta") == F.col("co.id_conta"))
            .join(clientes.alias("cl"), F.col("co.id_cliente") == F.col("cl.id_cliente"))
            .select(
                F.col("t.id_transacao"), F.col("t.data_transacao"),
                F.date_format(F.col("t.data_transacao"), "yyyy-MM").alias("ano_mes"),
                F.col("t.valor"), F.col("cl.id_cliente"),
                F.col("cl.nome").alias("nome_cliente"),
                F.col("cl.cidade").alias("cidade_cliente"), F.col("cl.estado"),
                F.col("co.id_conta"), F.col("co.tipo_conta"), F.col("co.status_conta"),
                F.col("ca.id_cartao"), F.col("ca.bandeira"), F.col("ca.tipo_cartao"),
                F.col("ca.status_cartao"), F.col("t.nome_estabelecimento"),
                F.col("t.mcc"), F.col("t.cidade_estabelecimento"),
                F.col("t.arquivo_origem"), F.col("t.data_referencia")))


def montar_resumo_cliente_mes(gold):
    return (gold.groupBy("id_cliente", "nome_cliente", "ano_mes")
            .agg(F.count("id_transacao").alias("qtd_transacoes"),
                 F.sum("valor").cast("decimal(20,2)").alias("valor_total"),
                 F.avg("valor").cast("decimal(20,2)").alias("ticket_medio"),
                 F.countDistinct("id_cartao").alias("qtd_cartoes_utilizados")))


def montar_ranking_estabelecimento(gold):
    agg = (gold.groupBy("ano_mes", "cidade_estabelecimento", "nome_estabelecimento")
           .agg(F.count("id_transacao").alias("qtd_transacoes"),
                F.sum("valor").cast("decimal(20,2)").alias("valor_total")))
    w = Window.partitionBy("ano_mes", "cidade_estabelecimento").orderBy(
        F.col("valor_total").desc(), F.col("qtd_transacoes").desc())
    return agg.withColumn("ranking_cidade", F.dense_rank().over(w))


def montar_alertas_qualidade(quarentena):
    return (quarentena
            .withColumn("tipo_erro", F.explode(F.split(F.col("motivo_erro"), "; ")))
            .groupBy("data_referencia", "entidade", "tipo_erro")
            .agg(F.count("*").alias("qtd_rejeitados")))


def executar_ouro(spark, cfg, logger):
    silver = {nome: spark.table(cfg.tabela("prata", nome))
              for nome in ("clientes", "contas", "cartoes", "transacoes", "quarentena")}
    ouro = {
        "gold_transacoes_cartao": montar_gold_transacoes(
            silver["transacoes"], silver["cartoes"], silver["contas"], silver["clientes"]),
    }
    ouro["gold_resumo_cliente_mes"] = montar_resumo_cliente_mes(ouro["gold_transacoes_cartao"])
    ouro["gold_ranking_estabelecimento"] = montar_ranking_estabelecimento(ouro["gold_transacoes_cartao"])
    ouro["gold_alertas_qualidade"] = montar_alertas_qualidade(silver["quarentena"])
    for nome, df in ouro.items():
        with log_etapa(logger, f"ouro_{nome}"):
            substituir_snapshot(df, cfg.tabela("ouro", nome))
            log_metricas(logger, f"ouro_{nome}", gravados=spark.table(cfg.tabela("ouro", nome)).count(), rejeitados=0)
