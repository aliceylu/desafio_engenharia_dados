from __future__ import annotations

from delta.tables import DeltaTable


def preparar_schemas(spark, cfg):
    for camada in ("bronze", "prata", "ouro"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalogo}.{cfg.schema(camada)}")


def existe_tabela(spark, nome_completo: str) -> bool:
    return spark.catalog.tableExists(nome_completo)


def sincronizar_snapshot(spark, df, tabela: str, chave: str) -> None:
    if not existe_tabela(spark, tabela):
        df.write.format("delta").mode("overwrite").saveAsTable(tabela)
        return
    (DeltaTable.forName(spark, tabela).alias("t")
     .merge(df.alias("s"), f"t.`{chave}` = s.`{chave}`")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .whenNotMatchedBySourceDelete()
     .execute())


def substituir_snapshot(df, tabela: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela)
