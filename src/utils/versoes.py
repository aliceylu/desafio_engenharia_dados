from pyspark.sql import functions as F
from pyspark.sql.window import Window


def selecionar_versoes_vigentes(df):
    if "id_ingestao" not in df.columns:
        df = df.withColumn(
            "id_ingestao", F.lit(None).cast("string")
        )

    df = df.withColumn(
        "_lote",
        F.coalesce(
            F.col("id_ingestao"),
            F.concat(F.lit("legado:"), F.col("arquivo_origem"))
        )
    )

    versoes = (
        df.groupBy("arquivo_origem", "_lote")
          .agg(
              F.max("timestamp_ingestao").alias("_ultima_ingestao")
          )
    )

    janela = (
        Window.partitionBy("arquivo_origem")
              .orderBy(
                  F.col("_ultima_ingestao").desc(),
                  F.col("_lote").desc()
              )
    )

    vigentes = (
        versoes.withColumn(
            "_ordem", F.row_number().over(janela)
        )
        .filter(F.col("_ordem") == 1)
        .select("arquivo_origem", "_lote")
    )

    return (
        df.join(
            vigentes,
            on=["arquivo_origem", "_lote"],
            how="inner"
        )
        .drop("_lote")
    )
