"""Ponto de entrada do job; importe main.run(spark, data_referencia='YYYY-MM-DD')."""
from __future__ import annotations

import argparse

from src.config import get_config
from src.ingestion.bronze import executar_bronze
from src.transformations.silver import executar_prata
from src.transformations.gold import executar_ouro
from src.utils.delta import preparar_schemas
from src.utils.logging import get_logger, log_etapa


def run(spark, *, data_referencia: str, ambiente: str = "dev", dbutils=None):
    cfg = get_config(data_referencia=data_referencia, ambiente=ambiente, dbutils=dbutils)
    logger = get_logger("novarota")
    with log_etapa(logger, f"pipeline_{data_referencia}"):
        preparar_schemas(spark, cfg)
        executar_bronze(spark, cfg, logger)
        executar_prata(spark, cfg, logger)
        executar_ouro(spark, cfg, logger)
    return cfg


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline NovaRota")
    parser.add_argument("--data-referencia", required=True, help="YYYY-MM-DD")
    parser.add_argument("--ambiente", default="dev")
    args = parser.parse_args()
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    run(spark, data_referencia=args.data_referencia, ambiente=args.ambiente)
