# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
import os
import sys
from pathlib import Path

diretorio = Path(os.getcwd()).resolve()

repo = next(
    (p for p in (diretorio, *diretorio.parents)
     if (p / "main.py").is_file()),
    None
)

if repo is None:
    raise FileNotFoundError(
        "main.py não encontrado. Verifique a estrutura do repositório."
    )

if str(repo) not in sys.path:
    sys.path.insert(0, str(repo))

print(f"Repositório encontrado: {repo}")

# COMMAND ----------

arquivos = dbutils.fs.ls("/Volumes/novarota/bronze/raw/")

display(arquivos)

# COMMAND ----------

from main import run

run(spark, data_referencia="2025-03-01")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM novarota.bronze.transacoes;
# MAGIC
# MAGIC SELECT * FROM novarota.prata.quarentena;
# MAGIC
# MAGIC SELECT * FROM novarota.ouro.gold_transacoes_cartao;

# COMMAND ----------

run(spark, data_referencia="2025-03-02")
run(spark, data_referencia="2025-03-03")

# COMMAND ----------

display(spark.table("novarota.ouro.gold_resumo_cliente_mes"))

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/novarota/bronze/raw/"))

# COMMAND ----------

from main import run

tabelas_verificacao = [
    "novarota.bronze.transacoes",
    "novarota.prata.transacoes",
    "novarota.prata.quarentena",
    "novarota.ouro.gold_transacoes_cartao",
    "novarota.ouro.gold_resumo_cliente_mes"
]

antes = {
    tabela: spark.table(tabela).count()
    for tabela in tabelas_verificacao
}

# Reprocessando o segundo dia
run(spark, data_referencia="2025-03-02")

depois = {
    tabela: spark.table(tabela).count()
    for tabela in tabelas_verificacao
}

resultado = [
    (tabela, antes[tabela], depois[tabela])
    for tabela in tabelas_verificacao
]

display(
    spark.createDataFrame(
        resultado,
        ["tabela", "antes", "depois"]
    )
)

assert antes == depois, "Foram encontradas diferenças após o reprocessamento!"

print("Validação de contagens após reprocessamento: OK")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY novarota.prata.transacoes;

# COMMAND ----------

from main import run

run(spark, data_referencia="2025-03-03")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     entidade,
# MAGIC     motivo_erro,
# MAGIC     COUNT(*) AS quantidade
# MAGIC FROM novarota.prata.quarentena
# MAGIC WHERE entidade = 'estornos'
# MAGIC GROUP BY entidade, motivo_erro
# MAGIC ORDER BY quantidade DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     arquivo_origem,
# MAGIC     COUNT(*) AS quantidade_registros
# MAGIC FROM novarota.bronze.transacoes
# MAGIC GROUP BY arquivo_origem
# MAGIC ORDER BY arquivo_origem;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE novarota.bronze.transacoes;