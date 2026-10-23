"""Regras de qualidade 100% Spark SQL, sem UDF Python."""
from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql import Window


def texto_vazio(coluna: str):
    return F.col(coluna).isNull() | (F.trim(F.col(coluna)) == "")


def cpf_valido(cpf_col):
    """Validação de 11 dígitos, sequência repetida e dois dígitos verificadores."""
    texto = F.regexp_replace(cpf_col, r"[^0-9]", "")
    formato = cpf_col.rlike(r"^[0-9]{11}$")
    repetido = cpf_col.rlike(r"^(\d)\1{10}$")
    soma_1 = sum((10 - i) * F.substring(texto, i + 1, 1).cast("int") for i in range(9))
    dv_1 = (soma_1 * 10) % 11
    dv_1 = F.when(dv_1 == 10, F.lit(0)).otherwise(dv_1)
    soma_2 = sum((11 - i) * F.substring(texto, i + 1, 1).cast("int") for i in range(10))
    dv_2 = (soma_2 * 10) % 11
    dv_2 = F.when(dv_2 == 10, F.lit(0)).otherwise(dv_2)
    return (formato & ~repetido
            & (dv_1 == F.substring(texto, 10, 1).cast("int"))
            & (dv_2 == F.substring(texto, 11, 1).cast("int")))


def deduplicar(df, chave: str, coluna_atualizacao: str | None = None):
    """Uma linha por chave; versão atualizada vence e hash desempata.

    Se não há data de atualização, prioridade para arquivo de carga mais recente.
    Entradas sem chave são tratadas antes desta função pela rotina de qualidade.
    """
    ordem = []
    if coluna_atualizacao:
        ordem.append(F.expr(f"try_cast(`{coluna_atualizacao}` as timestamp)").desc_nulls_last())
    ordem += [F.col("data_referencia").desc_nulls_last(),
              F.col("arquivo_origem").desc(), F.col("hash_linha").desc()]
    w = Window.partitionBy(chave).orderBy(*ordem)
    return (df.withColumn("_numero_versao", F.row_number().over(w))
            .filter(F.col("_numero_versao") == 1).drop("_numero_versao"))


def aplicar_regras(df, regras: list[tuple[str, object]]):
    """Adiciona motivo_erro, separando registros válidos/rejeitados."""
    motivos = [F.when(F.coalesce(condicao, F.lit(True)), F.lit(rotulo))
               for rotulo, condicao in regras]
    marcados = df.withColumn("motivo_erro", F.concat_ws("; ", *motivos))
    validos = marcados.filter(F.col("motivo_erro") == "").drop("motivo_erro")
    invalidos = marcados.filter(F.col("motivo_erro") != "")
    return validos, invalidos


def sem_referencia(df, coluna: str, dimensao):
    """Flags de chave estrangeira inválida, sem duplicar registros."""
    dim = dimensao.select(coluna).distinct().withColumn("_ref_encontrada", F.lit(True))
    return (df.join(dim, on=coluna, how="left")
            .withColumn("_sem_referencia", F.col("_ref_encontrada").isNull())
            .drop("_ref_encontrada"))
