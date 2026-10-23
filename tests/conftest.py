import pytest

@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")

    from pyspark.sql import SparkSession

    sessao = SparkSession.getActiveSession()

    if sessao is None:
        sessao = SparkSession.builder.getOrCreate()

    return sessao
