"""Configuração central do pipeline NovaRota (Unity Catalog/Databricks)."""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date

CAMADAS = ("bronze", "prata", "ouro")
TABELAS = {
    "bronze": ("clientes", "contas", "cartoes", "transacoes", "estornos"),
    "prata": ("clientes", "contas", "cartoes", "transacoes", "estornos", "quarentena"),
    "ouro": (
        "gold_transacoes_cartao", "gold_resumo_cliente_mes",
        "gold_ranking_estabelecimento", "gold_alertas_qualidade",
    ),
}
DEFAULTS_AMBIENTE = {
    "dev": {"catalogo": "novarota", "volume_raw": "/Volumes/novarota/bronze/raw"},
}


@dataclass(frozen=True)
class Config:
    ambiente: str
    catalogo: str
    volume_raw: str
    data_referencia: date
    schema_bronze: str = "bronze"
    schema_prata: str = "prata"
    schema_ouro: str = "ouro"

    def schema(self, camada: str) -> str:
        mapa = {"bronze": self.schema_bronze, "prata": self.schema_prata, "ouro": self.schema_ouro}
        if camada not in mapa:
            raise ValueError(f"Camada inválida: {camada!r}; opções: {CAMADAS}")
        return mapa[camada]

    def tabela(self, camada: str, nome: str) -> str:
        if camada not in TABELAS or nome not in TABELAS[camada]:
            raise ValueError(f"Tabela não configurada: {camada}.{nome}")
        return f"{self.catalogo}.{self.schema(camada)}.{nome}"

    def caminho_raw(self, nome_arquivo: str) -> str:
        return f"{self.volume_raw.rstrip('/')}/{nome_arquivo}"

    def arquivo_transacoes(self) -> str:
        return f"transacoes_cartao_{self.data_referencia:%Y%m%d}.csv"

    def caminho_transacoes(self) -> str:
        # Os arquivos YYYYMMDD.csv estão na raiz do volume, não em subpastas.
        return self.caminho_raw(self.arquivo_transacoes())


def _ler_parametro(nome: str, default: str | None = None, dbutils=None) -> str | None:
    if dbutils is not None:
        try:
            valor = dbutils.widgets.get(nome)
            if valor:
                return valor
        except Exception:  # Widget inexistente: variáveis de ambiente/defaults
            pass
    return os.getenv(nome.upper()) or default


def _parse_data(valor: str | date) -> date:
    if isinstance(valor, date):
        return valor
    try:
        return date.fromisoformat(valor)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"data_referencia inválida: {valor!r}. Use YYYY-MM-DD") from exc


def get_config(*, data_referencia: str | date | None = None,
               ambiente: str | None = None, dbutils=None) -> Config:
    ambiente = ambiente or _ler_parametro("ambiente", "dev", dbutils)
    if ambiente not in DEFAULTS_AMBIENTE:
        raise ValueError(f"Ambiente não configurado: {ambiente!r}")
    base = DEFAULTS_AMBIENTE[ambiente]
    data_ref = data_referencia or _ler_parametro("data_referencia", None, dbutils)
    if data_ref is None:
        raise ValueError("Informe data_referencia em YYYY-MM-DD")
    return Config(
        ambiente=ambiente,
        catalogo=_ler_parametro("catalogo", base["catalogo"], dbutils),
        volume_raw=_ler_parametro("volume_raw", base["volume_raw"], dbutils),
        data_referencia=_parse_data(data_ref),
        schema_bronze=_ler_parametro("schema_bronze", "bronze", dbutils),
        schema_prata=_ler_parametro("schema_prata", "prata", dbutils),
        schema_ouro=_ler_parametro("schema_ouro", "ouro", dbutils),
    )
