"""Logs estruturados em texto; falhas sempre propagadas para o Databricks Job."""
from __future__ import annotations

import logging
import sys
import time
from contextlib import contextmanager

FORMATO = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def get_logger(nome: str, nivel: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(nome)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(FORMATO))
        logger.addHandler(handler)
    logger.setLevel(nivel)
    logger.propagate = False
    return logger


def log_metricas(logger: logging.Logger, etapa: str, **contagens: int) -> int:
    """Registra contagens; devolve quantidade rejeitada para possível monitoramento."""
    partes = " ".join(f"{k}={v}" for k, v in contagens.items())
    logger.info("METRICAS etapa=%s %s", etapa, partes)
    rejeitados = int(contagens.get("rejeitados", 0))
    if rejeitados > 0:
        logger.warning("QUARENTENA etapa=%s rejeitados=%d", etapa, rejeitados)
    return rejeitados


@contextmanager
def log_etapa(logger: logging.Logger, etapa: str):
    inicio = time.perf_counter()
    logger.info("INICIO etapa=%s", etapa)
    try:
        yield
    except Exception:
        logger.exception("FALHA etapa=%s", etapa)
        raise
    else:
        logger.info("FIM etapa=%s duracao_s=%.2f", etapa, time.perf_counter() - inicio)
