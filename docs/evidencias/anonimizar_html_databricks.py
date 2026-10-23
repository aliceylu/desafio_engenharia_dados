#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Anonimiza emails e segmentos de usuario nos HTMLs exportados do Databricks.
"""

from __future__ import annotations

import argparse
import base64
import json
import re
import sys
from pathlib import Path
from urllib.parse import quote, unquote

EMAIL_RE = re.compile(r"(?i)\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b")
PASTA_USUARIO_RE = re.compile(
    r"(?i)((?:/|^)(?:Workspace/)?(?:Users|Repos)/)([^/\\\s\"'<>?#]+)"
)
MODELO_RE = re.compile(
    r"(?P<inicio>(?:window\.)?__DATABRICKS_NOTEBOOK_MODEL\s*=\s*)"
    r"(?P<aspas>['\"])(?P<conteudo>[^'\"]+)(?P=aspas)"
)


def anonimizar_texto(texto: str) -> tuple[str, int, int]:
    texto, emails = EMAIL_RE.subn('usuario_anonimo', texto)
    texto, pastas = PASTA_USUARIO_RE.subn(
        lambda m: m.group(1) + 'usuario_anonimo', texto
    )
    return texto, emails, pastas


def anonimizar_estrutura(objeto):
    """Percorre JSON sem alterar arrays, tipos nem quantidade de celulas."""
    if isinstance(objeto, dict):
        novo, total_emails, total_pastas = {}, 0, 0
        for chave, valor in objeto.items():
            chave_limpa, e, p = anonimizar_texto(chave)
            valor_limpo, e2, p2 = anonimizar_estrutura(valor)
            novo[chave_limpa] = valor_limpo
            total_emails += e + e2
            total_pastas += p + p2
        return novo, total_emails, total_pastas
    if isinstance(objeto, list):
        novos, total_emails, total_pastas = [], 0, 0
        for valor in objeto:
            val_limpo, e, p = anonimizar_estrutura(valor)
            novos.append(val_limpo)
            total_emails += e
            total_pastas += p
        return novos, total_emails, total_pastas
    if isinstance(objeto, str):
        return anonimizar_texto(objeto)
    return objeto, 0, 0


def decodificar_modelo(codificado: str):
    try:
        bruto = base64.b64decode(codificado, validate=True).decode('utf-8')
        return json.loads(unquote(bruto))
    except (ValueError, UnicodeError) as exc:
        raise ValueError('Modelo codificado do Databricks nao pode ser lido') from exc


def anonimizar_html(html: str) -> tuple[str, dict[str, int]]:
    metricas = {'emails': 0, 'usuarios_em_caminhos': 0, 'modelos': 0, 'celulas': 0}

    def substituir(m: re.Match) -> str:
        modelo = decodificar_modelo(m.group('conteudo'))
        novo, e, p = anonimizar_estrutura(modelo)
        celulas = len(modelo.get('commands', []))
        if len(novo.get('commands', [])) != celulas:
            raise ValueError('Quantidade de celulas alterada; operacao cancelada')
        codificado = base64.b64encode(
            quote(json.dumps(novo, ensure_ascii=False, separators=(',', ':')), safe='').encode('utf-8')
        ).decode('ascii')
        verificado = decodificar_modelo(codificado)
        if len(verificado.get('commands', [])) != celulas:
            raise ValueError('Falha na verificacao das celulas apos regravar')
        metricas['emails'] += e
        metricas['usuarios_em_caminhos'] += p
        metricas['modelos'] += 1