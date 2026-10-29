# -*- coding: utf-8 -*-
"""Backend Flask do Dashboard ClickUp: filtro por data, retries e cache de contingência."""
import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from io import BytesIO

import requests
from flask import Flask, jsonify, render_template, request, send_file

app = Flask(__name__, template_folder="templates")

API_BASE = "https://api.clickup.com/api/v2"
DIAS_MINIMOS = 100
CONNECT_TIMEOUT = 4
READ_TIMEOUT = 8
MAX_RETRIES = 2
CACHE_TTL_SECONDS = 300
MAX_PAGES = 10
MAX_FETCH_SECONDS = 75  # impede uma atualização de consumir todo o timeout do Gunicorn

STATUS_INATIVOS = {
    "cancelado", "cancelada", "cancelados", "canceladas",
    "cancelled", "canceled", "finalizado", "finalizada",
    "finalizados", "finalizadas", "closed", "done",
    "concluido", "concluida", "concluídos", "concluídas",
    "complete", "completed",
}

_cache_lock = threading.Lock()
_refresh_lock = threading.Lock()
_cache = {"at": 0.0, "tasks": None, "updated_at": None}


def _safe_json(response):
    """Valida HTTP e JSON, evitando tratar HTML de erro como JSON."""
    response.raise_for_status()
    try:
        payload = response.json()
    except (requests.exceptions.JSONDecodeError, ValueError) as exc:
        content_type = response.headers.get("Content-Type", "")
        app.logger.warning(
            "Resposta não-JSON do ClickUp (HTTP %s, Content-Type %s).",
            response.status_code, content_type
        )
        raise ValueError("O ClickUp retornou uma resposta que não é JSON válido.") from exc

    if not isinstance(payload, dict):
        raise ValueError("Resposta inesperada da API do ClickUp.")
    return payload


def _get_json(session, url, params=None):
    """Repete falhas transitórias de rede, HTTP 429 e HTTP 5xx."""
    last_error = None

    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(
                url,
                params=params,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            )

            transient = response.status_code == 429 or 500 <= response.status_code <= 599
            if transient and attempt < MAX_RETRIES - 1:
                retry_after = response.headers.get("Retry-After")
                try:
                    delay = min(8.0, max(1.0, float(retry_after))) if retry_after else 2 ** attempt
                except (TypeError, ValueError):
                    delay = 2 ** attempt
                app.logger.warning(
                    "ClickUp respondeu HTTP %s; nova tentativa em %.1fs (%s/%s).",
                    response.status_code, delay, attempt + 1, MAX_RETRIES
                )
                time.sleep(delay)
                continue

            return _safe_json(response)

        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = exc
            if attempt >= MAX_RETRIES - 1:
                raise
            delay = 2 ** attempt
            app.logger.warning(
                "Falha temporária de rede com ClickUp (%s); nova tentativa em %ss (%s/%s).",
                type(exc).__name__, delay, attempt + 1, MAX_RETRIES
            )
            time.sleep(delay)

    if last_error:
        raise last_error
    raise requests.RequestException("Não foi possível consultar a API do ClickUp.")
