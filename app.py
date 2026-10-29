# -*- coding: utf-8 -*-
"""Backend Flask do Dashboard ClickUp com filtro por data, retries e cache de contingência."""
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
CONNECT_TIMEOUT = 8
READ_TIMEOUT = 25
MAX_RETRIES = 3
CACHE_TTL_SECONDS = 300
STATUS_INATIVOS = {
    "cancelado", "cancelada", "cancelados", "canceladas", "cancelled", "canceled",
    "finalizado", "finalizada", "finalizados", "finalizadas", "closed", "done",
    "concluido", "concluida", "concluídos", "concluídas", "complete", "completed",
}
_cache_lock = threading.Lock()
_cache = {"at": 0.0, "tasks": None, "updated_at": None}


def _safe_json(response):
    response.raise_for_status()
    try:
        payload = response.json()
    except (requests.exceptions.JSONDecodeError, ValueError) as exc:
        raise ValueError("O ClickUp retornou uma resposta que não é JSON válido.") from exc
    if not isinstance(payload, dict):
        raise ValueError("Resposta inesperada da API do ClickUp.")
    return payload


def _get_json(session, url, params=None):
    """Repete apenas falhas transitórias: timeout, conexão, HTTP 429 e HTTP 5xx."""
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(url, params=params, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            transient = response.status_code == 429 or 500 <= response.status_code <= 599
            if transient and attempt < MAX_RETRIES - 1:
                retry_after = response.headers.get("Retry-After")
                try:
                    delay = min(8.0, max(1.0, float(retry_after))) if retry_after else 2 ** attempt
                except (TypeError, ValueError):
                    delay = 2 ** attempt
                time.sleep(delay)
                continue
            return _safe_json(response)
        except (requests.Timeout, requests.ConnectionError):
            if attempt >= MAX_RETRIES - 1:
                raise
            time.sleep(2 ** attempt)
    raise requests.RequestException("Não foi possível consultar a API do ClickUp.")


def _space_name(task, spaces_by_id):
    space = task.get("space") or {}
    if isinstance(space, dict):
        name, space_id = space.get("name"), space.get("id")
        if name:
            return str(name)
    else:
        space_id = None
    space_id = space_id or task.get("space_id")
    if space_id is not None and str(space_id) in spaces_by_id:
        return spaces_by_id[str(space_id)]
    list_obj = task.get("list") or {}
    if isinstance(list_obj, dict) and list_obj.get("space"):
        nested = list_obj["space"]
        if isinstance(nested, dict) and nested.get("name"):
            return str(nested["name"])
        if isinstance(nested, str) and nested in spaces_by_id:
            return spaces_by_id[nested]
    if isinstance(list_obj, dict) and list_obj.get("name"):
        return f"Lista: {list_obj['name']}"
    return "Espaço não identificado"


def _is_inactive_status(status):
    normalized = (status or "").lower().strip()
    return normalized in STATUS_INATIVOS or any(
        term in normalized for term in (
            "cancelad", "cancelled", "canceled", "finalizad", "conclu",
            "complet", "closed", "done",
        )
    )


def buscar_tarefas_clickup():
    """Busca demandas abertas com mais de 100 dias; token permanece no servidor."""
    token = os.getenv("CLICKUP_API_TOKEN", "").strip()
    if not token:
        raise RuntimeError("O serviço ainda não foi configurado com CLICKUP_API_TOKEN.")
    session = requests.Session()
    session.headers.update({"Authorization": token, "Accept": "application/json"})

    teams = _get_json(session, f"{API_BASE}/team").get("teams", [])
    if not teams:
        raise RuntimeError("Nenhum workspace do ClickUp foi encontrado para este token.")
    team_id = os.getenv("CLICKUP_TEAM_ID", "").strip()
    team = next((t for t in teams if str(t.get("id")) == team_id), None) if team_id else teams[0]
    if not team:
        raise RuntimeError("CLICKUP_TEAM_ID não corresponde a um workspace disponível para o token.")

    spaces_by_id = {}
    try:
        sp = _get_json(session, f"{API_BASE}/team/{team['id']}/space", {"archived": "false"})
        spaces_by_id = {
            str(s["id"]): str(s["name"]) for s in sp.get("spaces", [])
            if s.get("id") is not None and s.get("name")
        }
    except (requests.RequestException, ValueError) as exc:
        app.logger.warning("Não foi possível carregar a lista de Spaces: %s", type(exc).__name__)

    now = datetime.now(timezone.utc)
    cutoff_ms = int((now - timedelta(days=DIAS_MINIMOS)).timestamp() * 1000)
    critical = []
    page = 0
    while True:
        payload = _get_json(
            session, f"{API_BASE}/team/{team['id']}/task",
            {
                "page": page,
                "subtasks": "true",
                "date_created_lt": cutoff_ms,
                "include_closed": "false",
            },
        )
        tasks = payload.get("tasks", [])
        if not tasks:
            break
        for task in tasks:
            status_obj = task.get("status") or {}
            status = status_obj.get("status", "") if isinstance(status_obj, dict) else str(status_obj)
            if _is_inactive_status(status):
                continue
            created_ms = task.get("date_created")
            if not created_ms:
                continue
            try:
                created = datetime.fromtimestamp(int(created_ms) / 1000, tz=timezone.utc)
            except (TypeError, ValueError, OSError, OverflowError):
                continue
            days = max(0, (now - created).days)
            if days <= DIAS_MINIMOS:
                continue
            assignees = task.get("assignees") or []
            responsible = ", ".join(
                str(a.get("username") or a.get("email") or a.get("initials") or "")
                for a in assignees if isinstance(a, dict)
            ).strip(", ") or "Não atribuído"
            critical.append({
                "id": str(task.get("custom_id") or task.get("id") or ""),
                "name": str(task.get("name") or "Sem título"),
                "status": status or "Sem status",
                "dias": days,
                "criacao": created.strftime("%Y-%m-%d"),
                "responsavel": responsible,
                "espaco": _space_name(task, spaces_by_id),
                "url": str(task.get("url") or "#"),
            })
        page += 1
        time.sleep(0.1)
    return critical


def obter_tarefas_cache(force_refresh=False):
    """Usa cache recente e, se a API falhar, retorna a última consulta bem-sucedida."""
    now = time.time()
    with _cache_lock:
        has_cache = _cache["tasks"] is not None
        fresh = has_cache and now - _cache["at"] < CACHE_TTL_SECONDS
        if not force_refresh and fresh:
            return _cache["tasks"], _cache["updated_at"], True
        try:
            tasks = buscar_tarefas_clickup()
            updated_at = datetime.now(timezone.utc).isoformat()
            _cache.update(at=time.time(), tasks=tasks, updated_at=updated_at)
            return tasks, updated_at, False
        except Exception:
            if has_cache:
                app.logger.warning(
                    "Atualização do ClickUp falhou; servindo cache anterior (%s).",
                    _cache["updated_at"],
                )
                return _cache["tasks"], _cache["updated_at"], True
            raise


@app.after_request
def security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Cache-Control"] = "no-store"
    return response


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/api/tasks")
def api_tasks():
    try:
        tasks, updated_at, cached = obter_tarefas_cache(request.args.get("refresh") == "1")
        return jsonify({"tasks": tasks, "updated_at": updated_at, "cached": cached})
    except requests.HTTPError as exc:
        code = exc.response.status_code if exc.response is not None else "desconhecido"
        app.logger.warning("ClickUp API retornou HTTP %s", code)
        return jsonify({"error": "Não foi possível consultar o ClickUp. Verifique o token e as permissões configuradas no servidor."}), 502
    except (requests.RequestException, ValueError) as exc:
        app.logger.warning("Falha de comunicação com ClickUp: %s", type(exc).__name__)
        return jsonify({"error": "Falha de comunicação com o ClickUp. Tente novamente em alguns instantes."}), 502
    except RuntimeError as exc:
        app.logger.warning("Configuração ClickUp: %s", exc)
        return jsonify({"error": str(exc)}), 503
    except Exception:
        app.logger.exception("Erro inesperado ao carregar tarefas do ClickUp.")
        return jsonify({"error": "Erro interno ao carregar as tarefas. Consulte os logs do servidor."}), 500


@app.post("/api/export.xlsx")
def export_xlsx():
    """Exporta exatamente as tarefas enviadas pela tabela filtrada do dashboard."""
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Alignment, Font, PatternFill
        from openpyxl.utils import get_column_letter
    except ImportError:
        return jsonify({"error": "Exportação XLSX indisponível: dependência openpyxl não instalada."}), 503

    payload = request.get_json(silent=True) or {}
    rows = payload.get("tasks")
    if not isinstance(rows, list):
        return jsonify({"error": "Não foi possível identificar as tarefas para exportação."}), 400
    if len(rows) > 20000:
        return jsonify({"error": "A seleção excede o limite de 20.000 tarefas por exportação."}), 413

    headers = ["ID", "Nome da tarefa", "Status", "Dias em aberto", "Responsável", "Espaço do ClickUp", "Data de criação", "Link ClickUp"]
    fields = ["id", "name", "status", "dias", "responsavel", "espaco", "criacao", "url"]
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Detalhamento das Tarefas"
    sheet.append(headers)
    for cell in sheet[1]:
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill("solid", fgColor="334155")
        cell.alignment = Alignment(vertical="center", wrap_text=True)

    def safe_cell(value):
        if value is None:
            return ""
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return value
        value = str(value)
        return "'" + value if value.startswith(("=", "+", "-", "@")) else value

    for item in rows:
        if not isinstance(item, dict):
            continue
        values = [safe_cell(item.get(field, "")) for field in fields]
        url = str(item.get("url") or "")
        if not (url.startswith("https://app.clickup.com/") or url.startswith("https://sharing.clickup.com/")):
            values[-1] = ""
        sheet.append(values)
        link = sheet.cell(row=sheet.max_row, column=8)
        if values[-1]:
            link.hyperlink = values[-1]
            link.style = "Hyperlink"
        days_cell = sheet.cell(row=sheet.max_row, column=4)
        if isinstance(days_cell.value, (int, float)):
            days_cell.number_format = '0 "dias"'

    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = sheet.dimensions
    sheet.row_dimensions[1].height = 30
    for index, width in enumerate([18, 55, 22, 18, 32, 32, 18, 48], start=1):
        sheet.column_dimensions[get_column_letter(index)].width = width
    for row in sheet.iter_rows(min_row=2):
        for cell in row:
            cell.alignment = Alignment(vertical="top", wrap_text=(cell.column == 2))

    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    filename = f"detalhamento_tarefas_clickup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return send_file(
        output, as_attachment=True, download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False)
