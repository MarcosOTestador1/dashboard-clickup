# -*- coding: utf-8 -*-
"""Dashboard ClickUp: Flask backend for deployment."""
import os
import threading
from io import BytesIO
import time
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, render_template, request, send_file

app = Flask(__name__, template_folder="templates")
API_BASE = "https://api.clickup.com/api/v2"
STATUS_INATIVOS = {
    # Demandas encerradas não fazem parte do acompanhamento de demandas em aberto.
    "cancelado", "cancelada", "cancelados", "canceladas", "cancelled", "canceled",
    "finalizado", "finalizada", "finalizados", "finalizadas", "closed",
    "done", "concluido", "concluida", "concluídos", "concluídas", "complete", "completed",
}
REQUEST_TIMEOUT = 25
CACHE_TTL_SECONDS = 300
_cache_lock = threading.Lock()
_cache = {"at": 0.0, "tasks": None, "updated_at": None}


def _safe_json(response):
    response.raise_for_status()
    try:
        payload = response.json()
    except requests.exceptions.JSONDecodeError as exc:
        # Avoid logging response body because upstream responses may contain sensitive data.
        raise ValueError("O ClickUp retornou uma resposta que não é JSON válido.") from exc
    if not isinstance(payload, dict):
        raise ValueError("Resposta inesperada da API do ClickUp.")
    return payload


def _space_name(task, spaces_by_id):
    """Resolve the ClickUp Space name from the task payload, with safe fallbacks."""
    space = task.get("space") or {}
    if isinstance(space, dict):
        name = space.get("name")
        if name:
            return str(name)
        space_id = space.get("id")
    else:
        space_id = None

    space_id = space_id or task.get("space_id")
    if space_id is not None and str(space_id) in spaces_by_id:
        return spaces_by_id[str(space_id)]

    # Some task responses omit the Space object but include List/Folder context.
    list_obj = task.get("list") or {}
    if isinstance(list_obj, dict) and list_obj.get("space"):
        nested_space = list_obj["space"]
        if isinstance(nested_space, dict) and nested_space.get("name"):
            return str(nested_space["name"])
        if isinstance(nested_space, str) and nested_space in spaces_by_id:
            return spaces_by_id[nested_space]

    if isinstance(list_obj, dict) and list_obj.get("name"):
        return f"Lista: {list_obj['name']}"
    return "Espaço não identificado"


def buscar_tarefas_clickup():
    """Fetch active critical tasks; token remains server-side."""
    token = os.getenv("CLICKUP_API_TOKEN", "").strip()
    if not token:
        raise RuntimeError("O serviço ainda não foi configurado com CLICKUP_API_TOKEN.")

    session = requests.Session()
    session.headers.update({"Authorization": token, "Accept": "application/json"})

    teams = _safe_json(session.get(f"{API_BASE}/team", timeout=REQUEST_TIMEOUT)).get("teams", [])
    if not teams:
        raise RuntimeError("Nenhum workspace do ClickUp foi encontrado para este token.")

    team_id_configurado = os.getenv("CLICKUP_TEAM_ID", "").strip()
    team = next((t for t in teams if str(t.get("id")) == team_id_configurado), None) if team_id_configurado else teams[0]
    if not team:
        raise RuntimeError("CLICKUP_TEAM_ID não corresponde a um workspace disponível para o token.")

    spaces_by_id = {}
    try:
        spaces_payload = _safe_json(session.get(
            f"{API_BASE}/team/{team['id']}/space",
            params={"archived": "false"},
            timeout=REQUEST_TIMEOUT,
        ))
        spaces_by_id = {
            str(space.get("id")): str(space.get("name"))
            for space in spaces_payload.get("spaces", [])
            if space.get("id") is not None and space.get("name")
        }
    except (requests.RequestException, ValueError) as exc:
        app.logger.warning("Não foi possível carregar a lista de Spaces: %s", type(exc).__name__)

    agora = datetime.now(timezone.utc)
    tarefas_criticas = []
    page = 0

    while True:
        payload = _safe_json(session.get(
            f"{API_BASE}/team/{team['id']}/task",
            params={"page": page, "subtasks": "true"},
            timeout=REQUEST_TIMEOUT,
        ))
        tasks = payload.get("tasks", [])
        if not tasks:
            break

        for task in tasks:
            status_obj = task.get("status") or {}
            status = status_obj.get("status", "") if isinstance(status_obj, dict) else str(status_obj)
            status_normalizado = status.lower().strip()
            # Também trata variações de gênero/número e status que contenham o termo.
            if status_normalizado in STATUS_INATIVOS or any(
                termo in status_normalizado for termo in ("cancelad", "cancelled", "canceled", "finalizad", "conclu", "complet", "closed", "done")
            ):
                continue

            date_created_ms = task.get("date_created")
            if not date_created_ms:
                continue
            try:
                dt_criacao = datetime.fromtimestamp(int(date_created_ms) / 1000, tz=timezone.utc)
            except (TypeError, ValueError, OSError, OverflowError):
                continue

            dias = max(0, (agora - dt_criacao).days)
            # This dashboard is specifically for tasks open for more than 100 days.
            if dias <= 100:
                continue

            assignees = task.get("assignees") or []
            responsaveis = ", ".join(
                str(a.get("username") or a.get("email") or a.get("initials") or "")
                for a in assignees if isinstance(a, dict)
            ).strip(", ") or "Não atribuído"

            tarefas_criticas.append({
                "id": str(task.get("custom_id") or task.get("id") or ""),
                "name": str(task.get("name") or "Sem título"),
                "status": status or "Sem status",
                "dias": dias,
                "criacao": dt_criacao.strftime("%Y-%m-%d"),
                "responsavel": responsaveis,
                "espaco": _space_name(task, spaces_by_id),
                "url": str(task.get("url") or "#"),
            })

        page += 1
        # Rate-limit gently between pages. Cache below avoids repeating this on every visit.
        time.sleep(0.1)

    return tarefas_criticas


def obter_tarefas_cache(force_refresh=False):
    """Keep a short server-side cache so every visitor does not trigger a full API scan."""
    now = time.time()
    with _cache_lock:
        if not force_refresh and _cache["tasks"] is not None and now - _cache["at"] < CACHE_TTL_SECONDS:
            return _cache["tasks"], _cache["updated_at"], True

        tasks = buscar_tarefas_clickup()
        updated_at = datetime.now(timezone.utc).isoformat()
        _cache.update({"at": time.time(), "tasks": tasks, "updated_at": updated_at})
        return tasks, updated_at, False


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
        tasks, updated_at, cached = obter_tarefas_cache(force_refresh=request.args.get("refresh") == "1")
        return jsonify({"tasks": tasks, "updated_at": updated_at, "cached": cached})
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else "desconhecido"
        app.logger.warning("ClickUp API retornou HTTP %s", status_code)
        return jsonify({"error": "Não foi possível consultar o ClickUp. Verifique o token e as permissões configuradas no servidor."}), 502
    except (requests.RequestException, ValueError) as exc:
        app.logger.warning("Falha de comunicação com ClickUp: %s", type(exc).__name__)
        return jsonify({"error": "Falha de comunicação com o ClickUp. Tente novamente em alguns instantes."}), 502
    except RuntimeError as exc:
        app.logger.warning("Configuração ClickUp: %s", exc)
        return jsonify({"error": str(exc)}), 503


@app.post("/api/export.xlsx")
def export_xlsx():
    """Create an XLSX containing exactly the rows currently filtered in the dashboard."""
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
        text_value = str(value)
        # Prevent spreadsheet formula injection from user-controlled task text.
        if text_value.startswith(("=", "+", "-", "@")):
            text_value = "'" + text_value
        return text_value

    for item in rows:
        if not isinstance(item, dict):
            continue
        values = [safe_cell(item.get(field, "")) for field in fields]
        # Keep the ClickUp URL as a hyperlink only when it is a trusted HTTPS URL.
        url = str(item.get("url") or "")
        if not (url.startswith("https://app.clickup.com/") or url.startswith("https://sharing.clickup.com/")):
            values[-1] = ""
        sheet.append(values)
        link_cell = sheet.cell(row=sheet.max_row, column=8)
        if values[-1]:
            link_cell.hyperlink = values[-1]
            link_cell.style = "Hyperlink"
        days_cell = sheet.cell(row=sheet.max_row, column=4)
        if isinstance(days_cell.value, (int, float)):
            days_cell.number_format = '0 "dias"'

    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = sheet.dimensions
    sheet.row_dimensions[1].height = 30
    widths = [18, 55, 22, 18, 32, 32, 18, 48]
    for index, width in enumerate(widths, start=1):
        sheet.column_dimensions[get_column_letter(index)].width = width
    for row in sheet.iter_rows(min_row=2):
        for cell in row:
            cell.alignment = Alignment(vertical="top", wrap_text=(cell.column == 2))

    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    filename = f"detalhamento_tarefas_clickup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False)
