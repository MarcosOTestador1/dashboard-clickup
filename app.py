# -*- coding: utf-8 -*-
"""Backend Flask do Dashboard ClickUp: filtro por data, retries e cache de contingência."""
import logging
import os
import re
import hmac
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from io import BytesIO

import requests

try:
    import psycopg2
    from psycopg2.extras import Json
except ImportError:  # Permite iniciar sem persistência se a dependência não estiver instalada.
    psycopg2 = None
    Json = None
from flask import Flask, jsonify, render_template, request, send_file

app = Flask(__name__, template_folder="templates")

API_BASE = "https://api.clickup.com/api/v2"
DIAS_MINIMOS = 100
CONNECT_TIMEOUT = 4
READ_TIMEOUT = 12
MAX_RETRIES = 3
PAGE_RETRIES = 6
CACHE_TTL_SECONDS = 300
CLIENTE_FIELDS_CACHE_TTL_SECONDS = 1800
CLIENTE_FIELDS_MAX_WORKERS = 4
_cliente_fields_cache = {}  # list_id -> (cached_at, options_by_field_id)
_cliente_fields_cache_lock = threading.Lock()

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
_db_cache_loaded = False
_db_cache_lock = threading.Lock()
_progress_lock = threading.Lock()
_progress = {"tasks": [], "page": 0, "received": 0, "active": False}


def _database_url():
    """Retorna a URL do PostgreSQL sem imprimir credenciais nos logs."""
    return (os.getenv("DATABASE_URL") or "").strip()


def _db_connect():
    """Abre conexão com o Supabase/PostgreSQL, se configurado."""
    database_url = _database_url()
    if not database_url:
        return None
    if psycopg2 is None:
        raise RuntimeError(
            "DATABASE_URL está configurada, mas psycopg2 não está instalado. "
            "Adicione psycopg2-binary ao requirements.txt."
        )
    return psycopg2.connect(database_url, connect_timeout=8, sslmode="require")


def carregar_cache_supabase():
    global _db_cache_loaded
    if _db_cache_loaded:
        return
    with _db_cache_lock:
        if _db_cache_loaded:
            return
        try:
            conn = _db_connect()
            if conn is None:
                app.logger.info("DATABASE_URL não configurada; cache seguirá apenas em memória.")
                _db_cache_loaded = True
                return
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT tasks, updated_at, complete "
                        "FROM public.clickup_dashboard_cache WHERE id = 1"
                    )
                    row = cur.fetchone()
                if row and row[2] and row[0] is not None:
                    tasks, updated_at, complete = row
                    if isinstance(tasks, str):
                        import json
                        tasks = json.loads(tasks)
                    if isinstance(tasks, list):
                        updated_iso = updated_at.isoformat() if updated_at else None
                        with _cache_lock:
                            _cache.update(
                                at=time.time(),
                                tasks=tasks,
                                updated_at=updated_iso,
                            )
                        app.logger.info(
                            "Cache persistido carregado do Supabase: %s tarefas.",
                            len(tasks),
                        )
            finally:
                conn.close()
        except Exception:
            app.logger.exception("Não foi possível carregar o cache do Supabase.")
            _db_cache_loaded = False
            return
        _db_cache_loaded = True


def salvar_cache_supabase(tasks, updated_at):
    conn = _db_connect()
    if conn is None:
        return False
    try:
        parsed_updated_at = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO public.clickup_dashboard_cache (id, tasks, updated_at, complete) "
                    "VALUES (1, %s, %s, TRUE) "
                    "ON CONFLICT (id) DO UPDATE SET "
                    "tasks = EXCLUDED.tasks, "
                    "updated_at = EXCLUDED.updated_at, "
                    "complete = TRUE",
                    (Json(tasks), parsed_updated_at),
                )
        app.logger.info("Cache persistido no Supabase: %s tarefas.", len(tasks))
        return True
    finally:
        conn.close()


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
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(url, params=params, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            transient = response.status_code == 429 or 500 <= response.status_code <= 599
            if transient and attempt < MAX_RETRIES - 1:
                retry_after = response.headers.get("Retry-After")
                try:
                    delay = min(30.0, max(1.0, float(retry_after))) if retry_after else min(8.0, 2 ** attempt)
                except (TypeError, ValueError):
                    delay = min(8.0, 2 ** attempt)
                time.sleep(delay)
                continue
            return _safe_json(response)
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = exc
            if attempt >= MAX_RETRIES - 1:
                raise
            time.sleep(min(8.0, 2 ** attempt))
    if last_error:
        raise last_error
    raise requests.RequestException("Não foi possível consultar a API do ClickUp.")


def _space_name(task, spaces_by_id):
    space = task.get("space") or {}
    if isinstance(space, dict):
        name = space.get("name")
        space_id = space.get("id")
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


def _cliente_value(task, cliente_options_by_field_id=None):
    cliente_options_by_field_id = cliente_options_by_field_id or {}
    direct = task.get("Cliente")
    if direct not in (None, ""):
        return str(direct).strip()

    fields = task.get("custom_fields") or []
    if not isinstance(fields, list):
        return ""

    for field in fields:
        if not isinstance(field, dict):
            continue
        if str(field.get("name") or "").strip().casefold() != "cliente":
            continue

        value = field.get("value")
        if value is None or value == "":
            return ""

        field_id = str(field.get("id") or field.get("field_id") or "")
        options = (field.get("type_config") or {}).get("options") or []
        if not options:
            options = cliente_options_by_field_id.get(field_id, [])

        if isinstance(options, list):
            try:
                numeric_value = int(value)
            except (TypeError, ValueError):
                numeric_value = None

            for option in options:
                if not isinstance(option, dict):
                    continue
                option_name = option.get("name") or option.get("label")
                if not option_name:
                    continue
                if str(option.get("id", "")) == str(value):
                    return str(option_name).strip()

            if numeric_value is not None:
                for option in options:
                    if not isinstance(option, dict):
                        continue
                    orderindex = option.get("orderindex")
                    try:
                        if orderindex is not None and int(orderindex) == numeric_value:
                            option_name = option.get("name") or option.get("label")
                            if option_name:
                                return str(option_name).strip()
                    except (TypeError, ValueError):
                        pass

                if 0 <= numeric_value < len(options):
                    option = options[numeric_value]
                    if isinstance(option, dict):
                        option_name = option.get("name") or option.get("label")
                        if option_name:
                            return str(option_name).strip()

        elif isinstance(options, dict):
            resolved = options.get(str(value))
            if resolved not in (None, ""):
                return str(resolved).strip()

        field_type = str(field.get("type") or "").casefold()
        if field_type in ("drop_down", "dropdown") and str(value).strip().isdigit():
            return ""
        return str(value).strip()

    return ""


def _list_id_from_task(task):
    list_obj = task.get("list") or {}
    value = list_obj.get("id") if isinstance(list_obj, dict) else list_obj
    return str(value) if value not in (None, "") else ""


def _buscar_opcoes_cliente_lista(list_id, token):
    session = requests.Session()
    session.headers.update({"Authorization": token, "Accept": "application/json"})
    try:
        payload = _get_json(session, f"{API_BASE}/list/{list_id}/field")
        result = {}
        for field in payload.get("fields", []):
            if not isinstance(field, dict):
                continue
            if str(field.get("name") or "").strip().casefold() != "cliente":
                continue
            field_id = str(field.get("id") or "")
            options = (field.get("type_config") or {}).get("options") or []
            if field_id and isinstance(options, list):
                result[field_id] = options
        return list_id, result, None
    except (requests.RequestException, ValueError) as exc:
        return list_id, {}, exc
    finally:
        session.close()


def _carregar_opcoes_cliente_por_lista(session, list_ids):
    token = (os.getenv("CLICKUP_API_TOKEN") or "").strip()
    now = time.time()
    options_by_field_id = {}
    lists_to_fetch = []

    with _cliente_fields_cache_lock:
        for list_id in sorted(set(list_ids)):
            cached = _cliente_fields_cache.get(str(list_id))
            if cached and now - cached[0] < CLIENTE_FIELDS_CACHE_TTL_SECONDS:
                options_by_field_id.update(cached[1])
            else:
                lists_to_fetch.append(str(list_id))

    lists_ok = len(set(list_ids)) - len(lists_to_fetch)
    lists_failed = 0
    if lists_to_fetch:
        with ThreadPoolExecutor(max_workers=CLIENTE_FIELDS_MAX_WORKERS) as executor:
            futures = [executor.submit(_buscar_opcoes_cliente_lista, list_id, token)
                       for list_id in lists_to_fetch]
            for future in as_completed(futures):
                list_id, result, error = future.result()
                if error:
                    lists_failed += 1
                    continue
                lists_ok += 1
                options_by_field_id.update(result)
                with _cliente_fields_cache_lock:
                    _cliente_fields_cache[list_id] = (time.time(), result)

    return options_by_field_id


def _is_inactive_status(status):
    normalized = (status or "").lower().strip()
    return normalized in STATUS_INATIVOS or any(
        term in normalized
        for term in (
            "cancelad", "cancelled", "canceled", "finalizad",
            "conclu", "complet", "closed", "done",
        )
    )


def buscar_tarefas_clickup():
    token = (os.getenv("CLICKUP_API_TOKEN") or "").strip()
    if not token:
        raise RuntimeError("O serviço ainda não foi configurado com CLICKUP_API_TOKEN no Render.")

    session = requests.Session()
    session.headers.update({"Authorization": token, "Accept": "application/json"})
    try:
        teams = _get_json(session, f"{API_BASE}/team").get("teams", [])
        if not teams:
            raise RuntimeError("Nenhum workspace do ClickUp foi encontrado para este token.")
        team_id = (os.getenv("CLICKUP_TEAM_ID") or "").strip()
        team = next((t for t in teams if str(t.get("id")) == team_id), None) if team_id else teams[0]
        if not team:
            raise RuntimeError("CLICKUP_TEAM_ID não corresponde a um workspace disponível para o token.")

        spaces_by_id = {}
        try:
            spaces_payload = _get_json(session, f"{API_BASE}/team/{team['id']}/space", {"archived": "false"})
            spaces_by_id = {
                str(space["id"]): str(space["name"])
                for space in spaces_payload.get("spaces", [])
                if space.get("id") is not None and space.get("name")
            }
        except (requests.RequestException, ValueError):
            pass

        collection_started = time.perf_counter()
        pagination_started = collection_started
        now = datetime.now(timezone.utc)
        critical = []
        collected_list_ids = set()
        seen_ids = set()
        seen_page_signatures = set()
        page = 0
        total_received = 0

        while True:
            page_error = None
            for page_attempt in range(PAGE_RETRIES):
                try:
                    payload = _get_json(
                        session,
                        f"{API_BASE}/team/{team['id']}/task",
                        {
                            "page": page,
                            "limit": 100,
                            "subtasks": "true",
                            "include_closed": "false",
                        },
                    )
                    page_error = None
                    break
                except (requests.RequestException, ValueError) as exc:
                    page_error = exc
                    if page_attempt == PAGE_RETRIES - 1:
                        raise
                    time.sleep(min(30, 2 ** page_attempt))
            if page_error is not None:
                raise page_error

            tasks = payload.get("tasks", [])
            if not tasks:
                break

            page_ids = [str(t.get("id") or t.get("custom_id") or "") for t in tasks]
            signature = tuple(page_ids)
            if signature in seen_page_signatures:
                raise RuntimeError("A API do ClickUp repetiu a página; atualização cancelada.")
            seen_page_signatures.add(signature)
            total_received += len(tasks)

            for task in tasks:
                list_id = _list_id_from_task(task)
                if list_id:
                    collected_list_ids.add(list_id)
                task_id = str(task.get("id") or task.get("custom_id") or "")
                if task_id and task_id in seen_ids:
                    continue
                if task_id:
                    seen_ids.add(task_id)

                status_obj = task.get("status") or {}
                status = status_obj.get("status", "") if isinstance(status_obj, dict) else str(status_obj)
                if _is_inactive_status(status):
                    continue
                created_ms = task.get("date_created")
                if created_ms in (None, ""):
                    continue
                try:
                    created = datetime.fromtimestamp(int(created_ms) / 1000, tz=timezone.utc)
                except (TypeError, ValueError, OSError, OverflowError):
                    continue
                days = max(0, (now - created).days)
                responsible_names = []
                responsible_people = []
                seen_assignees = set()
                for assignee in (task.get("assignees") or []):
                    if not isinstance(assignee, dict):
                        continue
                    assignee_id = str(assignee.get("id") or "").strip()
                    name = (
                        assignee.get("username")
                        or assignee.get("email")
                        or assignee.get("initials")
                        or assignee_id
                    )
                    if not name:
                        continue
                    name = str(name).strip()
                    identity = assignee_id or name.casefold()
                    if identity in seen_assignees:
                        continue
                    seen_assignees.add(identity)
                    responsible_names.append(name)
                    responsible_people.append({"id": assignee_id, "name": name})
                critical.append({
                    "id": str(task.get("custom_id") or task.get("id") or ""),
                    "name": str(task.get("name") or "Sem título"),
                    "status": status or "Sem status",
                    "dias": days,
                    "criacao": created.strftime("%Y-%m-%d"),
                    "responsavel": ", ".join(responsible_names) or "Não atribuído",
                    "responsaveis": responsible_people or [{"id": "", "name": "Não atribuído"}],
                    "espaco": _space_name(task, spaces_by_id),
                    "_cliente_task": task,
                    "url": str(task.get("url") or "#"),
                })
            page += 1
            with _progress_lock:
                _progress.update(tasks=list(critical), page=page, received=total_received, active=True)

        cliente_options_by_field_id = _carregar_opcoes_cliente_por_lista(session, collected_list_ids)
        for item in critical:
            task_payload = item.pop("_cliente_task", {})
            item["cliente"] = _cliente_value(task_payload, cliente_options_by_field_id)

        return critical
    finally:
        session.close()


def _refresh_worker():
    global _refresh_thread, _refreshing, _last_refresh_error
    with _progress_lock:
        _progress.update(tasks=[], page=0, received=0, active=True)
    try:
        tasks = buscar_tarefas_clickup()
        updated_at = datetime.now(timezone.utc).isoformat()
        try:
            salvar_cache_supabase(tasks, updated_at)
        except Exception:
            pass
        with _cache_lock:
            _cache.update(at=time.time(), tasks=tasks, updated_at=updated_at)
        _last_refresh_error = None
    except Exception as exc:
        _last_refresh_error = type(exc).__name__
    finally:
        with _progress_lock:
            _progress["active"] = False
        with _refresh_lock:
            _refreshing = False
            _refresh_thread = None


def iniciar_atualizacao(force=False):
    global _refresh_thread, _refreshing, _last_refresh_error
    with _refresh_lock:
        now = time.time()
        with _cache_lock:
            has_cache = _cache["tasks"] is not None
            fresh = has_cache and (now - _cache["at"] < CACHE_TTL_SECONDS)
        if _refreshing:
            return False
        if not force and fresh:
            return False
        _refreshing = True
        _last_refresh_error = None
        _refresh_thread = threading.Thread(target=_refresh_worker, name="clickup-refresh", daemon=True)
        _refresh_thread.start()
        return True


_refresh_lock = threading.Lock()
_refreshing = False
_refresh_thread = None
_last_refresh_error = None


def estado_tarefas(force_refresh=False):
    carregar_cache_supabase()
    iniciar_atualizacao(force=force_refresh)
    with _cache_lock:
        tasks = list(_cache["tasks"]) if _cache["tasks"] is not None else None
        updated_at = _cache["updated_at"]
        fresh = _cache["tasks"] is not None and (time.time() - _cache["at"] < CACHE_TTL_SECONDS)
    return tasks, updated_at, fresh


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
    force_refresh = request.args.get("refresh") == "1"
    tasks, updated_at, fresh = estado_tarefas(force_refresh)
    with _refresh_lock:
        refreshing = _refreshing
    payload = {
        "tasks": tasks or [],
        "updated_at": updated_at,
        "cached": tasks is not None,
        "complete": tasks is not None,
        "refreshing": refreshing,
        "last_refresh_error": _last_refresh_error,
    }
    with _progress_lock:
        payload["progress"] = {
            "page": _progress["page"],
            "received": _progress["received"],
            "critical_accumulated": len(_progress["tasks"]),
        }
    if _last_refresh_error and tasks is None and not refreshing:
        payload["error"] = "Não foi possível concluir a busca completa no ClickUp."
        return jsonify(payload), 502
    return jsonify(payload), 200


@app.post("/api/refresh")
def scheduled_refresh():
    expected = (os.getenv("CRON_SECRET") or "").strip()
    supplied = (request.headers.get("X-Cron-Secret") or "").strip()
    if not expected:
        return jsonify({"ok": False, "error": "Agendamento não configurado no servidor."}), 503
    if not supplied or not hmac.compare_digest(supplied, expected):
        return jsonify({"ok": False, "error": "Não autorizado."}), 401

    carregar_cache_supabase()
    started = iniciar_atualizacao(force=True)
    with _refresh_lock:
        refreshing = _refreshing
    if started:
        return jsonify({"ok": True, "started": True, "message": "Atualização iniciada em segundo plano."}), 202
    if refreshing:
        return jsonify({"ok": True, "started": False, "message": "Atualização já em andamento."}), 200
    return jsonify({"ok": False, "error": "Não foi possível iniciar a atualização."}), 500


@app.post("/api/export.xlsx")
def export_xlsx():
    """Exporta as tarefas enviadas pela tabela filtrada do dashboard."""
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Alignment, Font, PatternFill
        from openpyxl.utils import get_column_letter
    except ImportError:
        return jsonify({
            "error": "Exportação XLSX indisponível: dependência openpyxl não instalada."
        }), 503

    payload = request.get_json(silent=True) or {}
    rows = payload.get("tasks")
    if not isinstance(rows, list):
        return jsonify({
            "error": "Não foi possível identificar as tarefas para exportação."
        }), 400
    if len(rows) > 20000:
        return jsonify({
            "error": "A seleção excede o limite de 20.000 tarefas por exportação."
        }), 413

    headers = [
        "ID", "Nome da tarefa", "Status", "Dias em aberto", "Responsável",
        "Espaço do ClickUp", "Cliente", "Data de criação", "Link ClickUp",
    ]
    fields = ["id", "name", "status", "dias", "responsavel", "espaco", "cliente", "criacao", "url"]

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
        if not (
            url.startswith("https://app.clickup.com/")
            or url.startswith("https://sharing.clickup.com/")
        ):
            values[-1] = ""

        sheet.append(values)
        link = sheet.cell(row=sheet.max_row, column=9)
        if values[-1]:
            link.hyperlink = values[-1]
            link.style = "Hyperlink"

        days_cell = sheet.cell(row=sheet.max_row, column=4)
        if isinstance(days_cell.value, (int, float)):
            days_cell.number_format = '0 "dias"'

    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = sheet.dimensions
    sheet.row_dimensions[1].height = 30
    for index, width in enumerate([18, 55, 22, 18, 32, 32, 24, 18, 48], start=1):
        sheet.column_dimensions[get_column_letter(index)].width = width
    for row in sheet.iter_rows(min_row=2):
        for cell in row:
            cell.alignment = Alignment(
                vertical="top",
                wrap_text=(cell.column == 2),
            )

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


@app.post("/api/export.relatorios.xlsx")
def export_relatorios_xlsx():
    """Exporta a aba Relatórios: uma aba por relatório, com resumo no topo e tarefas recolhíveis abaixo."""
    try:
        from openpyxl import Workbook
        from openpyxl.formatting.rule import DataBarRule
        from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
        from openpyxl.utils import get_column_letter
        from openpyxl.worksheet.properties import Outline
        from openpyxl.worksheet.table import Table, TableStyleInfo
    except ImportError:
        return jsonify({"error": "Exportação XLSX indisponível: dependência openpyxl não instalada."}), 503

    payload = request.get_json(silent=True) or {}
    rows = payload.get("tasks")
    if not isinstance(rows, list):
        return jsonify({"error": "Não foi possível identificar as demandas para exportação."}), 400
    available_reports = {
        "resumo", "envelhecimento", "espaco", "clientes_espaco", "cliente",
        "responsavel", "status", "internas", "tipo", "detalhamento"
    }
    requested_reports = payload.get("reports")
    if requested_reports is None:
        selected_reports = set(available_reports)
    elif not isinstance(requested_reports, list) or not requested_reports:
        return jsonify({"error": "Selecione pelo menos um relatório para exportação."}), 400
    else:
        selected_reports = set(requested_reports)
        if not selected_reports.issubset(available_reports):
            return jsonify({"error": "A seleção contém um tipo de relatório inválido."}), 400
    if len(rows) > 20000:
        return jsonify({"error": "A seleção excede o limite de 20.000 demandas por exportação."}), 413

    # ---------- utilitários ----------
    def safe_cell(value):
        if value is None:
            return ""
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return value
        value = str(value)
        return "'" + value if value.startswith(("=", "+", "-", "@")) else value

    def age(item):
        try:
            return max(0, int(float(item.get("dias") or 0)))
        except (TypeError, ValueError):
            return 0

    def label(value, default="Não informado"):
        value = str(value or "").strip()
        return value if value else default

    def is_internal(item):
        cliente = str(item.get("cliente") or "").strip()
        return not cliente or cliente.casefold() == "interno"

    def valid_link(url):
        url = str(url or "")
        return url if url.startswith(("https://app.clickup.com/", "https://sharing.clickup.com/")) else ""

    bands = [
        ("Até 30 dias", 0, 30),
        ("31 a 60 dias", 31, 60),
        ("61 a 100 dias", 61, 100),
        ("101 a 150 dias", 101, 150),
        ("151 a 200 dias", 151, 200),
        ("Mais de 200 dias", 201, float("inf")),
    ]

    def band_of(days):
        for name, low, high in bands:
            if low <= days <= high:
                return name
        return bands[-1][0]

    valid_rows = [item for item in rows if isinstance(item, dict)]
    internal_rows = [item for item in valid_rows if is_internal(item)]
    client_rows = [item for item in valid_rows if not is_internal(item)]
    total = len(valid_rows)
    all_ages = [age(item) for item in valid_rows]
    generated_at = datetime.now().strftime("%d/%m/%Y %H:%M")

    groups = {"space": {}, "client": {}, "owner": {}, "status": {}, "internal_space": {}, "client_space": {}}
    for item in valid_rows:
        groups["space"].setdefault(label(item.get("espaco")), []).append(item)
        groups["client"].setdefault(label(item.get("cliente"), "Demandas internas"), []).append(item)
        groups["owner"].setdefault(label(item.get("responsavel")), []).append(item)
        groups["status"].setdefault(label(item.get("status"), "Sem status"), []).append(item)
        space_name = label(item.get("espaco"), "Espaço não identificado")
        target = "internal_space" if is_internal(item) else "client_space"
        groups[target].setdefault(space_name, []).append(item)

    def ordered(group):
        return sorted(group.items(), key=lambda pair: (-len(pair[1]), pair[0].casefold()))

    # ---------- estilos (reaproveitados para manter a geração rápida) ----------
    FONT = "Calibri"
    f_title = Font(name=FONT, size=16, bold=True, color="0F172A")
    f_sub = Font(name=FONT, size=10, color="64748B")
    f_head = Font(name=FONT, size=10, bold=True, color="FFFFFF")
    f_body = Font(name=FONT, size=10, color="1E293B")
    f_bold = Font(name=FONT, size=10, bold=True, color="0F172A")
    f_section = Font(name=FONT, size=12, bold=True, color="0F172A")
    f_link = Font(name=FONT, size=10, color="0563C1", underline="single")
    f_band = Font(name=FONT, size=11, bold=True, color="0F172A")
    f_band_info = Font(name=FONT, size=10, color="475569")
    fill_head = PatternFill("solid", fgColor="334155")
    fill_band = PatternFill("solid", fgColor="DBEAFE")
    fill_total = PatternFill("solid", fgColor="F1F5F9")
    side = Side(style="thin", color="CBD5E1")
    border_bottom = Border(bottom=side)
    border_top = Border(top=Side(style="thin", color="64748B"))
    a_left = Alignment(horizontal="left", vertical="center")
    a_center = Alignment(horizontal="center", vertical="center")
    a_head = Alignment(horizontal="left", vertical="center", wrap_text=True)
    a_wrap = Alignment(vertical="top", wrap_text=True)
    a_top = Alignment(vertical="top")

    detail_headers = ["ID", "Nome da tarefa", "Status", "Dias em aberto", "Responsável",
                      "Espaço do ClickUp", "Cliente", "Data de criação", "Link ClickUp"]
    # A é larga porque no resumo guarda o nome do grupo; B porque guarda o nome da tarefa / barra de dados.
    column_widths = [40, 55, 22, 16, 32, 32, 26, 16, 20]
    LAST_COL = len(detail_headers)

    workbook = Workbook()
    workbook.remove(workbook.active)
    sheet_titles = {}
    has_summary = "resumo" in selected_reports

    def new_sheet(key, title, subtitle, tab_color="3B82F6"):
        ws = workbook.create_sheet(title=title)
        sheet_titles[key] = title
        ws.sheet_view.showGridLines = False
        ws.sheet_properties.tabColor = tab_color
        ws.sheet_properties.outlinePr = Outline(summaryBelow=False)
        ws.sheet_format.outlineLevelRow = 1  # sem isto o Excel não mostra os botões − / + à esquerda
        ws.page_setup.orientation = "landscape"
        ws.page_setup.fitToWidth = 1
        ws.page_setup.fitToHeight = 0
        ws.sheet_properties.pageSetUpPr.fitToPage = True
        for index, width in enumerate(column_widths, start=1):
            ws.column_dimensions[get_column_letter(index)].width = width
        ws["A1"] = title
        ws["A1"].font = f_title
        ws["A2"] = subtitle
        ws["A2"].font = f_sub
        ws.row_dimensions[1].height = 24
        if has_summary:
            link = ws["A3"]
            link.value = "← Voltar ao Resumo"
            link.hyperlink = "#'Resumo'!A1"
            link.font = f_link
        ws.freeze_panes = "A4"
        return ws

    def style_head(ws, row, values, first_col=1):
        for offset, value in enumerate(values):
            cell = ws.cell(row=row, column=first_col + offset, value=value)
            cell.font, cell.fill, cell.alignment = f_head, fill_head, a_head
        ws.row_dimensions[row].height = 22

    def write_task_row(ws, row, item, link_text="Abrir no ClickUp"):
        values = [item.get("id", ""), item.get("name", ""), item.get("status", ""), age(item),
                  item.get("responsavel", ""), item.get("espaco", ""), item.get("cliente", "") or "Demandas internas",
                  item.get("criacao", "")]
        for col, value in enumerate(values, start=1):
            cell = ws.cell(row=row, column=col, value=safe_cell(value))
            cell.font = f_body
            cell.alignment = a_wrap if col == 2 else a_top
            cell.border = border_bottom
        ws.cell(row=row, column=4).number_format = '0 "dias"'
        ws.cell(row=row, column=4).alignment = Alignment(horizontal="center", vertical="top")
        link_cell = ws.cell(row=row, column=9)
        link_cell.border = border_bottom
        url = valid_link(item.get("url"))
        if url:
            link_cell.value = link_text if link_text else url
            link_cell.hyperlink = url
            link_cell.font = f_link
        link_cell.alignment = a_top

    def build_report(key, title, group_label, grouped, kpis=None, subtitle_extra=""):
        """Resumo no topo (com 'Ver demandas') + blocos de tarefas recolhíveis logo abaixo."""
        ws = new_sheet(key, title, f"{total:,} demandas abertas consideradas · gerado em {generated_at}{subtitle_extra}".replace(",", "."))
        row = 5
        if kpis:
            style_head(ws, row, ["Indicador", "Resultado"])
            for name, value in kpis:
                row += 1
                a = ws.cell(row=row, column=1, value=name)
                b = ws.cell(row=row, column=2, value=value)
                a.font = b.font = f_body
                a.border = b.border = border_bottom
                b.alignment = a_left
            row += 2

        group_list = list(grouped)
        group_total = sum(len(items) for _, items in group_list) or 1
        summary_head = row
        style_head(ws, summary_head, [group_label, "Demandas", "% do total", "Média de dias",
                                      "Mais antiga (dias)", "Consultar"])
        first = summary_head + 1
        show_total = len(group_list) > 1
        last = first + len(group_list) - 1
        section_row = last + (3 if show_total else 2)
        cursor = section_row + 2

        # posição de cada bloco de detalhe
        block_rows = {}
        for name, items in group_list:
            if not items:
                continue
            block_rows[name] = cursor
            cursor += len(items) + 3  # faixa + cabeçalho + tarefas + linha em branco

        # --- tabela-resumo ---
        for offset, (name, items) in enumerate(group_list):
            r = first + offset
            ages = [age(item) for item in items]
            values = [name, len(items), len(items) / group_total,
                      round(sum(ages) / len(ages)) if ages else "—", max(ages) if ages else "—"]
            for col, value in enumerate(values, start=1):
                cell = ws.cell(row=r, column=col, value=safe_cell(value))
                cell.font = f_bold if col == 1 else f_body
                cell.border = border_bottom
                cell.alignment = a_left if col in (1, 2) else a_center
            ws.cell(row=r, column=3).number_format = "0.0%"
            link = ws.cell(row=r, column=6)
            link.border = border_bottom
            link.alignment = a_center
            if name in block_rows:
                link.value = "Ver demandas →"
                link.hyperlink = f"#'{title}'!A{block_rows[name]}"
                link.font = f_link
            else:
                link.value = "—"
                link.font = f_body
        if show_total:
            r = last + 1
            all_items = [age(item) for _, items in group_list for item in items]
            values = ["Total", sum(len(items) for _, items in group_list), 1.0,
                      round(sum(all_items) / len(all_items)) if all_items else "—",
                      max(all_items) if all_items else "—"]
            for col in range(1, 7):
                cell = ws.cell(row=r, column=col, value=values[col - 1] if col <= 5 else None)
                cell.font, cell.fill, cell.border = f_bold, fill_total, border_top
                cell.alignment = a_left if col in (1, 2) else a_center
            ws.cell(row=r, column=3).number_format = "0.0%"
        if group_list:
            ws.conditional_formatting.add(
                f"B{first}:B{last}",
                DataBarRule(start_type="num", start_value=0, end_type="max", color="93C5FD", showValue=True),
            )

        # --- blocos de detalhe ---
        ws.cell(row=section_row, column=1, value="Tarefas de cada grupo").font = f_section
        hint = ws.cell(row=section_row + 1, column=1,
                       value="Clique no sinal de − / + à esquerda (ou nos botões 1 / 2 no canto superior esquerdo) para recolher ou expandir as tarefas.")
        hint.font = f_sub
        summary_row_of = {name: first + offset for offset, (name, _) in enumerate(group_list)}
        for name, items in group_list:
            if name not in block_rows:
                continue
            band_row = block_rows[name]
            ages = [age(item) for item in items]
            for col in range(1, LAST_COL + 1):
                ws.cell(row=band_row, column=col).fill = fill_band
            ws.cell(row=band_row, column=1, value=safe_cell(name)).font = f_band
            info = ws.cell(row=band_row, column=2,
                           value=f"{len(items):,} demandas · média {round(sum(ages) / len(ages))} dias · mais antiga {max(ages)} dias".replace(",", "."))
            info.font = f_band_info
            back = ws.cell(row=band_row, column=LAST_COL, value="↑ Voltar ao resumo")
            back.hyperlink = f"#'{title}'!A{summary_row_of[name]}"
            back.font = f_link
            ws.row_dimensions[band_row].height = 20
            for col in range(1, LAST_COL + 1):
                ws.cell(row=band_row, column=col).alignment = a_left
            style_head(ws, band_row + 1, detail_headers)
            for offset, item in enumerate(sorted(items, key=age, reverse=True)):
                write_task_row(ws, band_row + 2 + offset, item)
            ws.row_dimensions.group(band_row + 1, band_row + 1 + len(items), outline_level=1, hidden=False)
        return ws

    # ---------- relatórios (ordem das abas) ----------
    descriptions = {}

    if "envelhecimento" in selected_reports:
        aging = [(name, [item for item in valid_rows if band_of(age(item)) == name]) for name, _, _ in bands]
        build_report("envelhecimento", "Envelhecimento", "Faixa de dias em aberto", aging)
        descriptions["envelhecimento"] = "Distribuição das demandas por tempo em aberto."

    if "espaco" in selected_reports:
        build_report("espaco", "Por Espaço", "Espaço do ClickUp", ordered(groups["space"]))
        descriptions["espaco"] = "Demandas, média e mais antiga por espaço."

    if "clientes_espaco" in selected_reports:
        build_report("clientes_espaco", "Clientes por Espaço", "Espaço do ClickUp",
                     ordered(groups["client_space"]), subtitle_extra=" · somente solicitações de clientes")
        descriptions["clientes_espaco"] = "Solicitações de clientes distribuídas por espaço."

    if "cliente" in selected_reports:
        build_report("cliente", "Por Cliente", "Cliente / categoria", ordered(groups["client"]))
        descriptions["cliente"] = "Demandas por cliente, incluindo as internas."

    if "responsavel" in selected_reports:
        build_report("responsavel", "Por Responsável", "Responsável", ordered(groups["owner"]))
        descriptions["responsavel"] = "Carga de demandas por responsável."

    if "status" in selected_reports:
        build_report("status", "Por Status", "Status", ordered(groups["status"]))
        descriptions["status"] = "Concentração das demandas por status."

    if "internas" in selected_reports:
        internal_ages = [age(item) for item in internal_rows]
        kpis = [
            ("Total de demandas internas", len(internal_rows)),
            ("Participação no total (%)", round(len(internal_rows) / total * 100, 1) if total else 0),
            ("Média de dias em aberto", round(sum(internal_ages) / len(internal_ages)) if internal_ages else 0),
            ("Mais antiga (dias)", max(internal_ages, default=0)),
        ]
        build_report("internas", "Demandas Internas", "Espaço do ClickUp", ordered(groups["internal_space"]),
                     kpis=kpis, subtitle_extra=" · somente demandas internas")
        descriptions["internas"] = "Indicadores e distribuição das demandas internas por espaço."

    if "tipo" in selected_reports:
        build_report("tipo", "Por Tipo de Demanda", "Tipo de demanda",
                     [("Demandas internas", internal_rows), ("Solicitações de clientes", client_rows)])
        descriptions["tipo"] = "Comparativo entre demandas internas e solicitações de clientes."

    if "detalhamento" in selected_reports:
        ws = workbook.create_sheet(title="Detalhamento")
        sheet_titles["detalhamento"] = "Detalhamento"
        ws.sheet_properties.tabColor = "10B981"
        headers = detail_headers + ["Tipo de demanda", "Faixa de dias"]
        ws.append(headers)
        for item in sorted(valid_rows, key=age, reverse=True):
            url = valid_link(item.get("url"))
            ws.append([
                safe_cell(item.get("id", "")), safe_cell(item.get("name", "")), safe_cell(item.get("status", "")),
                age(item), safe_cell(item.get("responsavel", "")), safe_cell(item.get("espaco", "")),
                safe_cell(item.get("cliente", "") or "Demandas internas"), safe_cell(item.get("criacao", "")),
                url,
                "Demanda interna" if is_internal(item) else "Solicitação de cliente",
                band_of(age(item)),
            ])
            if url:
                ws.cell(row=ws.max_row, column=9).hyperlink = url
        for index, width in enumerate([18, 55, 22, 16, 32, 32, 26, 16, 48, 24, 18], start=1):
            ws.column_dimensions[get_column_letter(index)].width = width
        for row in ws.iter_rows(min_row=2):
            row[3].number_format = '0 "dias"'
            row[1].alignment = a_wrap
        if ws.max_row > 1:
            table = Table(displayName="tblTarefas", ref=f"A1:{get_column_letter(len(headers))}{ws.max_row}")
            table.tableStyleInfo = TableStyleInfo(name="TableStyleMedium2", showRowStripes=True)
            ws.add_table(table)
        ws.freeze_panes = "A2"
        descriptions["detalhamento"] = "Base completa em tabela do Excel (filtros prontos; serve para criar tabelas dinâmicas)."

    # ---------- Resumo (primeira aba) com índice ----------
    if has_summary:
        ws = workbook.create_sheet(title="Resumo", index=0)
        ws.sheet_view.showGridLines = False
        ws.sheet_properties.tabColor = "0F172A"
        ws.column_dimensions["A"].width = 38
        ws.column_dimensions["B"].width = 70
        ws.column_dimensions["C"].width = 22
        ws["A1"] = "Monitor de Gargalos — Relatório de demandas abertas"
        ws["A1"].font = f_title
        ws.row_dimensions[1].height = 24
        ws["A2"] = f"Gerado em {generated_at} · demandas finalizadas e canceladas não são consideradas"
        ws["A2"].font = f_sub

        distinct_owners = {label(item.get("responsavel")) for item in valid_rows}
        indicators = [
            ("Demandas abertas consideradas", total),
            ("Média de dias em aberto", round(sum(all_ages) / total) if total else 0),
            ("Demanda mais antiga (dias)", max(all_ages, default=0)),
            ("Demandas internas", len(internal_rows)),
            ("Solicitações de clientes", len(client_rows)),
            ("Responsáveis envolvidos", len(distinct_owners)),
        ]
        style_head(ws, 4, ["Indicador", "Resultado"])
        for offset, (name, value) in enumerate(indicators):
            r = 5 + offset
            a, b = ws.cell(row=r, column=1, value=name), ws.cell(row=r, column=2, value=value)
            a.font, b.font = f_body, f_bold
            a.border = b.border = border_bottom
            b.alignment = a_left
            b.number_format = "#,##0"

        index_items = [(key, sheet_titles[key]) for key in
                       ["envelhecimento", "espaco", "clientes_espaco", "cliente", "responsavel",
                        "status", "internas", "tipo", "detalhamento"] if key in sheet_titles]
        if index_items:
            start = 5 + len(indicators) + 2
            ws.cell(row=start, column=1, value="Índice dos relatórios").font = f_section
            style_head(ws, start + 1, ["Relatório", "O que mostra", "Abrir"])
            for offset, (key, title) in enumerate(index_items):
                r = start + 2 + offset
                a = ws.cell(row=r, column=1, value=title)
                b = ws.cell(row=r, column=2, value=descriptions.get(key, ""))
                c = ws.cell(row=r, column=3, value="Ir para a aba →")
                c.hyperlink = f"#'{title}'!A1"
                a.font, b.font, c.font = f_bold, f_body, f_link
                for cell in (a, b, c):
                    cell.border = border_bottom
                c.alignment = a_center
            tip_row = start + 3 + len(index_items)
            ws.cell(row=tip_row, column=1, value="Como navegar").font = f_section
            tips = [
                "• Em cada relatório, o resumo fica no topo; clique em “Ver demandas →” para ir às tarefas daquele grupo.",
                "• Cada grupo de tarefas pode ser recolhido ou expandido com os sinais − / + à esquerda da planilha.",
                "• Em cada grupo, “↑ Voltar ao resumo” leva de volta à linha do resumo; no topo, “← Voltar ao Resumo” leva a esta página.",
                "• A aba Detalhamento é uma tabela do Excel: use Inserir → Tabela Dinâmica para montar análises próprias.",
            ]
            for offset, tip in enumerate(tips, start=1):
                ws.cell(row=tip_row + offset, column=1, value=tip).font = f_body

    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    filename = f"relatorio_monitor_gargalos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return send_file(output, as_attachment=True, download_name=filename,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "5000")),
        debug=False,
    )
