# -*- coding: utf-8 -*-
"""Backend Flask do Dashboard ClickUp: filtro por data, retries e cache de contingência."""
import logging
import os
import hmac
import threading
import time
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
# Não limitamos a quantidade de páginas: a busca só termina quando o ClickUp
# devolver uma página vazia. A atualização roda em segundo plano.

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
    """Carrega o último cache persistido; não falha a aplicação se o DB estiver indisponível."""
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
                        app.logger.info("Cache persistido carregado do Supabase: %s tarefas.", len(tasks))
                else:
                    app.logger.info("Supabase ainda não possui um cache completo para carregar.")
            finally:
                conn.close()
        except Exception:            # O dashboard continua funcional mesmo se o Supabase estiver temporariamente indisponível.            app.logger.exception(                "Falha ao persistir cache no Supabase. Erro detalhado:"            )
        _db_cache_loaded = True


def salvar_cache_supabase(tasks, updated_at):
    """Persiste somente uma coleta completa na tabela criada no Supabase."""
    conn = _db_connect()
    if conn is None:
        app.logger.warning("DATABASE_URL não configurada; resultados não serão persistidos entre reinicializações.")
        return False
    try:
        # A coluna updated_at é TIMESTAMPTZ; convertê-la para datetime com fuso.
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
    """Consulta a API com timeout por página e novas tentativas para falhas transitórias."""
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
                    delay = min(30.0, max(1.0, float(retry_after))) if retry_after else min(8.0, 2 ** attempt)
                except (TypeError, ValueError):
                    delay = min(8.0, 2 ** attempt)
                app.logger.warning(
                    "ClickUp respondeu HTTP %s; nova tentativa em %.1fs (%s/%s).",
                    response.status_code, delay, attempt + 1, MAX_RETRIES,
                )
                time.sleep(delay)
                continue
            return _safe_json(response)
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = exc
            if attempt >= MAX_RETRIES - 1:
                raise
            delay = min(8.0, 2 ** attempt)
            app.logger.warning(
                "Falha temporária de rede com ClickUp (%s); nova tentativa em %ss (%s/%s).",
                type(exc).__name__, delay, attempt + 1, MAX_RETRIES,
            )
            time.sleep(delay)
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
    """Busca TODAS as páginas de tarefas abertas criadas há mais de 100 dias.

    A função só retorna quando encontra uma página vazia. Se qualquer página
    falhar ou a API repetir a mesma página, lança erro e não publica resultados
    parciais no cache.
    """
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
        except (requests.RequestException, ValueError) as exc:
            app.logger.warning("Não foi possível carregar a lista de Spaces: %s", type(exc).__name__)

        now = datetime.now(timezone.utc)
        cutoff_ms = int((now - timedelta(days=DIAS_MINIMOS)).timestamp() * 1000)
        critical = []
        seen_ids = set()
        seen_page_signatures = set()
        page = 0
        total_received = 0
        app.logger.info("Iniciando busca completa de tarefas antigas no workspace %s.", team.get("id"))

        while True:
            # A falha em uma página não reinicia a coleta desde a página zero.
            # Repetimos a mesma página e mantemos o que já foi coletado em memória.
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
                            "date_created_lt": cutoff_ms,
                            "include_closed": "false",
                        },
                    )
                    page_error = None
                    break
                except (requests.RequestException, ValueError) as exc:
                    page_error = exc
                    if page_attempt == PAGE_RETRIES - 1:
                        app.logger.exception(
                            "Falha persistente na página %s após %s tentativas; coleta interrompida sem substituir o cache.",
                            page, PAGE_RETRIES,
                        )
                        raise
                    delay = min(30, 2 ** page_attempt)
                    app.logger.warning(
                        "Página %s falhou (%s/%s: %s); tentando a mesma página novamente em %ss.",
                        page, page_attempt + 1, PAGE_RETRIES, type(exc).__name__, delay,
                    )
                    time.sleep(delay)
            if page_error is not None:
                raise page_error

            tasks = payload.get("tasks", [])
            if not tasks:
                app.logger.info(
                    "Busca completa finalizada: %s tarefas recebidas, %s tarefas críticas após filtros, %s páginas.",
                    total_received, len(critical), page,
                )
                break

            # Protege contra uma API que, por erro, devolva indefinidamente a mesma página.
            page_ids = [str(t.get("id") or t.get("custom_id") or "") for t in tasks]
            signature = tuple(page_ids)
            if signature in seen_page_signatures:
                raise RuntimeError(
                    f"A API do ClickUp repetiu a página {page}; atualização cancelada para não publicar dados incompletos."
                )
            seen_page_signatures.add(signature)
            total_received += len(tasks)

            for task in tasks:
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
                if days <= DIAS_MINIMOS:
                    continue

                responsible_names = []
                for assignee in (task.get("assignees") or []):
                    if isinstance(assignee, dict):
                        name = assignee.get("username") or assignee.get("email") or assignee.get("initials")
                        if name:
                            responsible_names.append(str(name))
                critical.append({
                    "id": str(task.get("custom_id") or task.get("id") or ""),
                    "name": str(task.get("name") or "Sem título"),
                    "status": status or "Sem status",
                    "dias": days,
                    "criacao": created.strftime("%Y-%m-%d"),
                    "responsavel": ", ".join(responsible_names) or "Não atribuído",
                    "espaco": _space_name(task, spaces_by_id),
                    "url": str(task.get("url") or "#"),
                })
            page += 1
            with _progress_lock:
                _progress.update(tasks=list(critical), page=page, received=total_received, active=True)
            if page % 5 == 0:
                app.logger.info(
                    "Busca ClickUp em andamento: %s páginas consultadas; %s tarefas recebidas; %s tarefas críticas acumuladas.",
                    page, total_received, len(critical),
                )
        return critical
    finally:
        session.close()


def _refresh_worker():
    """Atualiza em segundo plano e só troca o cache após uma busca completa."""
    global _refresh_thread, _refreshing, _last_refresh_error
    with _progress_lock:
        _progress.update(tasks=[], page=0, received=0, active=True)
    try:
        tasks = buscar_tarefas_clickup()
        updated_at = datetime.now(timezone.utc).isoformat()
        # A coleta só é publicada após terminar todas as páginas. O banco é atualizado
        # antes do cache em memória para manter o último snapshot persistido íntegro.
        try:
            salvar_cache_supabase(tasks, updated_at)
        except Exception as db_exc:
            # O dashboard continua funcional mesmo se o Supabase estiver temporariamente indisponível.
            app.logger.warning("Falha ao persistir cache no Supabase (%s); mantendo cache em memória.", type(db_exc).__name__)
        with _cache_lock:
            _cache.update(at=time.time(), tasks=tasks, updated_at=updated_at)
        _last_refresh_error = None
        app.logger.info("Cache atualizado com %s tarefas após coleta completa.", len(tasks))
    except Exception as exc:
        _last_refresh_error = type(exc).__name__
        app.logger.exception("Atualização completa do ClickUp falhou; cache anterior foi preservado.")
    finally:
        with _progress_lock:
            _progress["active"] = False
        with _refresh_lock:
            _refreshing = False
            _refresh_thread = None


def iniciar_atualizacao(force=False):
    """Inicia no máximo uma atualização; retorna imediatamente ao navegador."""
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


# Estado em memória do processo. Uma reinicialização provoca nova busca completa.
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
        # complete descreve o cache servido, não a coleta que está em andamento.
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
        payload["error"] = (
            "Não foi possível concluir a busca completa no ClickUp. Nenhum resultado parcial foi marcado como completo. "
            "Consulte os logs do Render e tente atualizar novamente."
        )
        return jsonify(payload), 502
    if _last_refresh_error and tasks is not None:
        payload["warning"] = (
            "A última atualização falhou; os dados completos do cache anterior continuam disponíveis."
        )
    return jsonify(payload), 200


@app.post("/api/refresh")
def scheduled_refresh():
    """Endpoint protegido para iniciar a atualização agendada pelo GitHub Actions."""
    expected = (os.getenv("CRON_SECRET") or "").strip()
    supplied = (request.headers.get("X-Cron-Secret") or "").strip()
    if not expected:
        app.logger.error("CRON_SECRET não está configurado; atualização agendada recusada.")
        return jsonify({"ok": False, "error": "Agendamento não configurado no servidor."}), 503
    if not supplied or not hmac.compare_digest(supplied, expected):
        return jsonify({"ok": False, "error": "Não autorizado."}), 401

    carregar_cache_supabase()
    started = iniciar_atualizacao(force=True)
    with _refresh_lock:
        refreshing = _refreshing
    if started:
        return jsonify({"ok": True, "started": True, "message": "Atualização completa iniciada em segundo plano."}), 202
    if refreshing:
        return jsonify({"ok": True, "started": False, "message": "Já existe uma atualização em andamento."}), 200
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
        "Espaço do ClickUp", "Data de criação", "Link ClickUp",
    ]
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
        # Mitiga fórmulas maliciosas em valores exportados.
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


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


@app.get("/health/cache")
def health_cache():
    """Diagnóstico seguro de cache; não retorna tarefas nem dados de conexão."""
    with _cache_lock:
        has_cache = _cache["tasks"] is not None
        task_count = len(_cache["tasks"] or [])
        updated_at = _cache["updated_at"]
    db_configured = bool(_database_url())
    db_available = False
    if db_configured:
        conn = None
        try:
            conn = _db_connect()
            with conn.cursor() as cur:
                cur.execute("SELECT complete, updated_at, jsonb_array_length(tasks) FROM public.clickup_dashboard_cache WHERE id = 1")
                row = cur.fetchone()
            db_available = True
            db_info = {"record_exists": row is not None, "complete": bool(row[0]) if row else False,
                       "updated_at": row[1].isoformat() if row and row[1] else None,
                       "task_count": row[2] if row else 0}
        except Exception as exc:
            db_info = {"error_type": type(exc).__name__}
        finally:
            if conn is not None:
                conn.close()
    else:
        db_info = {"configured": False}
    with _refresh_lock:
        refreshing = _refresh_refreshing if '_refresh_refreshing' in globals() else _refreshing
    return jsonify({"memory_cache": {"available": has_cache, "task_count": task_count, "updated_at": updated_at},
                    "supabase": {"configured": db_configured, "available": db_available, **db_info},
                    "refreshing": refreshing, "last_refresh_error": _last_refresh_error})


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "5000")),
        debug=False,
    )
