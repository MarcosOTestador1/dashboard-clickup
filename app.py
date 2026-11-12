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
                        app.logger.info(
                            "Cache persistido carregado do Supabase: %s tarefas.",
                            len(tasks),
                        )
                else:
                    app.logger.info(
                        "Supabase ainda não possui um cache completo para carregar."
                    )
            finally:
                conn.close()
        except Exception:
            app.logger.exception(
                "Não foi possível carregar o cache do Supabase. Erro detalhado:"
            )
            _db_cache_loaded = False
            return
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


def _cliente_value(task, cliente_options_by_field_id=None):
    """Extrai Cliente resolvendo dropdown por ID, orderindex ou posição da opção."""
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
            # O ClickUp pode devolver o valor do dropdown como posição numérica.
            try:
                numeric_value = int(value)
            except (TypeError, ValueError):
                numeric_value = None

            # Primeiro tenta o identificador explícito; depois orderindex;
            # por último a posição da opção na lista.
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

        # Texto livre deve ser preservado. Um número sem correspondência em
        # dropdown fica sem classificação, sem inventar nome de cliente.
        field_type = str(field.get("type") or "").casefold()
        if field_type in ("drop_down", "dropdown") and str(value).strip().isdigit():
            app.logger.warning(
                "Opção do campo Cliente não resolvida (field_id=%s, value=%s).",
                field_id or "não informado", value,
            )
            return ""
        return str(value).strip()

    return ""


def _list_id_from_task(task):
    """Obtém o ID da List de origem da tarefa."""
    list_obj = task.get("list") or {}
    value = list_obj.get("id") if isinstance(list_obj, dict) else list_obj
    return str(value) if value not in (None, "") else ""


def _buscar_opcoes_cliente_lista(list_id, token):
    """Consulta os metadados de uma List usando sessão própria da thread."""
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
    """Carrega opções do campo Cliente em paralelo e reutiliza metadados recentes."""
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
        # Limite conservador para acelerar metadados sem disparar excesso de chamadas.
        with ThreadPoolExecutor(max_workers=CLIENTE_FIELDS_MAX_WORKERS) as executor:
            futures = [executor.submit(_buscar_opcoes_cliente_lista, list_id, token)
                       for list_id in lists_to_fetch]
            for future in as_completed(futures):
                list_id, result, error = future.result()
                if error:
                    lists_failed += 1
                    app.logger.warning(
                        "Não foi possível carregar campos personalizados da List %s (%s).",
                        list_id, type(error).__name__,
                    )
                    continue
                lists_ok += 1
                options_by_field_id.update(result)
                with _cliente_fields_cache_lock:
                    _cliente_fields_cache[list_id] = (time.time(), result)

    app.logger.info(
        "Metadados do campo Cliente: %s Lists atendidas por cache/consulta, %s falhas, %s campos com opções.",
        lists_ok, lists_failed, len(options_by_field_id),
    )
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
    """Busca TODAS as páginas de tarefas abertas, independentemente da idade.

    O filtro padrão de antiguidade é aplicado no dashboard. Assim, filtros por
    data de criação podem consultar também tarefas com menos de 100 dias.
    A função só retorna quando encontra uma página vazia; falhas não publicam
    resultados parciais no cache.
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

        collection_started = time.perf_counter()
        pagination_started = collection_started
        now = datetime.now(timezone.utc)
        critical = []
        collected_list_ids = set()
        seen_ids = set()
        seen_page_signatures = set()
        page = 0
        total_received = 0
        app.logger.info("Iniciando busca completa de tarefas abertas no workspace %s.", team.get("id"))

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
                    "Busca completa finalizada: %s tarefas recebidas, %s tarefas abertas após exclusão de status inativos, %s páginas.",
                    total_received, len(critical), page,
                )
                app.logger.info("Tempo de paginação do ClickUp: %.2fs.", time.perf_counter() - pagination_started)
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
                    "_cliente_task": task,
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
        metadata_started = time.perf_counter()
        cliente_options_by_field_id = _carregar_opcoes_cliente_por_lista(
            session, collected_list_ids
        )
        app.logger.info("Tempo de metadados de Cliente: %.2fs.", time.perf_counter() - metadata_started)
        for item in critical:
            task_payload = item.pop("_cliente_task", {})
            item["cliente"] = _cliente_value(
                task_payload, cliente_options_by_field_id
            )

        app.logger.info(
            "Tempo total da coleta ClickUp (sem persistência Supabase): %.2fs; tarefas abertas: %s.",
            time.perf_counter() - collection_started, len(critical),
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
        persistence_started = time.perf_counter()
        try:
            salvar_cache_supabase(tasks, updated_at)
            app.logger.info("Tempo de persistência no Supabase: %.2fs.", time.perf_counter() - persistence_started)
        except Exception as db_exc:
            # O dashboard continua funcional mesmo se o Supabase estiver temporariamente indisponível.
            app.logger.exception("Falha ao persistir cache no Supabase após %.2fs; mantendo cache em memória.", time.perf_counter() - persistence_started)
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
        "Espaço do ClickUp", "Cliente", "Data de criação", "Link ClickUp",
    ]
    if "detalhamento" in selected_reports:
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

    if not workbook.sheetnames:
        return jsonify({"error": "Nenhuma aba foi gerada. Confira os relatórios selecionados."}), 400

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
    """Exporta os quadros da aba Relatórios em planilhas separadas."""
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Alignment, Font, PatternFill
        from openpyxl.utils import get_column_letter
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
        selected_reports = set(available_reports)  # Compatibilidade com chamadas antigas.
    elif not isinstance(requested_reports, list) or not requested_reports:
        return jsonify({"error": "Selecione pelo menos um relatório para exportação."}), 400
    else:
        selected_reports = set(requested_reports)
        if not selected_reports.issubset(available_reports):
            return jsonify({"error": "A seleção contém um tipo de relatório inválido."}), 400
    if len(rows) > 20000:
        return jsonify({"error": "A seleção excede o limite de 20.000 demandas por exportação."}), 413

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

    valid_rows = [item for item in rows if isinstance(item, dict)]
    internal_rows = [
        item for item in valid_rows
        if not str(item.get("cliente") or "").strip()
        or str(item.get("cliente") or "").strip().casefold() == "interno"
    ]

    groups = {"space": {}, "client": {}, "owner": {}, "status": {}}
    for item in valid_rows:
        client = label(item.get("cliente"), "Demandas internas")
        values = {
            "space": label(item.get("espaco")),
            "client": client,
            "owner": label(item.get("responsavel")),
            "status": label(item.get("status"), "Sem status"),
        }
        for key, group_name in values.items():
            groups[key].setdefault(group_name, []).append(item)

    workbook = Workbook()
    workbook.remove(workbook.active)

    def add_sheet(title, headers, data, widths=None, hyperlink_col=None):
        sheet = workbook.create_sheet(title=title[:31])
        sheet.append(headers)
        for cell in sheet[1]:
            cell.font = Font(bold=True, color="FFFFFF")
            cell.fill = PatternFill("solid", fgColor="334155")
            cell.alignment = Alignment(vertical="center", wrap_text=True)
        for values in data:
            sheet.append([safe_cell(value) for value in values])
            if hyperlink_col:
                link_cell = sheet.cell(row=sheet.max_row, column=hyperlink_col)
                link = str(link_cell.value or "").lstrip("'")
                if link.startswith(("https://app.clickup.com/", "https://sharing.clickup.com/")):
                    link_cell.hyperlink = link
                    link_cell.style = "Hyperlink"
        sheet.freeze_panes = "A2"
        if sheet.max_row > 1:
            sheet.auto_filter.ref = sheet.dimensions
        sheet.row_dimensions[1].height = 30
        for col, width in enumerate(widths or [24] * len(headers), start=1):
            sheet.column_dimensions[get_column_letter(col)].width = width
        for row in sheet.iter_rows(min_row=2):
            for cell in row:
                cell.alignment = Alignment(vertical="top", wrap_text=(cell.column == 2))
        return sheet

    detail_headers = ["ID", "Nome da tarefa", "Status", "Dias em aberto", "Responsável",
                      "Espaço do ClickUp", "Cliente", "Data de criação", "Link ClickUp"]
    detail_fields = ["id", "name", "status", "dias", "responsavel", "espaco",
                     "cliente", "criacao", "url"]
    detail_widths = [18, 55, 22, 18, 32, 32, 26, 18, 48]
    used_detail_titles = set()

    def unique_detail_title(prefix, value):
        raw = f"{prefix} - {value}"
        # O Excel proíbe : \\ / ? * [ ] em nomes de abas.
        # Também removemos caracteres de controle e limitamos o nome a 31 caracteres.
        invalid_sheet_chars = set('\\\\/:*?[]')
        cleaned = "".join(
            "-" if char in invalid_sheet_chars or ord(char) < 32 else char
            for char in raw
        )
        base = cleaned.strip().strip("'") or "Detalhamento"
        base = base[:31].rstrip("'") or "Detalhamento"
        candidate = base
        suffix = 2
        while candidate.casefold() in used_detail_titles or candidate in workbook.sheetnames:
            ending = f" ({suffix})"
            candidate = base[:31 - len(ending)] + ending
            suffix += 1
        used_detail_titles.add(candidate.casefold())
        return candidate

    def add_detail_sheet(prefix, value, items, source_sheet=None, source_row=None):
        detail_title = unique_detail_title(prefix, value)
        ordered_items = sorted(items, key=age, reverse=True)
        detail_data = [[item.get(field, "") for field in detail_fields] for item in ordered_items]
        detail_sheet = add_sheet(detail_title, detail_headers, detail_data, detail_widths, hyperlink_col=9)
        for row in detail_sheet.iter_rows(min_row=2):
            row[3].number_format = '0 "dias"'
        if source_sheet and source_row:
            back = detail_sheet.cell(row=1, column=11, value="↩ Voltar ao relatório")
            back.hyperlink = f"#'{source_sheet}'!A{source_row}"
            back.style = "Hyperlink"
        return detail_title

    def add_group_report(title, headers, group, widths):
        ordered_groups = sorted(group.items(), key=lambda pair: (-len(pair[1]), pair[0].casefold()))
        summary_data = []
        for name, items in ordered_groups:
            ages = [age(item) for item in items]
            summary_data.append([name, len(items), round(sum(ages) / len(ages)), max(ages), "Ver demandas"])
        sheet = add_sheet(title, headers + ["Consultar"], summary_data, widths + [18])
        for index, (name, items) in enumerate(ordered_groups, start=2):
            detail_title = add_detail_sheet(title, name, items, title, index)
            link_cell = sheet.cell(row=index, column=len(headers) + 1)
            link_cell.hyperlink = f"#'{detail_title}'!A1"
            link_cell.style = "Hyperlink"
        return sheet

    def group_summary(group):
        result = []
        for name, items in sorted(group.items(), key=lambda pair: (-len(pair[1]), pair[0].casefold())):
            ages = [age(item) for item in items]
            result.append([name, len(items), round(sum(ages) / len(ages)), max(ages)])
        return result

    total = len(valid_rows)
    all_ages = [age(item) for item in valid_rows]
    client_rows = [item for item in valid_rows if item not in internal_rows]
    distinct_owners = {label(item.get("responsavel")) for item in valid_rows}

    if "resumo" in selected_reports:
        add_sheet("Resumo", ["Indicador", "Resultado"], [
            ["Demandas abertas consideradas", total],
            ["Média de dias em aberto", round(sum(all_ages) / total) if total else 0],
            ["Demanda mais antiga (dias)", max(all_ages, default=0)],
            ["Demandas internas", len(internal_rows)],
            ["Solicitações de clientes", len(client_rows)],
            ["Responsáveis envolvidos", len(distinct_owners)],
        ], [42, 24])

    if "envelhecimento" in selected_reports:
        aging_groups = {}
        aging_rows = []
        bands = [
            ("Até 30 dias", 0, 30),
            ("31 a 60 dias", 31, 60),
            ("61 a 100 dias", 61, 100),
            ("101 a 150 dias", 101, 150),
            ("151 a 200 dias", 151, 200),
            ("Mais de 200 dias", 201, float("inf")),
        ]
        for name, low, high in bands:
            aging_groups[name] = [item for item in valid_rows if low <= age(item) <= high]
            aging_rows.append([name, len(aging_groups[name]), "Ver demandas"])
        aging_sheet = add_sheet("Envelhecimento",
                                ["Faixa de dias em aberto", "Quantidade de demandas", "Consultar"],
                                aging_rows, [30, 28, 18])
        for index, (name, items) in enumerate(aging_groups.items(), start=2):
            detail_title = add_detail_sheet("Envelhecimento", name, items, "Envelhecimento", index)
            cell = aging_sheet.cell(row=index, column=3)
            cell.hyperlink = f"#'{detail_title}'!A1"
            cell.style = "Hyperlink"

    if "espaco" in selected_reports:
        add_group_report("Por Espaço", ["Espaço", "Demandas", "Média de dias", "Mais antiga (dias)"],
                         groups["space"], [36, 16, 18, 20])
    if "clientes_espaco" in selected_reports:
        client_tasks_by_space = {}
        for item in valid_rows:
            client = str(item.get("cliente") or "").strip()
            if client and client.casefold() != "interno":
                space = label(item.get("espaco"), "Espaço não identificado")
                client_tasks_by_space.setdefault(space, []).append(item)
        add_group_report("Clientes por Espaço",
                         ["Espaço", "Solicitações de clientes", "Média de dias", "Mais antiga (dias)"],
                         client_tasks_by_space, [36, 26, 18, 20])
    if "cliente" in selected_reports:
        add_group_report("Por Cliente", ["Cliente / categoria", "Demandas", "Média de dias", "Mais antiga (dias)"],
                         groups["client"], [36, 16, 18, 20])

    if "responsavel" in selected_reports:
        add_group_report("Por Responsável", ["Responsável", "Demandas", "Média de dias", "Mais antiga (dias)"],
                         groups["owner"], [36, 16, 18, 20])

    if "internas" in selected_reports:
        internal_ages = [age(item) for item in internal_rows]
        internal_sheet = add_sheet("Demandas Internas", ["Indicador", "Resultado", "Consultar"], [
            ["Total de demandas internas", len(internal_rows), "Ver demandas"],
            ["Participação no total (%)", round(len(internal_rows) / total * 100, 1) if total else 0, ""],
            ["Média de dias em aberto", round(sum(internal_ages) / len(internal_ages)) if internal_ages else 0, ""],
            ["Mais antiga (dias)", max(internal_ages, default=0), ""],
        ], [36, 24, 18])
        internal_detail = add_detail_sheet("Demandas Internas", "Todas", internal_rows, "Demandas Internas", 2)
        internal_link = internal_sheet.cell(row=2, column=3)
        internal_link.hyperlink = f"#'{internal_detail}'!A1"
        internal_link.style = "Hyperlink"

    if "status" in selected_reports:
        add_group_report("Por Status", ["Status", "Demandas", "Média de dias", "Mais antiga (dias)"],
                         groups["status"], [32, 16, 18, 20])

    if "tipo" in selected_reports:
        # A nova aba compara as mesmas duas categorias usadas no resumo.
        type_groups = {
            "Demandas internas": internal_rows,
            "Solicitações de clientes": client_rows,
        }
        add_group_report("Por Tipo de Demanda",
                         ["Tipo de demanda", "Demandas", "Média de dias", "Mais antiga (dias)"],
                         type_groups, [36, 16, 18, 20])

    if "detalhamento" in selected_reports:
        fields = ["id", "name", "status", "dias", "responsavel", "espaco", "cliente", "criacao", "url"]
        headers = ["ID", "Nome da tarefa", "Status", "Dias em aberto", "Responsável",
                   "Espaço do ClickUp", "Cliente", "Data de criação", "Link ClickUp"]
        detail_rows = []
        for item in sorted(valid_rows, key=age, reverse=True):
            detail_rows.append([item.get(field, "") for field in fields])
        detail_sheet = add_sheet("Detalhamento", headers, detail_rows,
                                 [18, 55, 22, 18, 32, 32, 26, 18, 48], hyperlink_col=9)
        for row in detail_sheet.iter_rows(min_row=2):
            row[3].number_format = '0 "dias"'

    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    filename = f"relatorio_monitor_gargalos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return send_file(output, as_attachment=True, download_name=filename,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


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
