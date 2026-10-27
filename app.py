# -*- coding: utf-8 -*-
"""Dashboard ClickUp: Flask backend for deployment."""
import os
import time
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, render_template

app = Flask(__name__, template_folder="templates")
API_BASE = "https://api.clickup.com/api/v2"
STATUS_INATIVOS = {
    "cancelado", "cancelled", "canceled", "finalizado", "closed",
    "done", "concluido", "concluído", "complete", "completed",
}
REQUEST_TIMEOUT = 30


def _safe_json(response):
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Resposta inesperada da API do ClickUp.")
    return payload


def buscar_tarefas_clickup():
    """Busca tarefas ativas. O token fica somente no servidor."""
    token = os.getenv("CLICKUP_API_TOKEN", "").strip()
    if not token:
        raise RuntimeError("O serviço ainda não foi configurado com CLICKUP_API_TOKEN.")

    session = requests.Session()
    session.headers.update({"Authorization": token, "Accept": "application/json"})

    teams = _safe_json(session.get(f"{API_BASE}/team", timeout=REQUEST_TIMEOUT)).get("teams", [])
    if not teams:
        raise RuntimeError("Nenhum workspace do ClickUp foi encontrado para este token.")

    # Opcional: defina CLICKUP_TEAM_ID no ambiente para escolher um workspace específico.
    team_id_configurado = os.getenv("CLICKUP_TEAM_ID", "").strip()
    team = next((t for t in teams if str(t.get("id")) == team_id_configurado), None) if team_id_configurado else teams[0]
    if not team:
        raise RuntimeError("CLICKUP_TEAM_ID não corresponde a um workspace disponível para o token.")

    agora = datetime.now(timezone.utc)
    tarefas_formatadas = []
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
            if status.lower().strip() in STATUS_INATIVOS:
                continue

            date_created_ms = task.get("date_created")
            if not date_created_ms:
                continue
            try:
                dt_criacao = datetime.fromtimestamp(int(date_created_ms) / 1000, tz=timezone.utc)
            except (TypeError, ValueError, OSError, OverflowError):
                continue

            assignees = task.get("assignees") or []
            responsaveis = ", ".join(
                str(a.get("username") or a.get("email") or "")
                for a in assignees if isinstance(a, dict)
            ).strip(", ") or "Não atribuído"
            space_obj = task.get("space") or {}

            tarefas_formatadas.append({
                "id": str(task.get("custom_id") or task.get("id") or ""),
                "name": str(task.get("name") or "Sem título"),
                "status": status or "Sem status",
                "dias": max(0, (agora - dt_criacao).days),
                "criacao": dt_criacao.strftime("%Y-%m-%d"),
                "responsavel": responsaveis,
                "espaco": str(space_obj.get("name") or "Geral"),
                "url": str(task.get("url") or "#"),
            })

        page += 1
        time.sleep(0.15)

    return tarefas_formatadas


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
        tasks = buscar_tarefas_clickup()
        # O token nunca é incluído na resposta; apenas os campos necessários ao painel.
        return jsonify({"tasks": tasks, "updated_at": datetime.now(timezone.utc).isoformat()})
    except requests.HTTPError as exc:
        app.logger.warning("ClickUp API retornou HTTP %s", exc.response.status_code if exc.response else "desconhecido")
        return jsonify({"error": "Não foi possível consultar o ClickUp. Verifique o token e as permissões configuradas no servidor."}), 502
    except (requests.RequestException, ValueError) as exc:
        app.logger.warning("Falha de comunicação com ClickUp: %s", type(exc).__name__)
        return jsonify({"error": "Falha de comunicação com o ClickUp. Tente novamente em alguns instantes."}), 502
    except RuntimeError as exc:
        app.logger.warning("Configuração ClickUp: %s", exc)
        return jsonify({"error": str(exc)}), 503


@app.get("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    # Apenas para teste local; em produção use Gunicorn (configurado no Render).
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False)
