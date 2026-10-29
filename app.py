    except requests.HTTPError as exc:
        code = exc.response.status_code if exc.response is not None else "desconhecido"
        app.logger.warning("ClickUp API retornou HTTP %s", code)
        return jsonify({
            "error": "Não foi possível consultar o ClickUp. Verifique o token e as permissões configuradas no Render."
        }), 502

    except (requests.RequestException, ValueError) as exc:
        app.logger.warning("Falha de comunicação com ClickUp: %s", type(exc).__name__)
        return jsonify({
            "error": "Falha de comunicação com o ClickUp. Tente novamente em alguns instantes."
        }), 502

    except RuntimeError as exc:
        app.logger.warning("Configuração ClickUp: %s", exc)
        return jsonify({"error": str(exc)}), 503

    except Exception:
        app.logger.exception("Erro inesperado ao carregar tarefas do ClickUp.")
        return jsonify({
            "error": "Erro interno ao carregar as tarefas. Consulte os logs do servidor."
        }), 500


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


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "5000")),
        debug=False,
