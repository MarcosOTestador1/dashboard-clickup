# Dashboard ClickUp — Monitor de Gargalos

Aplicação Flask que consulta tarefas ativas da API do ClickUp e exibe tarefas com mais de 100 dias em aberto, mantendo o visual do dashboard original.

## Arquivos

- `app.py`: servidor Flask e integração com a API do ClickUp.
- `templates/index.html`: interface, indicadores, gráficos e tabela.
- `requirements.txt`: dependências para hospedagem.
- `.gitignore`: evita enviar ambientes virtuais e arquivos `.env` ao Git.

## Configuração local (Windows PowerShell)

```powershell
cd caminho\para\dashboard_clickup_project
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:CLICKUP_API_TOKEN="SEU_TOKEN_DO_CLICKUP"
# Opcional:
# $env:CLICKUP_TEAM_ID="ID_DO_WORKSPACE"
python app.py
```

Abra `http://127.0.0.1:5000` no navegador.

## Publicação no Render

1. Envie os arquivos deste projeto para a raiz do repositório GitHub privado.
2. No Render, crie um **New + Web Service** e conecte o repositório.
3. Build Command: `pip install -r requirements.txt`
4. Start Command: `gunicorn app:app`
5. Em **Environment**, configure `CLICKUP_API_TOKEN` com o token real. Opcionalmente, configure `CLICKUP_TEAM_ID`.
6. Publique e teste a URL gerada.

Não coloque o token real em `app.py`, HTML, README, commits ou mensagens. Se um token já foi enviado ao GitHub, revogue-o no ClickUp e gere outro.

## Aviso de acesso

Esta versão não exige login. Qualquer pessoa que consiga acessar a URL poderá consultar os dados que o backend disponibiliza. Um repositório privado não torna o site privado. Não publique desta forma se as tarefas contiverem dados confidenciais ou se a política da empresa exigir acesso restrito.

## Observações

- O dashboard consulta a API ao abrir a página e ao clicar em **Atualizar dados**.
- O token permanece no servidor; não é enviado ao navegador.
- Os nomes de tarefas e demais campos são inseridos no HTML como texto, evitando interpretar conteúdo da API como HTML.
- O filtro de tarefas críticas continua sendo `dias > 100`.
- O workspace padrão é o primeiro retornado pelo ClickUp; defina `CLICKUP_TEAM_ID` para escolher outro.
