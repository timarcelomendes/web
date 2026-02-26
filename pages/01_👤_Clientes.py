import re, hashlib
import streamlit as st
import pandas as pd
from datetime import date
import requests
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
import time
from db import get_engine, exec_sql
from ui import sidebar_logo

sidebar_logo()

PERFIS = ["Decisor", "Influenciador"]

# Agora vem do secrets.toml
N8N_FORCE_URL = (st.secrets.get("N8N_FORCE_URL") or "").strip()

def disparar_n8n_force(cliente_id: str) -> tuple[bool, str, dict]:
    base_url = (N8N_FORCE_URL or "").strip()
    if not base_url:
        return False, "N8N_FORCE_URL não configurada.", {"stage": "config"}

    payload = {"cliente_id": cliente_id}

    # ✅ se vier /webhook-test/, prepara fallback para /webhook/
    urls_to_try = [base_url]
    if "/webhook-test/" in base_url:
        urls_to_try.append(base_url.replace("/webhook-test/", "/webhook/"))

    last_details = {}

    for url in urls_to_try:
        t0 = time.time()
        try:
            resp = requests.post(url, json=payload, timeout=40)
            ms = int((time.time() - t0) * 1000)

            if resp.status_code in (404, 410):
                last_details = {
                    "stage": "webhook_not_listening",
                    "http": resp.status_code,
                    "ms": ms,
                    "url": url,
                    "body": (resp.text or "")[:800],
                }
                # tenta próxima URL (fallback)
                continue

            if not (200 <= resp.status_code < 300):
                return False, f"Falha ao chamar webhook (HTTP {resp.status_code}).", {
                    "stage": "http_error", "http": resp.status_code, "ms": ms, "url": url, "body": (resp.text or "")[:800]
                }

            # sucesso
            try:
                data = resp.json()
            except Exception:
                data = None

            if isinstance(data, dict) and data.get("ok") is True:
                exec_id = data.get("executionId", "")
                msg = "Fluxo iniciado no n8n ✅"
                if exec_id:
                    msg += f" (ExecutionId: {exec_id})"
                return True, msg, {"stage": "started", "ms": ms, "url": url, "data": data}

            return True, "Envio concluído ✅ (sem confirmação explícita)", {
                "stage": "ok_no_confirm", "ms": ms, "url": url,
                "data": data if isinstance(data, dict) else None,
                "body": None if isinstance(data, dict) else (resp.text or "")[:800]
            }

        except requests.exceptions.Timeout:
            return False, "Timeout (40s) ao conectar no n8n.", {"stage": "timeout", "url": url}
        except Exception as e:
            return False, "Erro ao conectar no n8n.", {"stage": "exception", "url": url, "error": str(e)}

    # se tentou tudo e só deu 404/410:
    return False, (
        "Webhook do n8n não está disponível (404/410). "
        "Se estiver usando webhook-test, clique em Execute workflow/Test (Listening) ou use /webhook/ em produção."
    ), last_details

def normalize(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def make_cliente_id(email: str, empresa: str) -> str:
    base = f"{normalize(empresa)}|{normalize(email)}"
    digest = hashlib.sha256(base.encode("utf-8")).hexdigest()
    return "C" + digest[:16]

def load_clientes(q: str, ativo: str, perfil: str, topn: int):

    where = []
    params = {}

    # --- Status ativo ---
    if ativo == "Ativos":
        where.append("c.ativo = :ativo")
        params["ativo"] = 1
    elif ativo == "Inativos":
        where.append("c.ativo = :ativo")
        params["ativo"] = 0

    # --- Perfil ---
    if perfil in PERFIS:
        where.append("c.perfil_decisor = :perfil")
        params["perfil"] = perfil

    # --- Busca ---
    if (q or "").strip():
        where.append("""
        (
          LOWER(c.nome) LIKE :like OR
          LOWER(c.email) LIKE :like OR
          LOWER(c.empresa) LIKE :like OR
          CAST(c.cliente_id AS NVARCHAR(100)) LIKE :like_id
        )
        """)
        params["like"] = f"%{q.strip().lower()}%"
        params["like_id"] = f"%{q.strip()}%"

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    sql = f"""
    SELECT TOP ({int(topn)})
        c.cliente_id,
        c.nome,
        c.email,
        c.empresa,
        c.perfil_decisor,
        c.segmento,
        c.status_envio,
        c.ultimo_erro,
        c.ultimo_envio,
        c.proximo_envio,
        c.ativo,
        c.updated_at,
        c.status_execucao,
        c.last_execution_id,
        c.exec_started_at,
        c.exec_finished_at,

        -- Respostas por cliente
        (
            SELECT COUNT(1)
            FROM dbo.nps_respostas r
            WHERE r.cliente_id = c.cliente_id
        ) AS respostas_cliente,

        -- Respostas por empresa
        (
            SELECT COUNT(1)
            FROM dbo.nps_respostas r2
            WHERE r2.empresa = c.empresa
        ) AS respostas_empresa

    FROM dbo.nps_clientes c
    {where_sql}
    ORDER BY c.updated_at DESC;
    """

    df = read_df(sql, params)
    return df

def update_cliente(cliente_id: str, nome: str, email: str, empresa: str, perfil_decisor: str, segmento: str):
    sql = """
    UPDATE dbo.nps_clientes
    SET
      nome = NULLIF(LTRIM(RTRIM(:nome)), ''),
      email = NULLIF(LTRIM(RTRIM(:email)), ''),
      empresa = NULLIF(LTRIM(RTRIM(:empresa)), ''),
      perfil_decisor = NULLIF(LTRIM(RTRIM(:perfil_decisor)), ''),
      segmento = NULLIF(LTRIM(RTRIM(:segmento)), ''),
      updated_at = SYSUTCDATETIME()
    WHERE cliente_id = :cliente_id;
    """

    exec_sql(sql, {
        "cliente_id": cliente_id,
        "nome": nome,
        "email": email,
        "empresa": empresa,
        "perfil_decisor": perfil_decisor,
        "segmento": segmento or "",
    })

def forcar_elegivel(cliente_id: str):
    sql = """
    UPDATE dbo.nps_clientes
    SET
      ativo = 1,
      status_envio = 'Pendente',
      proximo_envio = CAST(GETDATE() AS DATE),
      ultimo_erro = NULL,
      updated_at = SYSUTCDATETIME()
    WHERE cliente_id = :cliente_id;
    """
    exec_sql(sql, {"cliente_id": cliente_id})

def forcar_envio(cliente_id: str):
    sql = """
    UPDATE dbo.nps_clientes
    SET
      ativo = 1,
      status_envio = 'Pendente',
      ultimo_envio = NULL,  -- força reenviar hoje
      proximo_envio = CAST(GETDATE() AS DATE),
      ultimo_erro = NULL,
      updated_at = SYSUTCDATETIME()
    WHERE cliente_id = :cliente_id;
    """
    exec_sql(sql, {"cliente_id": cliente_id})

def read_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        rows = result.fetchall()
        cols = list(result.keys())
    return pd.DataFrame(rows, columns=cols)

def load_one(cliente_id: str):
    sql = """
    SELECT TOP 1
      cliente_id, nome, email, empresa, perfil_decisor, segmento,
      ativo, status_envio, ultimo_envio, proximo_envio, ultimo_erro,
      created_at, updated_at
    FROM dbo.nps_clientes
    WHERE cliente_id = :cliente_id;
    """
    df = read_df(sql, {"cliente_id": cliente_id})
    if df.empty:
        return None
    return df.iloc[0].to_dict()

def create_cliente(nome, email, empresa, perfil_decisor, segmento):
    sql = """
    INSERT INTO dbo.nps_clientes
    (
        cliente_id,
        nome,
        email,
        empresa,
        perfil_decisor,
        segmento,
        status_envio,
        created_at
    )
    VALUES
    (
        NEWID(),  -- ou seu padrão de ID
        :nome,
        :email,
        :empresa,
        :perfil_decisor,
        :segmento,
        'Novo',
        SYSUTCDATETIME()
    );
    """

    exec_sql(sql, {
        "nome": (nome or "").strip(),
        "email": (email or "").strip(),
        "empresa": (empresa or "").strip(),
        "perfil_decisor": (perfil_decisor or "").strip(),
        "segmento": (segmento or "").strip(),
    })

def insert_cliente(nome, email, empresa, perfil_decisor, segmento):
    cliente_id = make_cliente_id(email, empresa)

    sql = """
    INSERT INTO dbo.nps_clientes
      (cliente_id, nome, email, empresa, perfil_decisor, segmento,
       ativo, status_envio, ultimo_envio, proximo_envio, ultimo_erro,
       created_at, updated_at)
    VALUES
      (:cliente_id, :nome, :email, :empresa, :perfil_decisor, :segmento,
       1, 'Pendente', NULL, CAST(GETDATE() AS DATE), NULL,
       SYSUTCDATETIME(), SYSUTCDATETIME());
    """

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(sql),
            {
                "cliente_id": cliente_id,
                "nome": (nome or "").strip(),
                "email": (email or "").strip(),
                "empresa": (empresa or "").strip(),
                "perfil_decisor": perfil_decisor,
                "segmento": (segmento or "").strip() or None,
            }
        )

    return cliente_id

def wait_status_update(cliente_id: str, timeout_s: int = 60, every_s: float = 1.0):
    """
    Aguarda o status_envio/ultimo_envio mudar após disparar o n8n.
    Retorna (done: bool, snapshot: dict).
    """
    t0 = time.time()
    today = date.today().isoformat()

    while (time.time() - t0) < timeout_s:
        row = load_one(cliente_id)
        if not row:
            return False, {"stage": "not_found"}

        status = (row.get("status_envio") or "").strip()
        ultimo = row.get("ultimo_envio")
        ultimo_s = str(ultimo)[:10] if ultimo else ""

        if status in ("Enviado", "Erro", "Respondido"):
            return True, {
                "stage": "done",
                "status_envio": status,
                "ultimo_envio": ultimo_s,
                "ultimo_erro": row.get("ultimo_erro"),
            }

        if ultimo_s == today:
            return True, {"stage": "done_by_date", "status_envio": status, "ultimo_envio": ultimo_s}

        time.sleep(every_s)

    return False, {"stage": "timeout_waiting_sql"}

def set_ativo(cliente_id: str, ativo: int):
    sql = """
    UPDATE dbo.nps_clientes
    SET
      ativo = :ativo,
      updated_at = SYSUTCDATETIME()
    WHERE cliente_id = :cliente_id;
    """
    exec_sql(sql, {"cliente_id": cliente_id, "ativo": int(ativo)})

def is_elegivel_row(row: dict) -> bool:
    """
    Elegível para envio manual (sem considerar ultimo_envio, porque o lote vai forçar).
    Usa regra parecida com o SQL do agendado:
      - ativo=1
      - status_envio Pendente ou Erro
      - proximo_envio <= hoje (ou NULL)
      - email válido
      - não está com status_execucao running
    """
    try:
        ativo = int(row.get("ativo") or 0)
    except Exception:
        ativo = 0

    status = (row.get("status_envio") or "").strip()
    email = (row.get("email") or "").strip()
    execs = (row.get("status_execucao") or "").strip().lower()

    # proximo_envio pode vir como date/datetime/string
    prox = row.get("proximo_envio")
    try:
        prox_date = pd.to_datetime(prox).date() if prox not in (None, "") else None
    except Exception:
        prox_date = None

    hoje = date.today()

    janela_ok = (prox_date is None) or (prox_date <= hoje)

    return (
        ativo == 1
        and status in ("Pendente", "Erro")
        and janela_ok
        and ("@" in email)
        and (execs != "running")
    )

def try_lock_execucao(cliente_id: str) -> bool:
    """
    Tenta travar o cliente para execução (evita disparos duplicados).
    Retorna True se conseguiu travar (rowcount=1).
    """
    sql = """
    UPDATE dbo.nps_clientes
    SET
      status_execucao = 'running',
      exec_started_at = SYSUTCDATETIME(),
      exec_finished_at = NULL,
      updated_at = SYSUTCDATETIME()
    WHERE
      cliente_id = :cliente_id
      AND ativo = 1
      AND (status_execucao IS NULL OR status_execucao <> 'running');
    """

    engine = get_engine()
    with engine.begin() as conn:
        res = conn.execute(text(sql), {"cliente_id": cliente_id})
        return (res.rowcount or 0) == 1


def unlock_execucao(cliente_id: str, ok: bool, execution_id: str = "", erro: str = "") -> None:
    """
    Libera o lock e registra status final da execução.
    """
    sql = """
    UPDATE dbo.nps_clientes
    SET
      status_execucao = :status_execucao,
      last_execution_id = NULLIF(LTRIM(RTRIM(:last_execution_id)), ''),
      ultimo_erro = NULLIF(LTRIM(RTRIM(:ultimo_erro)), ''),
      exec_finished_at = SYSUTCDATETIME(),
      updated_at = SYSUTCDATETIME()
    WHERE cliente_id = :cliente_id;
    """

    exec_sql(sql, {
        "cliente_id": cliente_id,
        "status_execucao": "done" if ok else "error",
        "last_execution_id": execution_id or "",
        "ultimo_erro": "" if ok else (erro or "Falha no envio"),
    })

def apply_batch_commands():
    """
    Aplica comandos pendentes antes de instanciar widgets (multiselect/checkbox).
    Evita StreamlitAPIException de session_state.
    """
    cmd = st.session_state.pop("batch_cmd", None)

    if not cmd:
        return

    if cmd.get("type") == "set_selection":
        st.session_state["batch_selected_labels"] = cmd.get("labels", [])
        return

    if cmd.get("type") == "clear_selection":
        st.session_state["batch_selected_labels"] = []
        return

    if cmd.get("type") == "reset_confirm":
        st.session_state["batch_confirm"] = False
        return

def count_respostas_cliente(cliente_id: str) -> int:
    sql = "SELECT COUNT(1) AS n FROM dbo.nps_respostas WHERE cliente_id = :cliente_id;"
    df = read_df(sql, {"cliente_id": cliente_id})
    return int(df.iloc[0]["n"]) if not df.empty else 0


def delete_cliente(cliente_id: str, delete_respostas: bool = False) -> tuple[bool, str]:
    """
    Exclui cliente (e opcionalmente respostas). Retorna (ok, msg).
    Protege contra deletar múltiplas linhas.
    """
    if delete_respostas:
        sql = """
        BEGIN TRANSACTION;

        DELETE FROM dbo.nps_respostas WHERE cliente_id = :cliente_id;

        DELETE FROM dbo.nps_clientes WHERE cliente_id = :cliente_id;

        IF @@ROWCOUNT = 1
            COMMIT;
        ELSE
            ROLLBACK;
        """
    else:
        sql = """
        BEGIN TRANSACTION;

        DELETE FROM dbo.nps_clientes WHERE cliente_id = :cliente_id;

        IF @@ROWCOUNT = 1
            COMMIT;
        ELSE
            ROLLBACK;
        """

    exec_sql(sql, {"cliente_id": cliente_id})
    return True, "Exclusão concluída ✅"

st.set_page_config(page_title="Clientes", layout="wide")
st.title("👥 Clientes")

# Top bar (UX)
left, right = st.columns([3,1])
with right:
    if st.button("🏠 Home", use_container_width=True, key="cli_home"):
        st.switch_page("Home.py")

st.divider()

with st.sidebar:

    st.header("Filtros")
    sort = st.selectbox("Ordenar por", ["Mais recentes", "Mais respostas (cliente)", "Mais respostas (empresa)"])
    q = st.text_input("Busca", "", key="cli_q")
    ativo = st.selectbox("Status", ["Ativos", "Todos", "Inativos"], index=0, key="cli_ativo")
    perfil = st.selectbox("Perfil", ["Todos", "Decisor", "Influenciador"], index=0, key="cli_perfil")
    topn = st.slider("Limite", 50, 2000, 200, step=50, key="cli_topn")
    st.caption("Dica: use a busca para achar por nome/email/empresa/id.")

df = load_clientes(q, ativo, perfil, int(topn))
df["respostas_cliente"] = df["respostas_cliente"].fillna(0).astype(int)
df["respostas_empresa"] = df["respostas_empresa"].fillna(0).astype(int)

# ✅ garanta “atualização real” + deduplicação ANTES do indicador
# (mantém o registro mais recente por cliente_id)
if "updated_at" in df.columns:
    df = df.sort_values("updated_at", ascending=False)

df = df.drop_duplicates(subset=["cliente_id"], keep="first").copy()

def indicador_envio_row(row):

    ativo_val = row.get("ativo")
    ativo = 0 if ativo_val is None or (isinstance(ativo_val, float) and pd.isna(ativo_val)) else int(ativo_val)

    status_val = row.get("status_envio")
    status = "" if status_val is None or (isinstance(status_val, float) and pd.isna(status_val)) else str(status_val).strip()

    exec_val = row.get("status_execucao")
    execucao = "" if exec_val is None else str(exec_val).strip()

    erro_val = row.get("ultimo_erro")
    ultimo_erro = "" if erro_val is None else str(erro_val).strip()


    # Inativo
    if ativo == 0:
        return "⛔ Inativo"


    # Executando (n8n rodando)
    if execucao in ("running","executando","processing"):
        return "🔄 Enviando"


    # Forçado elegível ou pendente
    if status in ("Pendente","Elegível",""):
        return "🟢 Elegível"


    # Falha
    if status == "Falha" or ultimo_erro:
        return "❌ Falha"


    # Enviado
    if status == "Enviado":
        return "✅ Enviado"


    return "—"

# ✅ garante 1 coluna (Series) SEM chance de virar DataFrame
tmp = df.apply(indicador_envio_row, axis=1)

if isinstance(tmp, pd.DataFrame):
    st.error("indicador_envio_row retornou múltiplos campos em alguma linha.")
    st.write("Colunas geradas:", list(tmp.columns))
    st.dataframe(tmp.head(), use_container_width=True)
    tmp = tmp.astype(str).agg(" | ".join, axis=1)

df["indicador_envio"] = tmp.astype(str)

# -------- Indicadores topo --------

total_resp = int(df["respostas_cliente"].sum()) if "respostas_cliente" in df else 0
media_resp = round(total_resp / max(len(df), 1), 2)
ativos = int((df["ativo"] == 1).sum()) if "ativo" in df else 0
inativos = int((df["ativo"] == 0).sum()) if "ativo" in df else 0

k1, k2, k3, k4 = st.columns(4)

k1.metric("Clientes na lista", len(df))
k2.metric("Respostas (lista)", total_resp)
k3.metric("Média / cliente", media_resp)
k4.metric("Ativos", ativos)

st.divider()

st.subheader("Lista")
cols = [
  "cliente_id","nome","empresa","perfil_badge",
  "status_envio","ultimo_envio","proximo_envio",   # 👈 add
  "indicador_envio","updated_at",
  "respostas_empresa","respostas_cliente"
]

cols = [c for c in cols if c in df.columns]

def perfil_badge(p):
    return "👤 Decisor" if p == "Decisor" else ("🧩 Influenciador" if p == "Influenciador" else "—")

df["perfil_badge"] = df["perfil_decisor"].apply(perfil_badge) if "perfil_decisor" in df.columns else "—"

st.dataframe(df[cols], width="stretch", hide_index=True)

st.divider()

# Ações “app-like”
st.subheader("Ações")
selected = None

if not df.empty:
    options = {
        f"{row['nome']} • {row['empresa'] or '-'} • {row['cliente_id']}": row['cliente_id']
        for _, row in df.iterrows()
    }

    selected_label = st.selectbox(
        "Selecionar cliente",
        list(options.keys()),
        key="cli_selected"
    )

    selected = options[selected_label]
else:
    st.info("Sem clientes para selecionar com os filtros atuais.")

tab_new, tab_view, tab_edit, tab_actions = st.tabs(
    ["➕ Novo", "🔎 Ver", "✏️ Editar", "⚡ Ações"]
)

with tab_new:
    st.markdown("### Criar cliente")

    with st.form("form_novo_cliente", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        nome = c1.text_input("Nome *", key="cli_new_nome")
        email = c2.text_input("Email *", key="cli_new_email")
        empresa = c3.text_input("Empresa *", key="cli_new_empresa")

        c4, c5 = st.columns(2)
        perfil_decisor = c4.selectbox("Perfil Decisor", PERFIS, index=0, key="cli_new_perfil")
        segmento = c5.text_input("Segmento", key="cli_new_segmento")

        preview_id = make_cliente_id(email, empresa) if email.strip() and empresa.strip() else ""
        st.text_input("cliente_id (gerado)", value=preview_id, disabled=True)

        submitted = st.form_submit_button("Cadastrar", type="primary")

    if submitted:
        # validação mínima
        if not nome.strip() or not email.strip() or not empresa.strip():
            st.warning("Preencha Nome, Email e Empresa.")
        else:
            try:
                new_id = insert_cliente(nome, email, empresa, perfil_decisor, segmento)

                # 🔥 feedback visual forte
                st.success("✅ Cliente cadastrado com sucesso!")
                st.toast(f"Cliente criado: {new_id}", icon="✅")
                st.info(f"Cadastro concluído às {time.strftime('%H:%M:%S')}")

                # Atualiza lista / tela
                time.sleep(0.6)
                st.rerun()

            except IntegrityError as e:
                if "2627" in str(e) or "2601" in str(e):
                    st.warning(
                        "⚠️ Já existe um cliente cadastrado com este **email e empresa**.\n\n"
                        "Use a busca para localizá-lo e edite o cadastro existente."
                    )
                else:
                    st.error(f"❌ Não foi possível concluir o cadastro: {e}")
            except Exception as e:
                st.error(f"❌ Erro ao cadastrar: {e}")

with tab_view:
    st.markdown("### Visualizar")
    if not selected:
        st.info("Selecione um cliente na lista.")
    else:
        item = load_one(selected)
        if not item:
            st.error("Não encontrado.")
        else:
            if item.get("ativo") == 1:
                st.success("ATIVO")
            else:
                st.warning("INATIVO")
            st.json(item)

with tab_edit:
    st.markdown("### Editar")

    if not selected:
        st.info("Selecione um cliente na lista.")
    else:
        item = load_one(selected)
        if not item:
            st.error("Não encontrado.")
        else:
            # ✅ Re-hidrata os campos do formulário quando trocar o cliente selecionado
            if st.session_state.get("cli_edit_last_id") != selected:
                st.session_state["cli_edit_last_id"] = selected
                st.session_state["cli_edit_nome"] = item.get("nome") or ""
                st.session_state["cli_edit_email"] = item.get("email") or ""
                st.session_state["cli_edit_empresa"] = item.get("empresa") or ""
                st.session_state["cli_edit_perfil"] = (
                    item.get("perfil_decisor") if item.get("perfil_decisor") in PERFIS else PERFIS[0]
                )
                st.session_state["cli_edit_segmento"] = item.get("segmento") or ""

            with st.form("form_editar_cliente"):
                c1, c2, c3 = st.columns(3)
                nome_e = c1.text_input("Nome", key="cli_edit_nome")
                email_e = c2.text_input("Email", key="cli_edit_email")
                empresa_e = c3.text_input("Empresa", key="cli_edit_empresa")

                c4, c5 = st.columns(2)
                perfil_e = c4.selectbox("Perfil Decisor", PERFIS, key="cli_edit_perfil")
                segmento_e = c5.text_input("Segmento", key="cli_edit_segmento")

                st.caption("⚠️ O ID não muda. Se email/empresa mudar, o hash teórico mudaria — por enquanto mantemos o ID fixo (POC).")

                submitted = st.form_submit_button("Salvar alterações", type="primary")

            if submitted:
                try:
                    if not (nome_e or "").strip() or not (email_e or "").strip() or not (empresa_e or "").strip():
                        st.warning("Preencha Nome, Email e Empresa.")
                    else:
                        update_cliente(selected, nome_e, email_e, empresa_e, perfil_e, segmento_e)

                        # 🔥 feedback visual forte
                        st.success("✅ Cliente atualizado com sucesso!")
                        st.toast("Alterações salvas.", icon="💾")

                        # opcional: mostrar horário da atualização
                        st.info(f"Atualizado em {time.strftime('%H:%M:%S')}")

                        # força reload limpo
                        st.session_state.pop("cli_edit_last_id", None)

                        time.sleep(0.6)  # pequena pausa para o usuário ver o feedback
                        st.rerun()

                except IntegrityError as e:
                    if "2627" in str(e) or "2601" in str(e):
                        st.warning("⚠️ Já existe um cliente com esse Email + Empresa.")
                    else:
                        st.error(f"Erro de integridade: {e}")
                except Exception as e:
                    st.error(f"Erro ao atualizar: {e}")

def flash(kind: str, msg: str, debug: dict | None = None):
    st.session_state["flash_kind"] = kind  # "success" | "error" | "warning" | "info"
    st.session_state["flash_msg"] = msg
    st.session_state["flash_debug"] = debug
    st.session_state["flash_ts"] = time.time()
    st.session_state["refresh_after_action"] = True

def show_flash():
    kind = st.session_state.get("flash_kind")
    msg = st.session_state.get("flash_msg")
    debug = st.session_state.get("flash_debug")

    if kind and msg:
        if kind == "success":
            st.success(msg)
        elif kind == "error":
            st.error(msg)
        elif kind == "warning":
            st.warning(msg)
        else:
            st.info(msg)

        if debug:
            with st.expander("Ver detalhes do disparo (debug)"):
                st.json(debug)

        # limpa para não repetir sempre
        st.session_state.pop("flash_kind", None)
        st.session_state.pop("flash_msg", None)
        st.session_state.pop("flash_debug", None)

# --- dentro do tab_actions ---
with tab_actions:
    disabled_actions = not bool(selected)

    # Linha 1: ações rápidas
    b1, b2 = st.columns(2)
    with b1:
        if st.button("⚡ Forçar elegível", use_container_width=True, key="btn_forcar_elegivel", disabled=disabled_actions):
            try:
                forcar_elegivel(selected)
                flash("success", "Cliente marcado como elegível para o próximo ciclo ✅")
            except Exception as e:
                flash("error", f"Erro ao forçar elegível: {e}")

    with b2:
        if st.button("🚀 Forçar envio agora", type="primary", use_container_width=True, key="btn_forcar_envio", disabled=disabled_actions):
            try:
                forcar_envio(selected)

                with st.spinner("Disparando envio no n8n..."):
                    ok, msg, details = disparar_n8n_force(selected)

                if not ok:
                    flash("error", msg, details)
                else:
                    with st.spinner("Aguardando confirmação no SQL (status de envio)..."):
                        done, snap = wait_status_update(selected, timeout_s=60, every_s=1)

                    if done:
                        st_txt = snap.get("status_envio")
                        if st_txt == "Enviado":
                            flash("success", f"Envio concluído ✅ (status: {st_txt})", {**details, **snap})
                        elif st_txt == "Erro":
                            flash("error", "Envio concluiu com ERRO ⚠️", {**details, **snap})
                        else:
                            flash("success", f"Envio confirmado ✅ (status: {st_txt})", {**details, **snap})
                    else:
                        flash("warning", "Fluxo iniciou no n8n, mas ainda não confirmei no SQL (aguarde e atualize).", {**details, **snap})

            except Exception as e:
                flash("error", f"Erro ao forçar envio: {e}")

    st.divider()

    # Linha 2: ações de estado / destrutivas
    c1, c2 = st.columns(2)

    with c1:
        disabled_actions = not bool(selected)

        item_act = load_one(selected) if selected else None
        ativo_now = int(item_act.get("ativo", 0)) if item_act else 0  # 1=ativo, 0=inativo

        st.markdown("#### 📴 Ativar / Inativar cliente")
        st.caption("Alterna o status do cliente (não apaga do banco).")

        if disabled_actions:
            st.info("Selecione um cliente para habilitar as ações.")
        else:
            if ativo_now == 1:
                st.write("Status atual: **🟢 Ativo**")
                confirm = st.checkbox("Confirmo inativação", key="cli_act_toggle_confirm_inativar")
                if st.button(
                    "Inativar agora",
                    use_container_width=True,
                    key="cli_act_toggle_inativar_btn",
                    disabled=not confirm
                ):
                    try:
                        set_ativo(selected, 0)
                        st.toast("Cliente inativado.", icon="🛑")
                        flash("success", "Cliente inativado ✅")
                    except Exception as e:
                        flash("error", f"Erro ao inativar: {e}")

            else:
                st.write("Status atual: **⚫ Inativo**")
                confirm = st.checkbox("Confirmo reativação", key="cli_act_toggle_confirm_ativar")
                if st.button(
                    "Ativar (Reativar) agora",
                    type="primary",
                    use_container_width=True,
                    key="cli_act_toggle_ativar_btn",
                    disabled=not confirm
                ):
                    try:
                        set_ativo(selected, 1)
                        st.toast("Cliente reativado.", icon="✅")
                        flash("success", "Cliente reativado ✅")
                    except Exception as e:
                        flash("error", f"Erro ao reativar: {e}")

    with c2:
        st.markdown("#### 🗑️ Excluir cliente")
        st.caption("⚠️ Exclusão física. Use com cuidado.")
        if not disabled_actions:
            n_resp = count_respostas_cliente(selected)
            st.write(f"Respostas vinculadas a este cliente: **{n_resp}**")

        delete_respostas = st.checkbox("Também excluir respostas vinculadas", key="cli_act_del_respostas", disabled=disabled_actions)
        st.warning("Esta ação é irreversível.")

        confirm_del_1 = st.checkbox("Eu entendo que é irreversível", key="cli_act_del_confirm1", disabled=disabled_actions)
        confirm_del_2 = st.text_input("Digite o cliente_id para confirmar", value="", key="cli_act_del_typed", disabled=disabled_actions)

        can_delete = (not disabled_actions) and confirm_del_1 and (confirm_del_2.strip() == (selected or ""))

        if st.button("Excluir definitivamente", use_container_width=True, key="cli_act_del_btn", disabled=not can_delete):
            try:
                ok, msg = delete_cliente(selected, delete_respostas=delete_respostas)
                st.toast("Cliente excluído definitivamente.", icon="🗑️")
                flash("success", msg)
                st.session_state.pop("cli_selected", None)
            except Exception as e:
                flash("error", f"Erro ao excluir: {e}")

    st.divider()

    st.markdown("### 📦 Envio em lote (forçado)")
    st.caption("Força o envio: marca Pendente e zera ultimo_envio antes de disparar o n8n.")

    df_lote = df.copy()
    need_cols = ["cliente_id","nome","empresa","email","ativo","status_envio","proximo_envio","status_execucao"]
    for c in need_cols:
        if c not in df_lote.columns:
            df_lote[c] = None

    max_lote = st.slider("Tamanho do lote (máx)", 1, 300, 50, step=10, key="batch_max")
    df_lote = df_lote.head(int(max_lote))

    options_batch = {
        f"{r['nome']} • {r['empresa'] or '-'} • {r['cliente_id']}": r["cliente_id"]
        for _, r in df_lote.iterrows()
    }

    # ✅ aplica comandos antes de criar widgets
    apply_batch_commands()

    selected_batch_labels = st.multiselect(
        "Selecionar clientes para envio",
        list(options_batch.keys()),
        default=st.session_state.get("batch_selected_labels", []),
        key="batch_selected_labels",
    )

    selected_batch_ids = [options_batch[lbl] for lbl in selected_batch_labels if lbl in options_batch]

    colA, colB, colC, colD = st.columns([1,1,1,2])

    with colA:
        if st.button("✅ Selecionar elegíveis", use_container_width=True, key="batch_sel_eleg"):
            elegiveis_labels = []
            for _, r in df_lote.iterrows():
                if is_elegivel_row(r.to_dict()):
                    lbl = f"{r['nome']} • {r['empresa'] or '-'} • {r['cliente_id']}"
                    if lbl in options_batch:
                        elegiveis_labels.append(lbl)

            st.session_state["batch_cmd"] = {"type": "set_selection", "labels": elegiveis_labels}
            st.rerun()

    with colB:
        if st.button("📋 Selecionar todos", use_container_width=True, key="batch_sel_all"):
            st.session_state["batch_cmd"] = {"type": "set_selection", "labels": list(options_batch.keys())}
            st.rerun()

    with colC:
        if st.button("🧹 Limpar", use_container_width=True, key="batch_sel_clear"):
            st.session_state["batch_cmd"] = {"type": "clear_selection"}
            st.rerun()

    with colD:
        st.info(f"Selecionados: {len(selected_batch_ids)} (de {len(df_lote)})")

    confirm_lote = st.checkbox(
        "Confirmo que quero disparar o envio em lote (ação forçada).",
        key="batch_confirm",
    )

    run_batch = st.button(
        "🚀 Disparar lote no n8n (forçado)",
        type="primary",
        use_container_width=True,
        disabled=(len(selected_batch_ids) == 0 or not confirm_lote),
    )

    if run_batch:
        results = []
        total = len(selected_batch_ids)

        progress = st.progress(0)
        status_box = st.empty()

        for i, cid in enumerate(selected_batch_ids, start=1):
            status_box.write(f"Enviando {i}/{total} — {cid}")

            locked = try_lock_execucao(cid)
            if not locked:
                results.append({"cliente_id": cid, "ok": False, "msg": "Ignorado: já está em execução (running)", "executionId": ""})
                progress.progress(int(i/total*100))
                continue

            try:
                forcar_envio(cid)
                ok, msg, details = disparar_n8n_force(cid)

                exec_id = ""
                if isinstance(details, dict):
                    data = details.get("data") if isinstance(details.get("data"), dict) else {}
                    exec_id = str(data.get("executionId") or details.get("executionId") or "")

                if ok:
                    unlock_execucao(cid, ok=True, execution_id=exec_id, erro="")
                    results.append({"cliente_id": cid, "ok": True, "msg": msg, "executionId": exec_id})
                else:
                    unlock_execucao(cid, ok=False, execution_id=exec_id, erro=msg)
                    results.append({"cliente_id": cid, "ok": False, "msg": msg, "executionId": exec_id})

            except Exception as e:
                unlock_execucao(cid, ok=False, execution_id="", erro=str(e))
                results.append({"cliente_id": cid, "ok": False, "msg": f"Exception: {e}", "executionId": ""})

            progress.progress(int(i/total*100))
            time.sleep(0.2)

        status_box.empty()

        df_res = pd.DataFrame(results)
        ok_count = int(df_res["ok"].sum()) if not df_res.empty else 0
        fail_count = total - ok_count

        st.success(f"Lote concluído ✅ Sucesso: {ok_count} | Falhas: {fail_count}")
        st.dataframe(df_res, use_container_width=True, hide_index=True)

        # ✅ pede reset (sem tocar direto no checkbox neste run)
        st.session_state["batch_cmd"] = {"type": "reset_confirm"}
        #st.rerun()

    # seu refresh padrão
    if st.session_state.get("refresh_after_action"):
        st.session_state["refresh_after_action"] = False
        time.sleep(0.15)
        st.rerun()
show_flash()