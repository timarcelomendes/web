import os, re, hashlib
import streamlit as st
import pyodbc
import pandas as pd
from dotenv import load_dotenv
from datetime import date
import requests
from sqlalchemy import create_engine
import urllib
from sqlalchemy import text
from functools import lru_cache
import time
import requests
from sqlalchemy.exc import IntegrityError
from ui import sidebar_logo

sidebar_logo()
load_dotenv()

PERFIS = ["Decisor", "Influenciador"]

@lru_cache
def get_engine():
    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{os.environ.get('SQL_SERVER','')},1433;"
        f"Database={os.environ.get('SQL_DB','')};"
        f"Uid={os.environ.get('SQL_USER','')};"
        f"Pwd={os.environ.get('SQL_PASSWORD','')};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
        "MARS_Connection=yes;"
        "Connection Timeout=30;"
    )

    params = urllib.parse.quote_plus(conn_str)

    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        pool_pre_ping=True,
        pool_recycle=1800
    )

N8N_FORCE_URL = os.environ.get(
    "N8N_FORCE_URL",
    "https://sai-marketing.app.n8n.cloud/webhook/10e983d1-9edb-459e-b6af-dc8950a8172b"
).strip()

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
    params = {}   # <-- dict, sem topn

    if ativo == "Ativos":
        where.append("c.ativo = 1")
    elif ativo == "Inativos":
        where.append("c.ativo = 0")

    if perfil in PERFIS:
        where.append("c.perfil_decisor = :perfil")
        params["perfil"] = perfil

    if q.strip():
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
        c.cliente_id, c.nome, c.email, c.empresa, c.perfil_decisor, c.segmento,
        c.status_envio, c.ultimo_erro, c.ultimo_envio, c.proximo_envio, c.ativo, c.updated_at,
        c.status_execucao, c.last_execution_id, c.exec_started_at, c.exec_finished_at,
        COALESCE(rc.respostas_cliente, 0) AS respostas_cliente,
        COALESCE(re.respostas_empresa, 0) AS respostas_empresa
    FROM dbo.nps_clientes c
    OUTER APPLY (
        SELECT COUNT(1) AS respostas_cliente
        FROM dbo.nps_respostas r
        WHERE r.cliente_id = c.cliente_id
    ) rc
    OUTER APPLY (
        SELECT COUNT(1) AS respostas_empresa
        FROM dbo.nps_respostas r2
        WHERE r2.empresa = c.empresa
    ) re
    {where_sql}
    ORDER BY c.updated_at DESC;
    """

    engine = get_engine()

    # ✅ Só passa params se tiver algum
    df = pd.read_sql(sql, engine, params=params if params else None)
    return df

    dups = df.columns[df.columns.duplicated()].tolist()
    st.write("Colunas duplicadas:", dups)
    st.write("Total colunas:", len(df.columns), "Únicas:", len(set(df.columns)))

def wait_status_update(cliente_id: str, timeout_s: int = 15, every_s: float = 1.0):
    """
    Aguarda o status_envio mudar após disparar o n8n.
    Retorna (ok, snapshot_dict).
    """
    t0 = time.time()
    today = date.today().isoformat()

    while (time.time() - t0) < timeout_s:
        row = load_one(cliente_id)  # usa sua função (via read_df)
        if not row:
            return False, {"stage": "not_found"}

        status = (row.get("status_envio") or "").strip()
        ultimo = row.get("ultimo_envio")
        ultimo_s = str(ultimo)[:10] if ultimo else ""

        # ✅ Concluiu se mudou para Enviado/Erro/Respondido
        if status in ("Enviado", "Erro", "Respondido"):
            return True, {"stage": "done", "status_envio": status, "ultimo_envio": ultimo_s, "ultimo_erro": row.get("ultimo_erro")}

        # ✅ Ou (opcional) se marcou ultimo_envio = hoje
        if ultimo_s == today:
            return True, {"stage": "done_by_date", "status_envio": status, "ultimo_envio": ultimo_s}

        time.sleep(every_s)

    return False, {"stage": "timeout_waiting_sql"}

def forcar_elegivel(cliente_id: str):
    sql = """
    UPDATE dbo.nps_clientes
    SET
      ativo = 1,
      status_envio = 'Pendente',
      -- não zera ultimo_envio aqui (vai respeitar regra do dia se você tiver)
      proximo_envio = CAST(GETDATE() AS DATE),
      ultimo_erro = NULL,
      updated_at = SYSUTCDATETIME()
    WHERE cliente_id = :cliente_id;
    """
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(sql), {"cliente_id": cliente_id})

def forcar_envio(cliente_id: str):
    sql = """
    UPDATE dbo.nps_clientes
    SET
      ativo = 1,
      status_envio = 'Pendente',
      ultimo_envio = NULL,  -- ✅ força reenviar hoje
      proximo_envio = CAST(GETDATE() AS DATE),
      ultimo_erro = NULL,
      updated_at = SYSUTCDATETIME()
    WHERE cliente_id = :cliente_id;
    """
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(sql), {"cliente_id": cliente_id})

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

def update_cliente(cliente_id, nome, email, empresa, perfil_decisor, segmento):
    sql = """
    UPDATE dbo.nps_clientes
    SET
      nome = :nome,
      email = :email,
      empresa = :empresa,
      perfil_decisor = :perfil_decisor,
      segmento = :segmento,
      updated_at = SYSUTCDATETIME()
    WHERE cliente_id = :cliente_id;
    """

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(sql),
            {
                "cliente_id": cliente_id,
                "nome": (nome or "").strip() or None,
                "email": (email or "").strip() or None,
                "empresa": (empresa or "").strip() or None,
                "perfil_decisor": perfil_decisor,
                "segmento": (segmento or "").strip() or None,
            }
        )

def set_ativo(cliente_id, ativo: int):
    sql = """
    UPDATE dbo.nps_clientes
    SET
      ativo = :ativo,
      updated_at = SYSUTCDATETIME()
    WHERE cliente_id = :cliente_id;
    """

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(sql),
            {
                "cliente_id": cliente_id,
                "ativo": int(ativo),
            }
        )

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
    hoje = date.today()

    ativo = int(row.get("ativo") or 0)
    status = (row.get("status_envio") or "").strip()
    exec_status = (row.get("status_execucao") or "").strip()
    exec_id = (row.get("last_execution_id") or "")
    ultimo_erro = (row.get("ultimo_erro") or "").strip()

    proximo = row.get("proximo_envio")
    ultimo = row.get("ultimo_envio")

    def to_date(v):
        if v is None or v == "" or (isinstance(v, float) and pd.isna(v)):
            return None
        try:
            return pd.to_datetime(v).date()
        except Exception:
            return None

    proximo_d = to_date(proximo)
    ultimo_d = to_date(ultimo)

    # Inativo
    if ativo != 1:
        return "⚪ Inativo"

    # Execução em andamento (prioridade máxima)
    if exec_status.lower() == "processando":
        return f"🚀 Processando" + (f" (Exec: {exec_id})" if exec_id else "")

    # Execução com erro (mostra erro se tiver)
    if exec_status.lower() == "erro":
        return "🔴 Erro Execução" + (f" • {ultimo_erro[:60]}" if ultimo_erro else "")

    # Enviado hoje
    if status.lower() == "enviado" and ultimo_d == hoje:
        return "📨 Enviado hoje"

    # Elegível
    if status in ("Pendente", "Erro") and (proximo_d is None or proximo_d <= hoje):
        return "🟢 Elegível"

    # Aguardando (próximo ciclo)
    if proximo_d and proximo_d > hoje:
        dias = (proximo_d - hoje).days
        return f"🟡 Próximo ciclo em {dias}d"

    return "🟡 Aguardando"

# ✅ garante 1 coluna (Series) SEM chance de virar DataFrame
df["indicador_envio"] = df.apply(indicador_envio_row, axis=1).astype(str)

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

tab_new, tab_view, tab_edit, tab_state, tab_actions = st.tabs(
    ["➕ Novo", "🔎 Ver", "✏️ Editar", "🗑️ Ativar/Inativar", "⚡ Ações"]
)

with tab_new:
    st.markdown("### Criar cliente")

    c1, c2, c3 = st.columns(3)
    nome = c1.text_input("Nome *", key="cli_new_nome")
    email = c2.text_input("Email *", key="cli_new_email")
    empresa = c3.text_input("Empresa *", key="cli_new_empresa")

    c4, c5 = st.columns(2)
    perfil_decisor = c4.selectbox("Perfil Decisor", PERFIS, index=0, key="cli_new_perfil")
    segmento = c5.text_input("Segmento", key="cli_new_segmento")

    preview_id = make_cliente_id(email, empresa) if email.strip() and empresa.strip() else ""
    st.text_input("cliente_id (gerado)", value=preview_id, disabled=True)

    # Etapa 1 — clicar salvar abre confirmação
    if st.button("Salvar", type="primary", key="cli_new_save"):
        if not nome.strip() or not email.strip() or not empresa.strip():
            st.error("Preencha Nome, Email e Empresa.")
        else:
            st.session_state["confirm_insert"] = True

    # Etapa 2 — confirmação
    if st.session_state.get("confirm_insert"):
        st.warning("Confirma a criação deste cliente?")
        st.write(
            f"""
            **Nome:** {nome}  
            **Email:** {email}  
            **Empresa:** {empresa}  
            **Perfil:** {perfil_decisor}  
            **Segmento:** {segmento or "-"}  
            **ID:** `{preview_id}`
            """
        )

        col_ok, col_cancel = st.columns(2)

        with col_ok:
            if st.button("✅ Confirmar", key="cli_new_confirm"):
                try:
                    new_id = insert_cliente(nome, email, empresa, perfil_decisor, segmento)

                    st.success(f"Cliente criado com sucesso ✅ ({new_id})")
                    st.toast("Cadastro concluído com sucesso.", icon="✅")

                    st.session_state["confirm_insert"] = False
                    st.rerun()

                except IntegrityError as e:
                    if "2627" in str(e) or "2601" in str(e):
                        st.warning(
                            "⚠️ Já existe um cliente cadastrado com este **email e empresa**.\n\n"
                            "Use a busca para localizá-lo e edite o cadastro existente."
                        )
                    else:
                        st.error("❌ Não foi possível concluir o cadastro.")

                    st.session_state["confirm_insert"] = False

        with col_cancel:
            if st.button("↩️ Cancelar", key="cli_new_cancel"):
                st.session_state["confirm_insert"] = False
                st.info("Criação cancelada.")

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
            c1, c2, c3 = st.columns(3)
            nome_e = c1.text_input("Nome", value=item.get("nome") or "", key="cli_edit_nome")
            email_e = c2.text_input("Email", value=item.get("email") or "", key="cli_edit_email")
            empresa_e = c3.text_input("Empresa", value=item.get("empresa") or "", key="cli_edit_empresa")

            c4, c5 = st.columns(2)
            current = item.get("perfil_decisor")
            idx = PERFIS.index(current) if current in PERFIS else 0
            perfil_e = c4.selectbox("Perfil Decisor", PERFIS, index=idx, key="cli_edit_perfil")
            segmento_e = c5.text_input("Segmento", value=item.get("segmento") or "", key="cli_edit_segmento")

            st.caption("⚠️ O ID não muda. Se email/empresa mudar, o hash teórico mudaria — por enquanto mantemos o ID fixo (POC).")

            if st.button("Salvar alterações", type="primary", key="cli_edit_save"):
                try:
                    update_cliente(selected, nome_e, email_e, empresa_e, perfil_e, segmento_e)
                    st.success("Atualizado ✅")
                    st.toast("Alterações salvas.", icon="✅")
                    st.rerun()
                except Exception as e:
                    st.error(f"Erro ao atualizar: {e}")

with tab_state:
    st.markdown("### Ativar/Inativar")
    if not selected:
        st.info("Selecione um cliente na lista.")
    else:
        item = load_one(selected)
        if not item:
            st.error("Não encontrado.")
        else:
            if item.get("ativo") == 1:
                st.warning("Isso não apaga do banco. Apenas marca como INATIVO.")
                confirm = st.checkbox("Confirmo inativação", key="cli_inativar_confirm")
                if st.button("Inativar", disabled=not confirm, key="cli_inativar_btn"):
                    try:
                        set_ativo(selected, 0)
                        st.success("Inativado ✅")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erro: {e}")
            else:
                if st.button("Reativar", key="cli_reativar_btn"):
                    try:
                        set_ativo(selected, 1)
                        st.success("Reativado ✅")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erro: {e}")

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
    st.markdown("### ⚡ Ações")

    # 1) sempre mostrar feedback pendente (se houver)
    show_flash()

    # 2) botões na mesma linha (lado a lado)
    b1, b2 = st.columns(2)

    with b1:
        if st.button("⚡ Forçar elegível", use_container_width=True, key="btn_forcar_elegivel"):
            try:
                forcar_elegivel(selected)
                flash("success", "Cliente marcado como elegível para o próximo ciclo ✅")
            except Exception as e:
                flash("error", f"Erro ao forçar elegível: {e}")

    with b2:
        if st.button("🚀 Forçar envio agora", type="primary", use_container_width=True, key="btn_forcar_envio"):
            try:
                forcar_envio(selected)

                with st.spinner("Disparando envio no n8n..."):
                    ok, msg, details = disparar_n8n_force(selected)

                if not ok:
                    flash("error", msg, details)
                else:
                    # ✅ AGUARDA conclusão no SQL
                    with st.spinner("Aguardando confirmação no SQL (status de envio)..."):
                        done, snap = wait_status_update(selected, timeout_s=40, every_s=1)

                    if done:
                        st_txt = snap.get("status_envio")
                        if st_txt == "Enviado":
                            flash("success", f"Envio concluído ✅ (status: {st_txt})", {**details, **snap})
                        elif st_txt == "Erro":
                            flash("error", f"Envio concluiu com ERRO ⚠️", {**details, **snap})
                        else:
                            flash("success", f"Envio confirmado ✅ (status: {st_txt})", {**details, **snap})
                    else:
                        # não confirmou a tempo, mas o fluxo iniciou
                        flash("warning", "Fluxo iniciou no n8n, mas ainda não confirmei o status no SQL (aguarde alguns segundos e atualize).", {**details, **snap})

            except Exception as e:
                flash("error", f"Erro ao forçar envio: {e}")

    # 3) depois da ação, atualiza a tela (mas sem perder o feedback)
    if st.session_state.get("refresh_after_action"):
        st.session_state["refresh_after_action"] = False
        # opcional: dá um tempinho pro usuário ver o "toast"/mudança
        time.sleep(0.15)
        st.rerun()