import time
import subprocess
import urllib.parse

import streamlit as st
import requests
from sqlalchemy import create_engine, text

from db import get_engine

st.set_page_config(page_title="Status de Conexão", layout="wide")
st.title("🔂 Status de Conexão")

DRIVERS_TO_TEST = [
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
]

# Opcional (secrets.toml)
N8N_FORCE_URL = (st.secrets.get("N8N_FORCE_URL") or "").strip()
ENV_NAME = (st.secrets.get("ENV") or st.secrets.get("AMBIENTE") or "local").strip() or "local"


@st.cache_data(ttl=60)
def ping_db_ms() -> tuple[bool, int | None, str | None]:
    """Ping simples do DB para cards do topo."""
    try:
        engine = get_engine()
        t0 = time.time()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        ms = int((time.time() - t0) * 1000)
        return True, ms, None
    except Exception as e:
        return False, None, str(e)[:1500]


@st.cache_data(ttl=300)
def sh(cmd: str) -> str:
    # cacheia por 5 min para não rodar a cada rerun
    return subprocess.getoutput(cmd)


def classify_error(msg: str) -> str:
    m = (msg or "").lower()
    if "can't open lib" in m or "file not found" in m or "data source name not found" in m or "im002" in m:
        return "driver_missing"
    if "is not allowed to access the server" in m or "(40615)" in m:
        return "firewall"
    if "login failed" in m or "(18456)" in m:
        return "auth"
    if "cannot open database" in m:
        return "database"
    if "timeout" in m or "hyt00" in m:
        return "timeout"
    return "other"


def hint_for(kind: str) -> str | None:
    return {
        "driver_missing": "Driver ODBC não está instalado/registrado no ambiente.",
        "firewall": "Firewall do Azure SQL bloqueando o IP do Streamlit Cloud. Adicione o IP nas regras do servidor.",
        "auth": "Usuário/senha inválidos ou sem permissão no Azure SQL.",
        "database": "Nome do banco incorreto ou usuário sem acesso ao DB.",
        "timeout": "Timeout (rede/firewall/DNS).",
    }.get(kind)


def build_conn_str(driver: str, server: str, database: str, username: str, password: str) -> str:
    return (
        f"Driver={{{driver}}};"
        f"Server=tcp:{server},1433;"
        f"Database={database};"
        f"Uid={username};"
        f"Pwd={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
        "MARS_Connection=yes;"
        "Connection Timeout=30;"
    )


def try_connect_with_driver(driver_name: str):
    server = st.secrets.get("SQL_SERVER")
    database = st.secrets.get("SQL_DB")
    username = st.secrets.get("SQL_USER")
    password = st.secrets.get("SQL_PASSWORD")

    if not all([server, database, username, password]):
        return False, None, "Secrets incompletos (SQL_SERVER/SQL_DB/SQL_USER/SQL_PASSWORD)."

    conn_str = build_conn_str(driver_name, server, database, username, password)

    try:
        params = urllib.parse.quote_plus(conn_str)
        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={params}",
            pool_pre_ping=True,
            future=True,
        )
        t0 = time.time()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        ms = int((time.time() - t0) * 1000)
        return True, ms, None
    except Exception as e:
        return False, None, str(e)[:2000]


def test_n8n(url: str, cliente_id: str = "C-TEST") -> tuple[bool, int | None, str, str | None]:
    if not url:
        return False, None, "N8N_FORCE_URL não configurada.", None

    # fallback: webhook-test -> webhook
    urls_to_try = [url]
    if "/webhook-test/" in url:
        urls_to_try.append(url.replace("/webhook-test/", "/webhook/"))

    last_err = ""
    for u in urls_to_try:
        try:
            t0 = time.time()
            resp = requests.post(u, json={"cliente_id": cliente_id}, timeout=20)
            ms = int((time.time() - t0) * 1000)
            if 200 <= resp.status_code < 300:
                return True, ms, f"HTTP {resp.status_code}", u
            last_err = f"HTTP {resp.status_code} | {resp.text[:300]}"
        except requests.exceptions.Timeout:
            last_err = "Timeout (20s)"
        except Exception as e:
            last_err = str(e)

    return False, None, last_err, None


# -------------------- Cards do topo --------------------
ok_db, ms_db, db_err = ping_db_ms()

c1, c2, c3 = st.columns(3)
with c1:
    st.subheader("Banco")
    if ok_db:
        st.success("Online ✅")
    else:
        st.error("Offline ❌")
with c2:
    st.subheader("Latência")
    st.metric("ms", ms_db if ms_db is not None else "—")
with c3:
    st.subheader("Ambiente")
    st.write(ENV_NAME)

if (not ok_db) and db_err:
    with st.expander("Detalhes do erro do Banco"):
        kind = classify_error(db_err)
        h = hint_for(kind)
        if h:
            st.warning(h)
        st.code(db_err, language="text")

st.divider()

# -------------------- Abas --------------------
tab_db, tab_n8n, tab_runtime = st.tabs(["🗄️ Banco / ODBC", "🔁 n8n", "🧰 Runtime"])

with tab_db:
    col_btn1, col_btn2 = st.columns([1, 3])
    run_tests = col_btn1.button("▶️ Rodar testes", use_container_width=True)
    col_btn2.caption("Executa diagnósticos mais pesados (drivers 18/17 e get_engine).")

    with st.expander("Runtime / ODBC", expanded=False):
        st.code(sh("python --version"), language="text")
        st.code(sh("odbcinst -j"), language="text")
        st.code(sh("odbcinst -q -d || true"), language="text")

    with st.expander("Secrets (sem dados sensíveis)", expanded=False):
        st.write("SQL_SERVER:", st.secrets.get("SQL_SERVER", "(vazio)"))
        st.write("SQL_DB:", st.secrets.get("SQL_DB", "(vazio)"))
        st.write("SQL_USER:", st.secrets.get("SQL_USER", "(vazio)"))
        st.write("SQL_PASSWORD:", "✅ definido" if st.secrets.get("SQL_PASSWORD") else "❌ vazio")

    if run_tests:
        st.subheader("Teste por driver (18 / 17)")
        cols = st.columns(2)

        for i, drv in enumerate(DRIVERS_TO_TEST):
            ok, ms, err = try_connect_with_driver(drv)
            with cols[i % 2]:
                st.write(f"**{drv}**")
                if ok:
                    st.success(f"Conexão OK ✅ ({ms} ms)")
                else:
                    st.error("Falhou ❌")
                    kind = classify_error(err or "")
                    h = hint_for(kind)
                    if h:
                        st.warning(h)
                    st.code(err or "", language="text")

        st.divider()
        st.subheader("Teste usando get_engine() (fallback 18 → 17)")

        try:
            engine = get_engine()
            t0 = time.time()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            ms = int((time.time() - t0) * 1000)
            st.success(f"get_engine() conectou ✅ ({ms} ms)")
        except Exception as e:
            err = str(e)[:2000]
            st.error("get_engine() falhou ❌")
            kind = classify_error(err)
            h = hint_for(kind)
            if h:
                st.warning(h)
            st.code(err, language="text")
    else:
        st.info("Clique em **Rodar testes** para executar diagnósticos avançados.")

with tab_n8n:
    st.write("**N8N_FORCE_URL (Original configurada no secrets)**:")
    st.code(N8N_FORCE_URL or "(vazio)", language="text")

    col1, col2 = st.columns([1, 2])
    do_test = col1.button("🧪 Testar n8n", use_container_width=True)
    test_id = col2.text_input("cliente_id de teste", value="C-TEST", key="n8n_test_id")

    if do_test:
        with st.spinner("Testando n8n..."):
            ok, ms, info, working_url = test_n8n(N8N_FORCE_URL, cliente_id=test_id)
        
        if ok:
            st.success(f"n8n OK ✅ ({ms} ms) — {info}")
            st.info(f"🔗 **Webhook válido em uso no disparo:**\n\n`{working_url}`")
        else:
            st.error("n8n falhou ❌")
            st.code(info, language="text")
    else:
        st.info("Clique em **Testar n8n** para validar conectividade e exibir a URL final do webhook ativo.")

with tab_runtime:
    st.code(sh("pwd"), language="text")
    st.code(sh("ls -la"), language="text")
    st.code(sh("env | sort | sed -n '1,120p'"), language="text")
