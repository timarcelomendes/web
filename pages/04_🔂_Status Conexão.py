import time
import subprocess
import urllib.parse

import streamlit as st
from sqlalchemy import create_engine, text

from db import get_engine

st.set_page_config(page_title="Debug DB", layout="wide")
st.title("🧪 Debug DB (ODBC + Azure SQL)")

DRIVERS_TO_TEST = [
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
]


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


with st.expander("1) Runtime / ODBC", expanded=True):
    st.code(sh("python --version"), language="text")
    st.code(sh("odbcinst -j"), language="text")
    st.code(sh("odbcinst -q -d || true"), language="text")

with st.expander("2) Secrets (sem dados sensíveis)", expanded=True):
    st.write("SQL_SERVER:", st.secrets.get("SQL_SERVER", "(vazio)"))
    st.write("SQL_DB:", st.secrets.get("SQL_DB", "(vazio)"))
    st.write("SQL_USER:", st.secrets.get("SQL_USER", "(vazio)"))
    st.write("SQL_PASSWORD:", "✅ definido" if st.secrets.get("SQL_PASSWORD") else "❌ vazio")

st.divider()

col_btn1, col_btn2 = st.columns([1, 3])
run_tests = col_btn1.button("▶️ Rodar testes", use_container_width=True)
col_btn2.caption("Evita rerun pesado. Clique para executar testes de conexão.")

if run_tests:
    st.subheader("3) Teste por driver (18 / 17)")
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
    st.subheader("4) Teste usando get_engine() (fallback 18 → 17)")

    try:
        engine = get_engine()
        t0 = time.time()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        ms = int((time.time() - t0) * 1000)
        st.success(f"get_engine() conectou ✅ ({ms} ms)")
        st.info("Se quiser exibir qual driver foi usado, exponha isso no db.py (ex.: get_engine_info()).")
    except Exception as e:
        err = str(e)[:2000]
        st.error("get_engine() falhou ❌")
        kind = classify_error(err)
        h = hint_for(kind)
        if h:
            st.warning(h)
        st.code(err, language="text")
else:
    st.info("Clique em **Rodar testes** para executar diagnósticos de conexão.")