import streamlit as st
import subprocess
from sqlalchemy import text

from db import get_engine, _build_conn_str  # se não quiser importar _build_conn_str, veja nota abaixo

st.set_page_config(page_title="Debug DB", layout="wide")
st.title("🧪 Debug DB (ODBC + Azure SQL)")

st.subheader("1) Runtime / ODBC")
st.code(subprocess.getoutput("python --version"), language="text")
st.code(subprocess.getoutput("odbcinst -j"), language="text")
st.code(subprocess.getoutput("odbcinst -q -d || true"), language="text")

st.subheader("2) Secrets (sem dados sensíveis)")
st.write("SQL_SERVER:", st.secrets.get("SQL_SERVER", "(vazio)"))
st.write("SQL_DB:", st.secrets.get("SQL_DB", "(vazio)"))
st.write("SQL_USER:", st.secrets.get("SQL_USER", "(vazio)"))
st.write("SQL_PASSWORD:", "✅ definido" if st.secrets.get("SQL_PASSWORD") else "❌ vazio")

st.subheader("3) Teste de conexão + driver efetivo")

def try_connect_with_driver(driver_name: str):
    server = st.secrets["SQL_SERVER"]
    database = st.secrets["SQL_DB"]
    username = st.secrets["SQL_USER"]
    password = st.secrets["SQL_PASSWORD"]

    conn_str = _build_conn_str(driver_name, server, database, username, password)
    # só mostramos o nome do driver (não exibimos conn_str)
    try:
        # usa seu próprio mecanismo de criação de engine (fallback está no get_engine, aqui é teste direto)
        import urllib.parse
        from sqlalchemy import create_engine
        params = urllib.parse.quote_plus(conn_str)
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", pool_pre_ping=True, future=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, None
    except Exception as e:
        return False, str(e)[:2000]

# Mostra qual conectaria
drivers_to_test = [
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
]

cols = st.columns(2)
for i, drv in enumerate(drivers_to_test):
    ok, err = try_connect_with_driver(drv)
    with cols[i % 2]:
        st.write(f"**{drv}**")
        if ok:
            st.success("Conexão OK ✅")
        else:
            st.error("Falhou ❌")
            st.code(err or "", language="text")

st.divider()
st.subheader("4) Conexão usando get_engine() (fallback 18 → 17)")

try:
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    st.success("get_engine() conectou ✅ (fallback funcionando)")
except Exception as e:
    st.error("get_engine() falhou ❌")
    st.code(str(e)[:2000], language="text")