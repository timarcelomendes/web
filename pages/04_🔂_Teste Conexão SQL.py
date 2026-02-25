import os
import urllib
import streamlit as st
from functools import lru_cache
from sqlalchemy import create_engine, text
from ui import sidebar_logo

sidebar_logo()

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
        pool_pre_ping=True,   # evita conexão morta
        pool_recycle=1800     # recicla a cada 30 min
    )


# -------------------------------------------------
# HEALTHCHECK
# -------------------------------------------------

def engine_healthcheck():
    try:
        engine = get_engine()

        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        return True, None

    except Exception as e:
        return False, str(e)


# -------------------------------------------------
# STREAMLIT TESTE
# -------------------------------------------------

st.title("🔌 Teste de Conexão - Azure SQL")

ok, err = engine_healthcheck()

if ok:
    st.success("Conectado com sucesso ao Azure SQL ✅")
else:
    st.error("Falha na conexão ❌")
    st.code(err)