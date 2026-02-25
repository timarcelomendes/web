import urllib
import streamlit as st
from sqlalchemy import create_engine

@st.cache_resource
def _engine(conn_key: str, odbc_connect: str):
    params = urllib.parse.quote_plus(odbc_connect)
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        pool_pre_ping=True,
        pool_recycle=1800
    )

def get_engine():
    server = st.secrets["SQL_SERVER"]
    database = st.secrets["SQL_DB"]
    username = st.secrets["SQL_USER"]
    password = st.secrets["SQL_PASSWORD"]

    # chave muda se qualquer segredo mudar
    conn_key = f"{server}|{database}|{username}|{hash(password)}"

    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{server},1433;"
        f"Database={database};"
        f"Uid={username};"
        f"Pwd={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
        "MARS_Connection=yes;"
        "Connection Timeout=30;"
    )

    return _engine(conn_key, conn_str)