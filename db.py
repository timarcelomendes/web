import urllib.parse
import streamlit as st
from sqlalchemy import create_engine


@st.cache_resource
def _engine(conn_key: str, odbc_connect: str):
    params = urllib.parse.quote_plus(odbc_connect)
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        pool_pre_ping=True,
        pool_recycle=1800,
    )


def _build_conn_str(driver: str, server: str, database: str, username: str, password: str) -> str:
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


def _is_driver_missing_error(exc: Exception) -> bool:
    msg = str(exc)
    # Erros típicos quando o driver não existe no sistema
    return (
        "Can't open lib" in msg
        or "file not found" in msg
        or "Data source name not found" in msg
        or "IM002" in msg
    )


def get_engine():
    server = st.secrets["SQL_SERVER"]
    database = st.secrets["SQL_DB"]
    username = st.secrets["SQL_USER"]
    password = st.secrets["SQL_PASSWORD"]

    # 1) tenta Driver 18
    driver18 = "ODBC Driver 18 for SQL Server"
    conn_str_18 = _build_conn_str(driver18, server, database, username, password)
    conn_key_18 = f"{driver18}|{server}|{database}|{username}|{hash(password)}"

    try:
        return _engine(conn_key_18, conn_str_18)
    except Exception as e:
        if not _is_driver_missing_error(e):
            # não é falta de driver → propaga (credencial, firewall, etc.)
            raise

    # 2) fallback Driver 17
    driver17 = "ODBC Driver 17 for SQL Server"
    conn_str_17 = _build_conn_str(driver17, server, database, username, password)
    conn_key_17 = f"{driver17}|{server}|{database}|{username}|{hash(password)}"

    return _engine(conn_key_17, conn_str_17)