import os
import urllib
import streamlit as st
from functools import lru_cache
from sqlalchemy import create_engine, text
from ui import sidebar_logo
from db import get_engine

sidebar_logo()

def engine_healthcheck():
    try:
        engine = get_engine()

        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        return True, None

    except Exception as e:
        return False, str(e)


st.title("🔌 Teste de Conexão - Azure SQL")

ok, err = engine_healthcheck()

if ok:
    st.success("Conectado com sucesso ao Azure SQL ✅")
else:
    st.error("Falha na conexão ❌")
    st.code(err)