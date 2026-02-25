import time
from datetime import datetime

import streamlit as st
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from ui import sidebar_logo, sidebar_info, configurar_layout
from db import get_engine

configurar_layout()

sidebar_logo()
sidebar_info()

APP_TITLE = "NPS POC — Admin"
TZ_LABEL = "Horário local"


# =========================
# Healthcheck conexão
# =========================

def conn_healthcheck():

    t0 = time.time()

    try:

        engine = get_engine()

        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        ms = int((time.time() - t0) * 1000)

        return {
            "ok": True,
            "ms": ms,
            "err": None,
            "hint": None
        }

    except SQLAlchemyError as e:

        ms = int((time.time() - t0) * 1000)

        msg = str(e)

        hint = None

        if "Login failed" in msg:
            hint = "Credenciais inválidas no secrets.toml"

        elif "timeout" in msg.lower():
            hint = "Firewall ou servidor inacessível"

        return {
            "ok": False,
            "ms": ms,
            "err": msg,
            "hint": hint
        }


# =========================
# HOME
# =========================

st.set_page_config(
    page_title=APP_TITLE,
    layout="wide"
)

st.title("🧭 NPS Admin (POC)")

top_left, top_mid, top_right = st.columns([3,1,1])

with top_left:

    st.caption(
        "Painel inicial. Use as páginas para operar dados."
    )

with top_mid:

    if st.button("🔄 Revalidar conexão", use_container_width=True):

        st.cache_resource.clear()
        st.rerun()

with top_right:

    st.caption(
        f"{TZ_LABEL}: **{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}**"
    )


# =========================
# Healthcheck
# =========================

hc = conn_healthcheck()

k1,k2,k3,k4 = st.columns(4)

k1.metric(
    "Banco",
    "Online ✅" if hc["ok"] else "Offline ❌"
)

k2.metric(
    "Latência",
    f'{hc["ms"]} ms'
)

k3.metric(
    "Ambiente",
    st.secrets.get("ENV","local")
)

k4.metric(
    "Driver",
    "ODBC 18"
)


if not hc["ok"]:

    st.error("Sem conexão com Azure SQL")

    st.code(hc["err"])

    if hc["hint"]:
        st.warning(hc["hint"])

    st.stop()


# =========================
# Navegação
# =========================

st.divider()

st.subheader("O que você quer fazer agora?")


def card(title,desc,button,page,icon):

    with st.container(border=True):

        c1,c2 = st.columns([5,2])

        with c1:

            st.markdown(f"### {icon} {title}")
            st.write(desc)

        with c2:

            st.write("")
            st.write("")

            if st.button(button,use_container_width=True,type="primary"):
                st.switch_page(page)


colA,colB = st.columns(2)

with colA:

    card(
        "Clientes",
        "Cadastro e ações",
        "Abrir Clientes →",
        "pages/01_👤_Clientes.py",
        "👥"
    )


with colB:

    card(
        "Respostas",
        "Auditoria de respostas",
        "Abrir Respostas →",
        "pages/02_📨_Respostas.py",
        "📩"
    )