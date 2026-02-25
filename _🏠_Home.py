import os
import time
import urllib.parse
from datetime import datetime
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from functools import lru_cache
from ui import sidebar_logo, sidebar_info, configurar_layout

configurar_layout()

sidebar_logo()
sidebar_info()
load_dotenv()

APP_TITLE = "NPS POC — Admin"
TZ_LABEL = "Horário local"

@lru_cache
def get_engine():
    server = os.environ.get("SQL_SERVER", "").strip()
    db = os.environ.get("SQL_DB", "").strip()
    user = os.environ.get("SQL_USER", "").strip()
    pwd = os.environ.get("SQL_PASSWORD", "").strip()

    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{server},1433;"
        f"Database={db};"
        f"Uid={user};"
        f"Pwd={pwd};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
        "MARS_Connection=yes;"
        "Connection Timeout=10;"
    )
    params = urllib.parse.quote_plus(conn_str)

    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        pool_pre_ping=True,
        pool_recycle=1800,
        future=True,
    )


def conn_healthcheck() -> dict:
    """
    Retorna um dict com:
      ok: bool
      ms: int
      err: str|None
      hint: str|None
    """
    t0 = time.time()
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        ms = int((time.time() - t0) * 1000)
        return {"ok": True, "ms": ms, "err": None, "hint": None}
    except SQLAlchemyError as e:
        ms = int((time.time() - t0) * 1000)
        msg = str(e)
        hint = None
        # dicas comuns (sem inventar demais)
        if "Login failed" in msg or "28000" in msg:
            hint = "Credenciais inválidas (SQL_USER/SQL_PASSWORD) ou usuário sem permissão."
        elif "Cannot open server" in msg or "08001" in msg:
            hint = "Servidor inacessível (DNS/Firewall/Network). Verifique o Firewall do Azure SQL."
        elif "timeout" in msg.lower():
            hint = "Timeout ao conectar. Pode ser Firewall, rede, ou Server name incorreto."
        return {"ok": False, "ms": ms, "err": msg, "hint": hint}
    except Exception as e:
        ms = int((time.time() - t0) * 1000)
        return {"ok": False, "ms": ms, "err": str(e), "hint": "Erro inesperado ao abrir conexão."}


def mask(v: str, keep: int = 3) -> str:
    v = (v or "").strip()
    if not v:
        return "(vazio)"
    if len(v) <= keep:
        return "*" * len(v)
    return v[:keep] + "*" * (len(v) - keep)


def missing_envs() -> list[str]:
    keys = ["SQL_SERVER", "SQL_DB", "SQL_USER", "SQL_PASSWORD"]
    miss = [k for k in keys if not (os.environ.get(k, "") or "").strip()]
    return miss

st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title("🧭 NPS Admin (POC)")

# Top actions
top_left, top_mid, top_right = st.columns([3, 1, 1])
with top_left:
    st.caption("Painel inicial **sem gráficos**. Use as páginas para operar dados e auditoria.")
with top_mid:
    if st.button("🔄 Revalidar conexão", use_container_width=True):
        st.cache_resource.clear() if hasattr(st, "cache_resource") else None  # não quebra se não existir
        get_engine.cache_clear()
        st.rerun()
with top_right:
    st.caption(f"{TZ_LABEL}: **{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}**")

# Pre-check env
miss = missing_envs()
if miss:
    with st.container(border=True):
        st.error("Configuração incompleta no ambiente (.env).")
        st.write("Faltando:")
        st.code("\n".join(miss), language="text")
        st.info("Dica: confirme o `.env` e se o Streamlit está carregando as variáveis (load_dotenv).")
    st.stop()

# Healthcheck
hc = conn_healthcheck()

k1, k2, k3, k4 = st.columns(4)
with k1:
    st.metric("Banco", "Online ✅" if hc["ok"] else "Offline ❌")
with k2:
    st.metric("Latência", f'{hc["ms"]} ms')
with k3:
    st.metric("Ambiente", os.environ.get("ENV", "local"))
with k4:
    st.metric("Driver", "ODBC 18")

if not hc["ok"]:
    with st.container(border=True):
        st.error("Sem conexão com o Azure SQL.")
        st.write("**Motivo (erro bruto):**")
        st.code(hc["err"][:2000], language="text")
        if hc.get("hint"):
            st.warning(hc["hint"])
        st.info("Se for Firewall: adicione o IP atual no Azure SQL Server Firewall / Networking.")
    st.stop()

st.divider()

st.subheader("O que você quer fazer agora?")

def card(title: str, desc: str, button_label: str, page_path: str, icon: str):
    with st.container(border=True):
        c1, c2 = st.columns([5, 2])
        with c1:
            st.markdown(f"### {icon} {title}")
            st.write(desc)
        with c2:
            st.write("")  # espaçador
            st.write("")
            if st.button(button_label, use_container_width=True, type="primary"):
                st.switch_page(page_path)

colA, colB = st.columns(2)

with colA:
    card(
        title="Clientes",
        desc=(
            "• Cadastrar / editar / inativar\n"
            "• Perfil: Decisor / Influenciador\n"
            "• ID automático por empresa + email\n"
            "• Ações: forçar elegível / forçar envio (n8n)\n"
        ),
        button_label="Abrir Clientes →",
        page_path="pages/01_👤_Clientes.py",
        icon="👥",
    )

with colB:
    card(
        title="Respostas",
        desc=(
            "• Filtrar e investigar respostas\n"
            "• Ajustar nota/categoria/motivo/canal\n"
            "• Exclusão lógica por `deleted_at`\n"
            "• Auditoria por período (data_resposta)\n"
        ),
        button_label="Abrir Respostas →",
        page_path="pages/02_📨_Respostas.py",
        icon="📩",
    )

st.divider()

with st.expander("⚙️ Status e Configuração (seguro)", expanded=False):
    st.caption("Exibimos apenas dados essenciais (sem senha).")
    st.code(
        "\n".join(
            [
                f"SQL_SERVER={mask(os.environ.get('SQL_SERVER',''))}",
                f"SQL_DB={mask(os.environ.get('SQL_DB',''))}",
                f"SQL_USER={mask(os.environ.get('SQL_USER',''))}",
                "SQL_PASSWORD=(oculto)",
                f"N8N_FORCE_URL={mask(os.environ.get('N8N_FORCE_URL',''), keep=12)}",
            ]
        ),
        language="text",
    )
    st.caption("Recomendado: `.env` no `.gitignore` e variáveis configuradas também no deploy (Azure/Streamlit Cloud).")