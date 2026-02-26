import time
from datetime import datetime
from db import detect_odbc_driver
import streamlit as st
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from ui import sidebar_logo, sidebar_info, configurar_layout
from db import get_engine
import pandas as pd


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
        return {"ok": True, "ms": ms, "err": None, "hint": None, "engine": engine}

    except SQLAlchemyError as e:
        ms = int((time.time() - t0) * 1000)
        msg = str(e)

        hint = None
        if "Login failed" in msg or "(18456)" in msg:
            hint = "Credenciais inválidas (Secrets)."
        elif "(40615)" in msg or "not allowed to access the server" in msg:
            hint = "Firewall do Azure SQL bloqueando o IP do Streamlit Cloud."
        elif "Can't open lib" in msg or "file not found" in msg:
            hint = "Driver ODBC não está instalado no ambiente."
        elif "timeout" in msg.lower() or "hyt00" in msg.lower():
            hint = "Timeout: firewall/rede/DNS."

        return {"ok": False, "ms": ms, "err": msg, "hint": hint, "engine": None}

@st.cache_data(ttl=60)
def load_nps_kpis(_engine, cache_key: str):
    sql = text("""
        WITH base AS (
            SELECT CAST(r.nota AS INT) AS nota, r.created_at
            FROM dbo.nps_respostas r
        ),
        all_time AS (
            SELECT
                COUNT(1) AS total_respostas,
                SUM(CASE WHEN nota BETWEEN 9 AND 10 THEN 1 ELSE 0 END) AS promotores,
                SUM(CASE WHEN nota BETWEEN 7 AND 8 THEN 1 ELSE 0 END) AS neutros,
                SUM(CASE WHEN nota BETWEEN 0 AND 6 THEN 1 ELSE 0 END) AS detratores
            FROM base
        ),
        last7 AS (
            SELECT
                COUNT(1) AS total_respostas_7d,
                SUM(CASE WHEN nota BETWEEN 9 AND 10 THEN 1 ELSE 0 END) AS promotores_7d,
                SUM(CASE WHEN nota BETWEEN 7 AND 8 THEN 1 ELSE 0 END) AS neutros_7d,
                SUM(CASE WHEN nota BETWEEN 0 AND 6 THEN 1 ELSE 0 END) AS detratores_7d
            FROM base
            WHERE created_at >= DATEADD(day, -7, GETDATE())
        )
        SELECT
            a.total_respostas,
            a.promotores, a.neutros, a.detratores,
            CAST(
                CASE WHEN a.total_respostas = 0 THEN 0
                     ELSE ((a.promotores*100.0/a.total_respostas) - (a.detratores*100.0/a.total_respostas))
                END AS DECIMAL(10,1)
            ) AS nps_geral,

            l.total_respostas_7d,
            l.promotores_7d, l.neutros_7d, l.detratores_7d,
            CAST(
                CASE WHEN l.total_respostas_7d = 0 THEN 0
                     ELSE ((l.promotores_7d*100.0/l.total_respostas_7d) - (l.detratores_7d*100.0/l.total_respostas_7d))
                END AS DECIMAL(10,1)
            ) AS nps_7d
        FROM all_time a
        CROSS JOIN last7 l;
    """)
    return pd.read_sql(sql, _engine).iloc[0]


def pct(part: int, total: int) -> float:
    return 0.0 if total <= 0 else (part * 100.0 / total)


def nps_badge(nps: float) -> tuple[str, str]:
    # label, emoji (simples e efetivo)
    if nps >= 50:
        return "Excelente", "🟢"
    if nps >= 0:
        return "Atenção", "🟡"
    return "Crítico", "🔴"

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
    detect_odbc_driver()
)


if not hc["ok"]:

    st.error("Sem conexão com Azure SQL")

    st.code(hc["err"])

    if hc["hint"]:
        st.warning(hc["hint"])

    st.stop()

# =========================
# KPIs NPS
# =========================

# =========================
# KPIs NPS
# =========================

engine = hc["engine"]  # ✅ usa o engine validado no healthcheck

cache_key = f'{st.secrets.get("SQL_SERVER","")}|{st.secrets.get("SQL_DB","")}'
kpi = load_nps_kpis(engine, cache_key)

nps_geral = float(kpi["nps_geral"] or 0)
nps_7d = float(kpi["nps_7d"] or 0)

total = int(kpi["total_respostas"] or 0)
total_7d = int(kpi["total_respostas_7d"] or 0)

prom = int(kpi["promotores"] or 0)
neu  = int(kpi["neutros"] or 0)
det  = int(kpi["detratores"] or 0)

prom7 = int(kpi["promotores_7d"] or 0)
neu7  = int(kpi["neutros_7d"] or 0)
det7  = int(kpi["detratores_7d"] or 0)

prom_p = pct(prom, total)
neu_p  = pct(neu, total)
det_p  = pct(det, total)

prom_p7 = pct(prom7, total_7d)
neu_p7  = pct(neu7, total_7d)
det_p7  = pct(det7, total_7d)

label, emoji = nps_badge(nps_geral)

# deltas reais vs 7 dias
nps_delta = nps_geral - nps_7d
resp_delta = total - total_7d
prom_delta = prom_p - prom_p7
neu_delta  = neu_p - neu_p7
det_delta  = det_p - det_p7

st.divider()

head_left, head_mid, head_right = st.columns([4, 1.2, 1.2])

with head_left:
    st.subheader("📊 Indicadores NPS (Geral)")
    st.caption(
        f"{emoji} **{label}**  •  Atualizado: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}"
    )

with head_mid:
    if st.button("🔄 Atualizar KPIs", use_container_width=True):
        load_nps_kpis.clear()
        st.rerun()

with head_right:
    st.caption("Comparação")
    st.write("Últimos **7 dias**")

m1, m2, m3, m4, m5 = st.columns(5)

m1.metric("NPS Geral", f"{nps_geral:.1f}", delta=f"{nps_delta:+.1f} vs 7d", help="NPS = %Promotores - %Detratores")
m2.metric("Respostas", f"{total}", delta=f"{resp_delta:+d} vs 7d")
m3.metric("🟢 Promotores", f"{prom} ({prom_p:.1f}%)", delta=f"{prom_delta:+.1f} p.p.")
m4.metric("🟡 Neutros", f"{neu} ({neu_p:.1f}%)", delta=f"{neu_delta:+.1f} p.p.")
m5.metric("🔴 Detratores", f"{det} ({det_p:.1f}%)", delta=f"{det_delta:+.1f} p.p.")

# Linha “premium” extra, sem poluir: resumo do 7d
st.caption(
    f"📅 Últimos 7 dias: **NPS {nps_7d:.1f}** | **{total_7d} respostas** | "
    f"🟢 {prom7} ({prom_p7:.1f}%) • 🟡 {neu7} ({neu_p7:.1f}%) • 🔴 {det7} ({det_p7:.1f}%)"
)

# Gauge simples (bem discreto)
gauge = max(0.0, min(1.0, (nps_geral + 100.0) / 200.0))
st.progress(gauge)
st.caption("NPS Gauge: -100 → 0 → 100")

# =========================
# Navegação
# =========================

st.divider()

st.subheader("O que você quer fazer agora?")


def card(title, desc, button, page, icon):
    with st.container(border=True):
        left, right = st.columns([6,2])

        with left:
            st.markdown(f"### {icon} {title}")
            st.caption(desc)

        with right:
            st.write("")
            if st.button(button, use_container_width=True, type="primary"):
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