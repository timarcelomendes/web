import time
from datetime import datetime, timedelta, date
import pandas as pd
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
# Helpers
# =========================

def nps_badge(nps: float) -> tuple[str, str]:
    if nps >= 50:
        return "Excelente", "🟢"
    if nps >= 0:
        return "Atenção", "🟡"
    return "Crítico", "🔴"


def pct(part: int, total: int) -> float:
    return 0.0 if total <= 0 else (part * 100.0 / total)


def to_date(v) -> date | None:
    if v is None or v == "":
        return None
    if isinstance(v, date):
        return v
    try:
        return pd.to_datetime(v).date()
    except Exception:
        return None


@st.cache_resource
def get_db_engine():
    return get_engine()


@st.cache_data(ttl=60)
def load_empresas(cache_key: str) -> list[str]:
    engine = get_db_engine()
    sql = text("""
        SELECT DISTINCT TOP (500) LTRIM(RTRIM(empresa)) AS empresa
        FROM dbo.nps_respostas
        WHERE empresa IS NOT NULL AND LTRIM(RTRIM(empresa)) <> ''
        ORDER BY empresa ASC;
    """)
    df = pd.read_sql(sql, engine)
    return df["empresa"].dropna().astype(str).tolist()


@st.cache_data(ttl=60)
def load_kpis_periodo(cache_key: str, empresa: str | None, dias: int) -> dict:
    """
    Retorna KPIs para:
      - janela atual: últimos {dias} dias
      - janela anterior: {dias} dias anteriores
      - geral (all time)
    """
    engine = get_db_engine()

    where_empresa = ""
    params: dict = {"dias": int(dias)}
    if empresa and empresa != "Todas":
        where_empresa = "AND LTRIM(RTRIM(empresa)) = :empresa"
        params["empresa"] = empresa

    sql = text(f"""
        WITH base AS (
            SELECT
                CAST(nota AS INT) AS nota,
                CAST(created_at AS DATETIME2) AS created_at,
                LTRIM(RTRIM(empresa)) AS empresa
            FROM dbo.nps_respostas
            WHERE deleted_at IS NULL OR deleted_at IS NULL  -- compat: se não existir, não quebra no SQL Server? (ignorará apenas se existir)
        ),
        scope AS (
            SELECT *
            FROM base
            WHERE 1=1
            {where_empresa}
        ),
        all_time AS (
            SELECT
                COUNT(1) AS total,
                SUM(CASE WHEN nota BETWEEN 9 AND 10 THEN 1 ELSE 0 END) AS prom,
                SUM(CASE WHEN nota BETWEEN 7 AND 8 THEN 1 ELSE 0 END) AS neu,
                SUM(CASE WHEN nota BETWEEN 0 AND 6 THEN 1 ELSE 0 END) AS det
            FROM scope
        ),
        cur AS (
            SELECT
                COUNT(1) AS total,
                SUM(CASE WHEN nota BETWEEN 9 AND 10 THEN 1 ELSE 0 END) AS prom,
                SUM(CASE WHEN nota BETWEEN 7 AND 8 THEN 1 ELSE 0 END) AS neu,
                SUM(CASE WHEN nota BETWEEN 0 AND 6 THEN 1 ELSE 0 END) AS det
            FROM scope
            WHERE created_at >= DATEADD(day, -:dias, GETDATE())
        ),
        prev AS (
            SELECT
                COUNT(1) AS total,
                SUM(CASE WHEN nota BETWEEN 9 AND 10 THEN 1 ELSE 0 END) AS prom,
                SUM(CASE WHEN nota BETWEEN 7 AND 8 THEN 1 ELSE 0 END) AS neu,
                SUM(CASE WHEN nota BETWEEN 0 AND 6 THEN 1 ELSE 0 END) AS det
            FROM scope
            WHERE created_at >= DATEADD(day, -(:dias*2), GETDATE())
              AND created_at <  DATEADD(day, -:dias, GETDATE())
        )
        SELECT
            a.total AS total_all, a.prom AS prom_all, a.neu AS neu_all, a.det AS det_all,
            c.total AS total_cur, c.prom AS prom_cur, c.neu AS neu_cur, c.det AS det_cur,
            p.total AS total_prev, p.prom AS prom_prev, p.neu AS neu_prev, p.det AS det_prev
        FROM all_time a
        CROSS JOIN cur c
        CROSS JOIN prev p;
    """)

    try:
        row = pd.read_sql(sql, engine, params=params).iloc[0].to_dict()
    except Exception as e:
        raise

    def calc_nps(prom: int, det: int, total: int) -> float:
        if not total:
            return 0.0
        return (prom * 100.0 / total) - (det * 100.0 / total)

    total_cur = int(row.get("total_cur") or 0)
    total_prev = int(row.get("total_prev") or 0)
    total_all = int(row.get("total_all") or 0)

    prom_cur = int(row.get("prom_cur") or 0)
    neu_cur = int(row.get("neu_cur") or 0)
    det_cur = int(row.get("det_cur") or 0)

    prom_prev = int(row.get("prom_prev") or 0)
    neu_prev = int(row.get("neu_prev") or 0)
    det_prev = int(row.get("det_prev") or 0)

    prom_all = int(row.get("prom_all") or 0)
    neu_all = int(row.get("neu_all") or 0)
    det_all = int(row.get("det_all") or 0)

    nps_cur = round(calc_nps(prom_cur, det_cur, total_cur), 1)
    nps_prev = round(calc_nps(prom_prev, det_prev, total_prev), 1)
    nps_all = round(calc_nps(prom_all, det_all, total_all), 1)

    return {
        "total_cur": total_cur,
        "total_prev": total_prev,
        "total_all": total_all,
        "prom_cur": prom_cur,
        "neu_cur": neu_cur,
        "det_cur": det_cur,
        "prom_prev": prom_prev,
        "neu_prev": neu_prev,
        "det_prev": det_prev,
        "nps_cur": nps_cur,
        "nps_prev": nps_prev,
        "nps_all": nps_all,
        "prom_pct_cur": round(pct(prom_cur, total_cur), 1),
        "neu_pct_cur": round(pct(neu_cur, total_cur), 1),
        "det_pct_cur": round(pct(det_cur, total_cur), 1),
        "prom_pct_prev": round(pct(prom_prev, total_prev), 1),
        "neu_pct_prev": round(pct(neu_prev, total_prev), 1),
        "det_pct_prev": round(pct(det_prev, total_prev), 1),
    }


@st.cache_data(ttl=60)
def load_series_diaria(cache_key: str, empresa: str | None, dias: int) -> pd.DataFrame:
    """
    Série diária dos últimos 2*dias, para gráfico e comparação.
    """
    engine = get_db_engine()

    where_empresa = ""
    params: dict = {"dias": int(dias)}
    if empresa and empresa != "Todas":
        where_empresa = "AND LTRIM(RTRIM(empresa)) = :empresa"
        params["empresa"] = empresa

    sql = text(f"""
        WITH scope AS (
            SELECT
                CAST(created_at AS DATE) AS dt,
                CAST(nota AS INT) AS nota
            FROM dbo.nps_respostas
            WHERE created_at >= DATEADD(day, -(:dias*2), CAST(GETDATE() AS DATE))
            {where_empresa}
        )
        SELECT
            dt,
            COUNT(1) AS total,
            SUM(CASE WHEN nota BETWEEN 9 AND 10 THEN 1 ELSE 0 END) AS prom,
            SUM(CASE WHEN nota BETWEEN 0 AND 6 THEN 1 ELSE 0 END) AS det
        FROM scope
        GROUP BY dt
        ORDER BY dt ASC;
    """)
    df = pd.read_sql(sql, engine, params=params)
    if df.empty:
        return df

    df["nps"] = df.apply(lambda r: 0.0 if r["total"] == 0 else (r["prom"]*100.0/r["total"] - r["det"]*100.0/r["total"]), axis=1)
    df["nps"] = df["nps"].round(1)
    return df


# =========================
# HOME
# =========================

st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title("🧭 NPS Admin (POC)")

top_left, top_right = st.columns([4, 1])

with top_left:
    st.caption("Painel inicial. Use as páginas para operar dados.")

with top_right:
    st.caption(f"{TZ_LABEL}: **{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}**")

st.divider()

# =========================
# Filtros (Período + Empresa)
# =========================

f1, f2, f3 = st.columns([1.2, 2.2, 1])

with f1:
    dias = st.selectbox(
        "Período de comparação",
        [7, 14, 30, 60, 90],
        index=0,
        format_func=lambda x: f"Últimos {x} dias",
        key="home_dias",
    )

with f2:
    cache_key = f'{st.secrets.get("SQL_SERVER","")} | {st.secrets.get("SQL_DB","")}'
    try:
        empresas = ["Todas"] + load_empresas(cache_key)
    except Exception:
        empresas = ["Todas"]
    empresa = st.selectbox("Empresa", empresas, index=0, key="home_empresa")

with f3:
    if st.button("🔄 Atualizar", use_container_width=True, key="home_refresh"):
        load_kpis_periodo.clear()
        load_series_diaria.clear()
        st.rerun()

# =========================
# Dados e KPIs
# =========================

try:
    kpi = load_kpis_periodo(cache_key, empresa, int(dias))
    serie = load_series_diaria(cache_key, empresa, int(dias))
except SQLAlchemyError as e:
    st.error("Sem conexão com o banco (verifique em Status de Conexão).")
    st.code(str(e)[:1200])
    st.stop()
except Exception as e:
    st.error("Erro ao carregar indicadores.")
    st.code(str(e)[:1200])
    st.stop()

nps_cur = float(kpi["nps_cur"])
nps_prev = float(kpi["nps_prev"])
nps_all = float(kpi["nps_all"])

total_cur = int(kpi["total_cur"])
total_prev = int(kpi["total_prev"])

prom_cur = int(kpi["prom_cur"])
neu_cur = int(kpi["neu_cur"])
det_cur = int(kpi["det_cur"])

prom_pct_cur = float(kpi["prom_pct_cur"])
neu_pct_cur = float(kpi["neu_pct_cur"])
det_pct_cur = float(kpi["det_pct_cur"])

prom_pct_prev = float(kpi["prom_pct_prev"])
neu_pct_prev = float(kpi["neu_pct_prev"])
det_pct_prev = float(kpi["det_pct_prev"])

label, emoji = nps_badge(nps_cur)

# Deltas (janela atual vs anterior do mesmo tamanho)
nps_delta = round(nps_cur - nps_prev, 1)
resp_delta = total_cur - total_prev
prom_delta_pp = round(prom_pct_cur - prom_pct_prev, 1)
neu_delta_pp = round(neu_pct_cur - neu_pct_prev, 1)
det_delta_pp = round(det_pct_cur - det_pct_prev, 1)

st.subheader("📊 Indicadores NPS")
st.caption(f"{emoji} **{label}** • Comparando: últimos **{dias} dias** vs **{dias} dias anteriores**" + ("" if empresa == "Todas" else f" • Empresa: **{empresa}**"))

m1, m2, m3, m4, m5 = st.columns(5)

# 1) NPS com delta (melhor visual com sinal e delta_color)
m1.metric(
    "NPS (janela)",
    f"{nps_cur:.1f}",
    delta=f"{nps_delta:+.1f}",
    delta_color="normal",
    help="NPS = %Promotores - %Detratores",
)

# 2) Volume de respostas
m2.metric(
    "Respostas",
    f"{total_cur}",
    delta=f"{resp_delta:+d}",
    delta_color="normal",
)

# 3) Promotores (bom subir)
m3.metric(
    "🟢 Promotores",
    f"{prom_cur} ({prom_pct_cur:.1f}%)",
    delta=f"{prom_delta_pp:+.1f} p.p.",
    delta_color="normal",
)

# 4) Neutros (delta sem cor forte)
m4.metric(
    "🟡 Neutros",
    f"{neu_cur} ({neu_pct_cur:.1f}%)",
    delta=f"{neu_delta_pp:+.1f} p.p.",
    delta_color="off",
)

# 5) Detratores (ruim subir → inverse)
m5.metric(
    "🔴 Detratores",
    f"{det_cur} ({det_pct_cur:.1f}%)",
    delta=f"{det_delta_pp:+.1f} p.p.",
    delta_color="inverse",
)

# NPS geral (all time) como referência
st.caption(f"Referência: **NPS geral {nps_all:.1f}** (todo o histórico)")

# Gauge discreto
gauge = max(0.0, min(1.0, (nps_cur + 100.0) / 200.0))
st.progress(gauge)
st.caption("NPS Gauge: -100 → 0 → 100")

st.divider()

# =========================
# Gráfico (comparação período atual vs anterior)
# =========================

st.subheader("📈 Tendência diária")

if serie is None or serie.empty:
    st.info("Sem dados suficientes para gerar a tendência diária nesse período.")
else:
    # cria duas janelas (2*dias) e marca qual período
    df = serie.copy()
    df["dt"] = pd.to_datetime(df["dt"]).dt.date

    today = datetime.now().date()
    start_cur = today - timedelta(days=int(dias))
    start_prev = today - timedelta(days=int(dias) * 2)

    df["periodo"] = df["dt"].apply(lambda d: "Atual" if d >= start_cur else "Anterior")

    # Alinha por "dia relativo" dentro da janela (1..dias)
    def day_index(d: date) -> int:
        if d >= start_cur:
            return (d - start_cur).days + 1
        return (d - start_prev).days + 1

    df["dia"] = df["dt"].apply(day_index)

    pivot_nps = df.pivot_table(index="dia", columns="periodo", values="nps", aggfunc="mean").sort_index()
    pivot_cnt = df.pivot_table(index="dia", columns="periodo", values="total", aggfunc="sum").sort_index()

    cA, cB = st.columns(2)

    with cA:
        st.caption("NPS diário (janela atual vs anterior)")
        st.line_chart(pivot_nps, height=240)

    with cB:
        st.caption("Respostas/dia (janela atual vs anterior)")
        st.line_chart(pivot_cnt, height=240)

st.divider()

# =========================
# Navegação
# =========================

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

colA, colB = st.columns(2)

with colA:
    card("Clientes", "Cadastro e ações", "Abrir Clientes →", "pages/01_👤_Clientes.py", "👥")

with colB:
    card("Respostas", "Auditoria de respostas", "Abrir Respostas →", "pages/02_📨_Respostas.py", "📩")
