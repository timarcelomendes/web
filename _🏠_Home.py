import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
from sqlalchemy import text
from db import get_engine
from ui import sidebar_logo

sidebar_logo()

st.set_page_config(page_title="Home", layout="wide")
st.title("🏠 Painel")
st.caption("Painel inicial. Use as páginas para operar dados.")

# Horário local (mantém)
st.write(f"Horário local: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")

# =========================
# DB helpers
# =========================
def read_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        res = conn.execute(text(sql), params or {})
        rows = res.fetchall()
        cols = list(res.keys())
    return pd.DataFrame(rows, columns=cols)

def safe_int(v, default=0):
    try:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return default
        return int(v)
    except Exception:
        return default

def safe_float(v, default=0.0):
    try:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return default
        return float(v)
    except Exception:
        return default

def nps_from_counts(p: int, n: int, d: int) -> float:
    tot = p + n + d
    if tot <= 0:
        return 0.0
    return ((p / tot) - (d / tot)) * 100.0

def fmt_delta(v: float) -> str:
    sign = "+" if v > 0 else ""
    # 2 casas pra NPS
    return f"{sign}{v:.2f}"

# =========================
# Controles
# =========================
st.divider()
cA, cB, cC = st.columns([1, 1, 2])

with cA:
    periodo = st.selectbox(
        "Período de comparação",
        [7, 14, 30, 60, 90],
        index=0,
        format_func=lambda x: f"Últimos {x} dias",
        key="home_periodo",
    )

hoje = date.today()
ini_atual = hoje - timedelta(days=int(periodo))
ini_anterior = ini_atual - timedelta(days=int(periodo))
fim_anterior = ini_atual

# empresas disponíveis
df_emp = read_df("""
    SELECT DISTINCT empresa
    FROM dbo.nps_respostas
    WHERE deleted_at IS NULL
      AND empresa IS NOT NULL
      AND LTRIM(RTRIM(empresa)) <> ''
    ORDER BY empresa ASC;
""")
empresas = ["Todas"] + (df_emp["empresa"].dropna().astype(str).tolist() if not df_emp.empty else [])

with cB:
    empresa_sel = st.selectbox("Empresa", empresas, index=0, key="home_empresa")

empresa_where = ""
params_base = {
    "ini_atual": str(ini_atual),
    "fim_atual": str(hoje),
    "ini_ant": str(ini_anterior),
    "fim_ant": str(fim_anterior),
}
if empresa_sel != "Todas":
    empresa_where = " AND empresa = :empresa "
    params_base["empresa"] = empresa_sel

# =========================
# Dados agregados (KPIs)
# =========================
sql_kpis = f"""
WITH base AS (
  SELECT
    CAST(data_resposta AS DATE) AS dia,
    CASE
      WHEN CAST(data_resposta AS DATE) >= CAST(:ini_atual AS DATE)
       AND CAST(data_resposta AS DATE) <= CAST(:fim_atual AS DATE) THEN 'atual'
      WHEN CAST(data_resposta AS DATE) >= CAST(:ini_ant AS DATE)
       AND CAST(data_resposta AS DATE) <  CAST(:fim_ant AS DATE) THEN 'anterior'
      ELSE NULL
    END AS periodo,
    nota
  FROM dbo.nps_respostas
  WHERE deleted_at IS NULL
    AND data_resposta IS NOT NULL
    {empresa_where}
),
agg AS (
  SELECT
    periodo,
    COUNT(1) AS total,
    SUM(CASE WHEN nota >= 9 THEN 1 ELSE 0 END) AS promotores,
    SUM(CASE WHEN nota BETWEEN 7 AND 8 THEN 1 ELSE 0 END) AS neutros,
    SUM(CASE WHEN nota <= 6 THEN 1 ELSE 0 END) AS detratores
  FROM base
  WHERE periodo IS NOT NULL
  GROUP BY periodo
)
SELECT * FROM agg;
"""
df_k = read_df(sql_kpis, params_base)

def pick(df: pd.DataFrame, periodo: str, col: str):
    if df.empty:
        return 0
    sub = df[df["periodo"] == periodo]
    if sub.empty:
        return 0
    return sub.iloc[0].get(col, 0)

tot_a = safe_int(pick(df_k, "atual", "total"))
p_a = safe_int(pick(df_k, "atual", "promotores"))
n_a = safe_int(pick(df_k, "atual", "neutros"))
d_a = safe_int(pick(df_k, "atual", "detratores"))
nps_a = nps_from_counts(p_a, n_a, d_a)

tot_b = safe_int(pick(df_k, "anterior", "total"))
p_b = safe_int(pick(df_k, "anterior", "promotores"))
n_b = safe_int(pick(df_k, "anterior", "neutros"))
d_b = safe_int(pick(df_k, "anterior", "detratores"))
nps_b = nps_from_counts(p_b, n_b, d_b)

delta_tot = tot_a - tot_b
delta_nps = nps_a - nps_b

# delta_color: NPS é melhor quando sobe => normal
# total respostas depende do seu objetivo. vou deixar normal.
col1, col2, col3, col4 = st.columns(4)
col1.metric("Respostas (janela)", tot_a, delta=f"{delta_tot:+d}", delta_color="normal")
col2.metric("NPS (janela)", f"{nps_a:.2f}", delta=fmt_delta(delta_nps), delta_color="normal")
col3.metric("Promotores", p_a, delta=f"{(p_a-p_b):+d}", delta_color="normal")
col4.metric("Detratores", d_a, delta=f"{(d_a-d_b):+d}", delta_color="inverse")

# “gauge” simples de tendência (barra)
st.caption("Tendência do NPS (janela atual)")
g_min, g_max = -100.0, 100.0
pct = (nps_a - g_min) / (g_max - g_min)
pct = max(0.0, min(1.0, pct))
st.progress(pct, text=f"NPS {nps_a:.2f} (de -100 a 100)")

st.divider()

# =========================
# Série diária (gráficos)
# =========================
sql_daily = f"""
WITH base AS (
  SELECT
    CAST(data_resposta AS DATE) AS dia,
    CASE
      WHEN CAST(data_resposta AS DATE) >= CAST(:ini_atual AS DATE)
       AND CAST(data_resposta AS DATE) <= CAST(:fim_atual AS DATE) THEN 'atual'
      WHEN CAST(data_resposta AS DATE) >= CAST(:ini_ant AS DATE)
       AND CAST(data_resposta AS DATE) <  CAST(:fim_ant AS DATE) THEN 'anterior'
      ELSE NULL
    END AS periodo,
    nota
  FROM dbo.nps_respostas
  WHERE deleted_at IS NULL
    AND data_resposta IS NOT NULL
    {empresa_where}
),
daily AS (
  SELECT
    dia,
    periodo,
    COUNT(1) AS total,
    SUM(CASE WHEN nota >= 9 THEN 1 ELSE 0 END) AS promotores,
    SUM(CASE WHEN nota BETWEEN 7 AND 8 THEN 1 ELSE 0 END) AS neutros,
    SUM(CASE WHEN nota <= 6 THEN 1 ELSE 0 END) AS detratores
  FROM base
  WHERE periodo IS NOT NULL
  GROUP BY dia, periodo
)
SELECT
  dia,
  periodo,
  total,
  promotores,
  neutros,
  detratores
FROM daily
ORDER BY dia ASC;
"""
df = read_df(sql_daily, params_base)

if df.empty:
    st.info("Sem dados no período selecionado.")
    st.stop()

df["nps"] = df.apply(lambda r: nps_from_counts(
    safe_int(r.get("promotores")),
    safe_int(r.get("neutros")),
    safe_int(r.get("detratores")),
), axis=1)

pivot_nps = df.pivot_table(index="dia", columns="periodo", values="nps", aggfunc="mean").sort_index()
pivot_cnt = df.pivot_table(index="dia", columns="periodo", values="total", aggfunc="sum").sort_index()

tab_g, tab_d = st.tabs(["📈 Gráficos", "📋 Detalhes"])

with tab_g:
    left, right = st.columns([2, 1])
    with right:
        modo = st.radio(
            "Exibição",
            ["Somente diário", "Somente média móvel", "Diário + média móvel"],
            index=0,
            horizontal=False,
            key="home_chart_mode",
        )
        mm = st.slider("Janela da média móvel (dias)", 3, 14, 7, key="home_mm_window")

    def with_moving_average(pivot: pd.DataFrame, suffix: str) -> pd.DataFrame:
        if pivot.empty:
            return pivot
        out = pivot.copy()
        for col in pivot.columns:
            out[f"{col} {suffix}"] = pivot[col].rolling(int(mm), min_periods=1).mean()
        return out

    nps_mm = with_moving_average(pivot_nps, f"(MM{int(mm)})")
    cnt_mm = with_moving_average(pivot_cnt, f"(MM{int(mm)})")

    if modo == "Somente diário":
        nps_plot = pivot_nps
        cnt_plot = pivot_cnt
    elif modo == "Somente média móvel":
        nps_plot = nps_mm[[c for c in nps_mm.columns if "(MM" in c]]
        cnt_plot = cnt_mm[[c for c in cnt_mm.columns if "(MM" in c]]
    else:
        nps_plot = nps_mm
        cnt_plot = cnt_mm

    c1, c2 = st.columns(2)
    with c1:
        st.caption("NPS (janela atual vs anterior)")
        st.line_chart(nps_plot, height=260)
    with c2:
        st.caption("Respostas/dia (janela atual vs anterior)")
        st.line_chart(cnt_plot, height=260)

    with left:
        st.info("Dica: use **Diário + média móvel** para enxergar tendência sem perder o detalhe do dia.")

with tab_d:
    st.caption("Tabela diária por período, com deltas (Atual − Anterior).")

    tbl = pivot_nps.rename(columns=lambda c: f"nps_{c}").join(
        pivot_cnt.rename(columns=lambda c: f"total_{c}"),
        how="outer"
    ).sort_index()

    if "nps_atual" in tbl.columns and "nps_anterior" in tbl.columns:
        tbl["Δ nps"] = (tbl["nps_atual"] - tbl["nps_anterior"]).round(2)
    if "total_atual" in tbl.columns and "total_anterior" in tbl.columns:
        tbl["Δ respostas"] = (tbl["total_atual"] - tbl["total_anterior"]).astype("Int64")

    tbl = tbl.reset_index().rename(columns={"dia": "Dia"})

    st.dataframe(tbl, use_container_width=True, hide_index=True)

    csv = tbl.to_csv(index=False).encode("utf-8")
    st.download_button(
        "⬇️ Baixar CSV (detalhes)",
        data=csv,
        file_name="home_detalhes_diarios.csv",
        mime="text/csv",
        key="home_download_details_csv",
    )