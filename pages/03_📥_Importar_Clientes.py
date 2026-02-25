# pages/03_📥_Importar_Clientes.py
import os
import io
import re
import time
import pandas as pd
import streamlit as st
import pyodbc
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import urllib
from functools import lru_cache
import hashlib
from ui import sidebar_logo

sidebar_logo()
load_dotenv()

APP_TITLE = "Importar Clientes — NPS POC"

PERFIS = ["Decisor", "Influenciador"]

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
        pool_pre_ping=True,
        pool_recycle=1800
    )

def conn_healthcheck():
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, None
    except Exception as e:
        return False, str(e)

def read_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        rows = result.fetchall()
        cols = list(result.keys())
    return pd.DataFrame(rows, columns=cols)

def normalize(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def make_cliente_id(email: str, empresa: str) -> str:
    base = f"{normalize(empresa)}|{normalize(email)}"
    digest = hashlib.sha256(base.encode("utf-8")).hexdigest()
    return "C" + digest[:16]

def insert_cliente(nome, email, empresa, perfil_decisor, segmento):
    cliente_id = make_cliente_id(email, empresa)

    sql = """
    INSERT INTO dbo.nps_clientes
      (cliente_id, nome, email, empresa, perfil_decisor, segmento,
       ativo, status_envio, ultimo_envio, proximo_envio, ultimo_erro,
       created_at, updated_at)
    VALUES
      (:cliente_id, :nome, :email, :empresa, :perfil_decisor, :segmento,
       1, 'Pendente', NULL, CAST(GETDATE() AS DATE), NULL,
       SYSUTCDATETIME(), SYSUTCDATETIME());
    """

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(
            text(sql),
            {
                "cliente_id": cliente_id,
                "nome": (nome or "").strip(),
                "email": (email or "").strip(),
                "empresa": (empresa or "").strip(),
                "perfil_decisor": (perfil_decisor or "").strip(),
                "segmento": (segmento or "").strip() or None,
            }
        )

    return cliente_id

def _norm_col(c: str) -> str:
    return (c or "").strip().lower().replace(" ", "_")

def read_import_file(uploaded_file) -> pd.DataFrame:
    name = (uploaded_file.name or "").lower()
    if name.endswith((".xlsx", ".xls")):
        df = pd.read_excel(uploaded_file, dtype=str)
    elif name.endswith(".csv"):
        df = pd.read_csv(uploaded_file, dtype=str)
    else:
        raise ValueError("Formato não suportado. Envie .xlsx/.xls (ou .csv).")

    df.columns = [_norm_col(c) for c in df.columns]
    df = df.fillna("")

    missing = [c for c in ["nome", "email", "empresa"] if c not in df.columns]
    if missing:
        raise ValueError(
            f"Planilha sem colunas obrigatórias: {missing}. "
            "Esperado: nome, email, empresa (opcionais: perfil_decisor, segmento)."
        )

    if "perfil_decisor" not in df.columns:
        df["perfil_decisor"] = "Decisor"
    if "segmento" not in df.columns:
        df["segmento"] = ""

    df["nome"] = df["nome"].astype(str).str.strip()
    df["email"] = df["email"].astype(str).str.strip()
    df["empresa"] = df["empresa"].astype(str).str.strip()
    df["perfil_decisor"] = df["perfil_decisor"].astype(str).str.strip()
    df["segmento"] = df["segmento"].astype(str).str.strip()

    df = df[(df["nome"] != "") & (df["email"] != "") & (df["empresa"] != "")]
    return df

def validate_import_df(df: pd.DataFrame) -> dict:
    invalid_email = df[~df["email"].str.contains("@", na=False)]
    invalid_perfil = df[~df["perfil_decisor"].isin(PERFIS)]
    dup = df.duplicated(subset=["email", "empresa"], keep=False)
    dup_df = df[dup].sort_values(["empresa", "email"])
    return {"invalid_email": invalid_email, "invalid_perfil": invalid_perfil, "dup_df": dup_df}

def exists_email_empresa_bulk(df: pd.DataFrame) -> set[tuple[str, str]]:
    # busca existentes no banco para mostrar antes de importar
    if df.empty:
        return set()

    pairs = {(e.lower().strip(), emp.lower().strip()) for e, emp in zip(df["email"], df["empresa"])}

    # SQL simples: traz tudo e cruza em memória (ok pra POC)
    # Se quiser otimizar depois, a gente faz TVP ou tabela temporária.
    q = """
    SELECT LOWER(email) AS email, LOWER(empresa) AS empresa
    FROM dbo.nps_clientes;
    """
    all_db = read_df(q)
    db_pairs = set(zip(all_db["email"].astype(str), all_db["empresa"].astype(str)))

    return pairs.intersection(db_pairs)

def import_clientes_df(df: pd.DataFrame) -> dict:
    inserted = 0
    skipped = 0
    errors = []

    for _, r in df.iterrows():
        try:
            insert_cliente(r["nome"], r["email"], r["empresa"], r["perfil_decisor"], r["segmento"])
            inserted += 1
        except pyodbc.IntegrityError:
            skipped += 1
        except Exception as e:
            errors.append(
                {"nome": r["nome"], "email": r["email"], "empresa": r["empresa"], "error": str(e)}
            )

    return {"inserted": inserted, "skipped": skipped, "errors": errors}


# ----------------------------
# UI
# ----------------------------
st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title("📥 Importar Clientes (Excel)")

ok, err = conn_healthcheck()
c1, c2 = st.columns([3, 1])
with c1:
    st.caption("Importação em lote com preview + validação + confirmação antes de gravar.")
with c2:
    st.metric("Banco", "Online ✅" if ok else "Offline ❌")

if not ok:
    st.error(f"Sem conexão com o Azure SQL. Motivo: {err}")
    st.stop()

st.divider()

with st.expander("⬇️ Template", expanded=True):
    template = pd.DataFrame(
        [
            {"nome": "Maria Silva", "email": "maria@empresa.com", "empresa": "Empresa X", "perfil_decisor": "Decisor", "segmento": "Finance"},
            {"nome": "João Souza", "email": "joao@empresa.com", "empresa": "Empresa Y", "perfil_decisor": "Influenciador", "segmento": "TI"},
        ]
    )
    st.download_button(
        "Baixar template (CSV)",
        data=template.to_csv(index=False).encode("utf-8"),
        file_name="template_clientes.csv",
        mime="text/csv"
    )
    st.caption("Você pode abrir o CSV no Excel, preencher, e salvar como .xlsx se preferir.")

st.subheader("1) Enviar planilha")
up = st.file_uploader("Escolha o arquivo (.xlsx/.xls/.csv)", type=["xlsx", "xls", "csv"])

if not up:
    st.info("Envie um arquivo para começar.")
    st.stop()

try:
    df_imp = read_import_file(up)
except Exception as e:
    st.error(f"Não consegui ler o arquivo: {e}")
    st.stop()

st.success(f"Arquivo lido ✅ Linhas importáveis: {len(df_imp)}")

issues = validate_import_df(df_imp)

if not issues["invalid_email"].empty:
    st.warning(f"E-mails inválidos: {len(issues['invalid_email'])}")
    st.dataframe(issues["invalid_email"], use_container_width=True, hide_index=True)

if not issues["invalid_perfil"].empty:
    st.warning(f"Perfil inválido (use Decisor/Influenciador): {len(issues['invalid_perfil'])}")
    st.dataframe(issues["invalid_perfil"], use_container_width=True, hide_index=True)

if not issues["dup_df"].empty:
    st.warning(f"Duplicados na planilha (email+empresa): {len(issues['dup_df'])}")
    st.dataframe(issues["dup_df"], use_container_width=True, hide_index=True)

with st.expander("👀 Preview (primeiras 200 linhas)", expanded=True):
    st.dataframe(df_imp.head(200), use_container_width=True, hide_index=True)

has_critical = (len(df_imp) == 0) or (not issues["invalid_email"].empty) or (not issues["invalid_perfil"].empty)

st.subheader("2) Conferir duplicados no banco")
with st.spinner("Checando registros existentes no banco..."):
    existing_pairs = exists_email_empresa_bulk(df_imp)

if existing_pairs:
    st.info(f"Já existem no banco (email+empresa): {len(existing_pairs)}. Eles serão marcados como 'já existiam' na importação.")
else:
    st.success("Nenhum duplicado encontrado por email+empresa ✅")

st.subheader("3) Confirmar e importar")
if has_critical:
    st.error("Corrija e-mails/perfis inválidos (ou linhas vazias) para liberar a importação.")
    st.stop()

confirm = st.checkbox("Confirmo que quero importar esses clientes agora")
do_import = st.button("✅ Importar agora", type="primary", disabled=not confirm)

if do_import:
    with st.spinner("Importando..."):
        res = import_clientes_df(df_imp)

    st.success(f"Importação concluída ✅ Inseridos: {res['inserted']} | Já existiam: {res['skipped']}")

    if res["errors"]:
        st.error(f"Falhas em {len(res['errors'])} linhas.")
        with st.expander("Ver erros"):
            st.dataframe(pd.DataFrame(res["errors"]), use_container_width=True, hide_index=True)

    st.toast("Import finalizado.", icon="✅")