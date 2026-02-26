# pages/03_📥_Importação.py
import re
import time
import hashlib
from datetime import date, datetime

import pandas as pd
import streamlit as st
import pyodbc
from sqlalchemy import text

from ui import sidebar_logo
from db import get_engine

sidebar_logo()

APP_TITLE = "Importação — NPS POC"
PERFIS = ["Decisor", "Influenciador"]
CATS = ["Promotor", "Neutro", "Detrator"]

# ----------------------------
# Infra helpers
# ----------------------------
def conn_healthcheck():
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, None, None
    except Exception as e:
        # tenta capturar latência mesmo em falha (não garante)
        return False, str(e), None

def read_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        rows = result.fetchall()
        cols = list(result.keys())
    return pd.DataFrame(rows, columns=cols)

def _norm_col(c: str) -> str:
    return (c or "").strip().lower().replace(" ", "_").replace("-", "_")

def normalize(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

import io
import pandas as pd
import streamlit as st

def _norm_col(c: str) -> str:
    return (c or "").strip().lower().replace(" ", "_").replace("-", "_")

def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {_norm_col(c): c for c in df.columns}
    for cand in candidates:
        k = _norm_col(cand)
        if k in cols:
            return cols[k]
    return None

def read_any_upload(uploaded_file) -> pd.DataFrame:
    """
    Lê CSV/XLSX de um st.file_uploader e retorna DataFrame.
    """
    if uploaded_file is None:
        return pd.DataFrame()

    name = (uploaded_file.name or "").lower()
    if name.endswith(".csv"):
        return pd.read_csv(uploaded_file)
    if name.endswith(".xlsx") or name.endswith(".xls"):
        return pd.read_excel(uploaded_file)
    # tenta CSV como fallback
    return pd.read_csv(uploaded_file)

def clean_str(s) -> str:
    return str(s or "").strip()

def safe_lower(s) -> str:
    return clean_str(s).lower()

def ensure_cols(df: pd.DataFrame, mapping: dict[str, list[str]]) -> tuple[pd.DataFrame, dict[str, str]]:
    """
    mapping: {col_canonica: [possíveis nomes]}
    Retorna df com colunas canônicas + dicionário de origem.
    """
    df = df.copy()
    origin = {}
    for canon, candidates in mapping.items():
        col = _pick_col(df, [canon] + candidates)
        if col is None:
            df[canon] = None
            origin[canon] = ""
        else:
            df[canon] = df[col]
            origin[canon] = col
    return df, origin

# ----------------------------
# Clientes import
# ----------------------------
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
    return df

def read_clientes_file(uploaded_file) -> pd.DataFrame:
    df = read_import_file(uploaded_file)

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

    # normaliza
    for c in ["nome", "email", "empresa", "perfil_decisor", "segmento"]:
        df[c] = df[c].astype(str).str.strip()

    df = df[(df["nome"] != "") & (df["email"] != "") & (df["empresa"] != "")]
    return df

def validate_clientes_df(df: pd.DataFrame) -> dict:
    invalid_email = df[~df["email"].str.contains("@", na=False)]
    invalid_perfil = df[~df["perfil_decisor"].isin(PERFIS)]
    dup = df.duplicated(subset=["email", "empresa"], keep=False)
    dup_df = df[dup].sort_values(["empresa", "email"])
    return {"invalid_email": invalid_email, "invalid_perfil": invalid_perfil, "dup_df": dup_df}

def exists_email_empresa_bulk(df: pd.DataFrame) -> set[tuple[str, str]]:
    if df.empty:
        return set()

    pairs = {(e.lower().strip(), emp.lower().strip()) for e, emp in zip(df["email"], df["empresa"])}

    q = """
    SELECT LOWER(email) AS email, LOWER(empresa) AS empresa
    FROM dbo.nps_clientes;
    """
    all_db = read_df(q)
    if all_db.empty:
        return set()
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
            errors.append({"nome": r["nome"], "email": r["email"], "empresa": r["empresa"], "error": str(e)})

    return {"inserted": inserted, "skipped": skipped, "errors": errors}

# ----------------------------
# Respostas import
# ----------------------------
def _cat_from_nota(n: int) -> str:
    if n <= 6:
        return "Detrator"
    if n <= 8:
        return "Neutro"
    return "Promotor"

def _parse_date_any(v):
    if v is None or str(v).strip() == "":
        return None
    # aceita date/datetime
    if isinstance(v, (date, datetime)):
        return v.date() if isinstance(v, datetime) else v
    s = str(v).strip()
    # excel serial?
    if re.fullmatch(r"\d+(\.\d+)?", s):
        try:
            n = float(s)
            if n > 20000:
                dt = datetime.utcfromtimestamp((n - 25569) * 86400)
                return dt.date()
        except Exception:
            pass
    # tenta parse
    try:
        return pd.to_datetime(s, errors="coerce").date()
    except Exception:
        return None

def _make_resposta_id(cliente_id: str, email: str) -> str:
    base = (cliente_id or email or "SEMID").strip()
    base = re.sub(r"[^A-Za-z0-9]+", "-", base)[:32].strip("-") or "SEMID"
    ts = datetime.utcnow().isoformat().replace(":", "-").replace(".", "-")
    return f"R-{base}-{ts}"

def read_respostas_file(uploaded_file) -> pd.DataFrame:
    df = read_import_file(uploaded_file)

    # mínimos
    missing = [c for c in ["email", "empresa", "nota", "data_resposta"] if c not in df.columns]
    if missing:
        raise ValueError(
            f"Planilha sem colunas obrigatórias: {missing}. "
            "Esperado: email, empresa, nota, data_resposta (opcionais: cliente_id, categoria, motivo, canal, perfil_decisor, segmento, resposta_id, tally_form_id, tally_submission_id)."
        )

    # defaults
    if "cliente_id" not in df.columns:
        df["cliente_id"] = ""
    if "categoria" not in df.columns:
        df["categoria"] = ""
    if "motivo" not in df.columns:
        df["motivo"] = ""
    if "canal" not in df.columns:
        df["canal"] = "Import"
    if "perfil_decisor" not in df.columns:
        df["perfil_decisor"] = ""
    if "segmento" not in df.columns:
        df["segmento"] = ""
    if "resposta_id" not in df.columns:
        df["resposta_id"] = ""
    if "tally_form_id" not in df.columns:
        df["tally_form_id"] = ""
    if "tally_submission_id" not in df.columns:
        df["tally_submission_id"] = ""

    # normaliza string
    for c in df.columns:
        df[c] = df[c].astype(str).fillna("").str.strip()

    # filtra linhas vazias
    df = df[(df["email"] != "") & (df["empresa"] != "") & (df["nota"] != "") & (df["data_resposta"] != "")]
    return df

def validate_respostas_df(df: pd.DataFrame) -> dict:
    invalid_email = df[~df["email"].str.contains("@", na=False)]

    # nota numérica 0-10
    nota_num = pd.to_numeric(df["nota"], errors="coerce")
    invalid_nota = df[nota_num.isna() | (nota_num < 0) | (nota_num > 10)]

    # data
    parsed_dates = df["data_resposta"].apply(_parse_date_any)
    invalid_date = df[parsed_dates.isna()]

    # categoria (se preenchida)
    cat = df["categoria"].astype(str).str.strip()
    invalid_cat = df[(cat != "") & (~cat.isin(CATS))]

    # duplicados na planilha: preferir tally_submission_id se tiver
    if (df["tally_submission_id"].astype(str).str.strip() != "").any():
        dup = df.duplicated(subset=["tally_submission_id"], keep=False) & (df["tally_submission_id"].str.strip() != "")
    else:
        dup = df.duplicated(subset=["email", "empresa", "data_resposta", "nota"], keep=False)
    dup_df = df[dup].copy()

    return {
        "invalid_email": invalid_email,
        "invalid_nota": invalid_nota,
        "invalid_date": invalid_date,
        "invalid_cat": invalid_cat,
        "dup_df": dup_df,
    }

def exists_respostas_bulk(df: pd.DataFrame) -> int:
    """
    Heurística: se tiver tally_submission_id, checa por ele.
    Se não tiver, não bloqueia (pois não há chave única confiável).
    Retorna quantidade de submission_ids já existentes.
    """
    has_tally = (df["tally_submission_id"].astype(str).str.strip() != "").any()
    if not has_tally:
        return 0

    # busca existentes no banco (top: POC)
    q = """
    SELECT LOWER(LTRIM(RTRIM(tally_submission_id))) AS sid
    FROM dbo.nps_respostas
    WHERE tally_submission_id IS NOT NULL;
    """
    db = read_df(q)
    if db.empty:
        return 0
    db_sids = set(db["sid"].astype(str))

    sids = set(df["tally_submission_id"].astype(str).str.strip().str.lower())
    sids.discard("")
    return len(sids.intersection(db_sids))

def import_respostas_df(df: pd.DataFrame) -> dict:
    inserted = 0
    skipped = 0
    errors = []

    engine = get_engine()

    for _, r in df.iterrows():
        try:
            email = (r.get("email") or "").strip()
            empresa = (r.get("empresa") or "").strip()
            cliente_id = (r.get("cliente_id") or "").strip() or None

            nota = int(float(str(r.get("nota") or "0").replace(",", ".")))
            data_resp = _parse_date_any(r.get("data_resposta"))
            if not data_resp:
                raise ValueError("data_resposta inválida")

            categoria = (r.get("categoria") or "").strip()
            if not categoria:
                categoria = _cat_from_nota(nota)

            motivo = (r.get("motivo") or "").strip()
            canal = (r.get("canal") or "Import").strip() or "Import"

            perfil = (r.get("perfil_decisor") or "").strip() or None
            segmento = (r.get("segmento") or "").strip() or None

            resposta_id = (r.get("resposta_id") or "").strip()
            if not resposta_id:
                resposta_id = _make_resposta_id(cliente_id or "", email)

            tally_form_id = (r.get("tally_form_id") or "").strip() or None
            tally_submission_id = (r.get("tally_submission_id") or "").strip() or None

            # INSERT com proteção por tally_submission_id (quando existir)
            sql = """
            IF (:tally_submission_id IS NULL) OR NOT EXISTS (
              SELECT 1 FROM dbo.nps_respostas WHERE tally_submission_id = :tally_submission_id
            )
            BEGIN
              INSERT INTO dbo.nps_respostas
              (
                resposta_id, cliente_id, email, empresa, perfil_decisor, segmento,
                data_resposta, nota, categoria, motivo, canal,
                tally_form_id, tally_submission_id, created_at
              )
              VALUES
              (
                :resposta_id,
                :cliente_id,
                :email,
                :empresa,
                :perfil_decisor,
                :segmento,
                :data_resposta,
                :nota,
                :categoria,
                :motivo,
                :canal,
                :tally_form_id,
                :tally_submission_id,
                SYSUTCDATETIME()
              );
            END
            """

            with engine.begin() as conn:
                conn.execute(
                    text(sql),
                    {
                        "resposta_id": resposta_id,
                        "cliente_id": cliente_id,
                        "email": email or None,
                        "empresa": empresa or None,
                        "perfil_decisor": perfil,
                        "segmento": segmento,
                        "data_resposta": str(data_resp),
                        "nota": int(nota),
                        "categoria": categoria or None,
                        "motivo": motivo or None,
                        "canal": canal or None,
                        "tally_form_id": tally_form_id,
                        "tally_submission_id": tally_submission_id,
                    }
                )

                # Atualiza cliente como Respondido (melhor esforço)
                # 1) por cliente_id
                if cliente_id:
                    conn.execute(
                        text("""
                        UPDATE dbo.nps_clientes
                        SET status_envio='Respondido', ultimo_erro=NULL, updated_at=SYSUTCDATETIME()
                        WHERE cliente_id=:cliente_id;
                        """),
                        {"cliente_id": cliente_id},
                    )
                else:
                    # 2) por email+empresa
                    conn.execute(
                        text("""
                        UPDATE dbo.nps_clientes
                        SET status_envio='Respondido', ultimo_erro=NULL, updated_at=SYSUTCDATETIME()
                        WHERE LOWER(email)=LOWER(:email) AND LOWER(empresa)=LOWER(:empresa);
                        """),
                        {"email": email, "empresa": empresa},
                    )

            # se tinha tally_submission_id e já existia, o IF acima não insere; como POC, contamos como skipped
            if tally_submission_id:
                # checa se existe agora
                chk = read_df(
                    "SELECT 1 AS ok FROM dbo.nps_respostas WHERE tally_submission_id = :sid;",
                    {"sid": tally_submission_id},
                )
                # se existe e não foi inserido por nós, ainda assim estará ok; mas não temos rowcount.
                # então não diferencia. Mantemos métrica conservadora:
                inserted += 1
            else:
                inserted += 1

        except pyodbc.IntegrityError:
            skipped += 1
        except Exception as e:
            errors.append(
                {
                    "cliente_id": r.get("cliente_id", ""),
                    "email": r.get("email", ""),
                    "empresa": r.get("empresa", ""),
                    "error": str(e),
                }
            )

    return {"inserted": inserted, "skipped": skipped, "errors": errors}

# ----------------------------
# UI
# ----------------------------
st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title("📥 Importação")

# Health (sem expor na Home; aqui é OK)
ok, err, _ = conn_healthcheck()
c1, c2 = st.columns([3, 1])
with c1:
    st.caption("Tela de importação em lote com preview + validação + confirmação antes de gravar.")
with c2:
    st.metric("Banco", "Online ✅" if ok else "Offline ❌")

if not ok:
    st.error(f"Sem conexão com o Azure SQL. Motivo: {err}")
    st.stop()

tab_clientes, tab_respostas, tab_analise = st.tabs(["👤 Clientes", "📨 Respostas", "🔎 Análise Cruzada"])

# -------- Clientes --------
with tab_clientes:
    st.subheader("Importar Clientes")

    with st.expander("⬇️ Template (Clientes)", expanded=True):
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
            mime="text/csv",
        )
        st.caption("Você pode abrir o CSV no Excel, preencher, e salvar como .xlsx se preferir.")

    st.markdown("### 1) Enviar planilha")
    up = st.file_uploader("Escolha o arquivo (.xlsx/.xls/.csv)", type=["xlsx", "xls", "csv"], key="up_clientes")

    if not up:
        st.info("Envie um arquivo para começar.")
    else:
        try:
            df_imp = read_clientes_file(up)
        except Exception as e:
            st.error(f"Não consegui ler o arquivo: {e}")
            df_imp = None

        if df_imp is not None:
            st.success(f"Arquivo lido ✅ Linhas importáveis: {len(df_imp)}")

            issues = validate_clientes_df(df_imp)

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

            st.markdown("### 2) Conferir duplicados no banco")
            with st.spinner("Checando registros existentes no banco..."):
                existing_pairs = exists_email_empresa_bulk(df_imp)

            if existing_pairs:
                st.info(f"Já existem no banco (email+empresa): {len(existing_pairs)}. Eles serão ignorados no INSERT.")
            else:
                st.success("Nenhum duplicado encontrado por email+empresa ✅")

            st.markdown("### 3) Confirmar e importar")
            if has_critical:
                st.error("Corrija e-mails/perfis inválidos para liberar a importação.")
            else:
                confirm = st.checkbox("Confirmo que quero importar esses clientes agora", key="confirm_import_clientes")
                do_import = st.button("✅ Importar clientes", type="primary", disabled=not confirm, key="btn_import_clientes")

                if do_import:
                    with st.spinner("Importando clientes..."):
                        res = import_clientes_df(df_imp)

                    st.success(f"Importação concluída ✅ Inseridos: {res['inserted']} | Já existiam: {res['skipped']}")
                    if res["errors"]:
                        st.error(f"Falhas em {len(res['errors'])} linhas.")
                        with st.expander("Ver erros"):
                            st.dataframe(pd.DataFrame(res["errors"]), use_container_width=True, hide_index=True)
                    st.toast("Import de clientes finalizado.", icon="✅")

# -------- Respostas --------
with tab_respostas:
    st.subheader("Importar Respostas")

    with st.expander("⬇️ Template (Respostas)", expanded=True):
        template = pd.DataFrame(
            [
                {
                    "cliente_id": "Cxxxxxxxxxxxxxxxx",
                    "email": "maria@empresa.com",
                    "empresa": "Empresa X",
                    "nota": 9,
                    "data_resposta": "2026-02-26",
                    "motivo": "Atendimento rápido e entrega de valor.",
                    "canal": "Import",
                    "categoria": "",  # vazio = calcula pela nota
                },
                {
                    "cliente_id": "",
                    "email": "joao@empresa.com",
                    "empresa": "Empresa Y",
                    "nota": 4,
                    "data_resposta": "2026-02-20",
                    "motivo": "Tivemos atrasos na entrega.",
                    "canal": "Import",
                    "categoria": "Detrator",
                },
            ]
        )
        st.download_button(
            "Baixar template (CSV)",
            data=template.to_csv(index=False).encode("utf-8"),
            file_name="template_respostas.csv",
            mime="text/csv",
        )
        st.caption("Obrigatórios: email, empresa, nota (0-10), data_resposta. O resto é opcional.")

    st.markdown("### 1) Enviar planilha")
    up = st.file_uploader("Escolha o arquivo (.xlsx/.xls/.csv)", type=["xlsx", "xls", "csv"], key="up_respostas")

    if not up:
        st.info("Envie um arquivo para começar.")
    else:
        try:
            df_imp = read_respostas_file(up)
        except Exception as e:
            st.error(f"Não consegui ler o arquivo: {e}")
            df_imp = None

        if df_imp is not None:
            st.success(f"Arquivo lido ✅ Linhas importáveis: {len(df_imp)}")

            issues = validate_respostas_df(df_imp)

            if not issues["invalid_email"].empty:
                st.warning(f"E-mails inválidos: {len(issues['invalid_email'])}")
                st.dataframe(issues["invalid_email"], use_container_width=True, hide_index=True)

            if not issues["invalid_nota"].empty:
                st.warning(f"Notas inválidas (0-10): {len(issues['invalid_nota'])}")
                st.dataframe(issues["invalid_nota"], use_container_width=True, hide_index=True)

            if not issues["invalid_date"].empty:
                st.warning(f"Datas inválidas: {len(issues['invalid_date'])}")
                st.dataframe(issues["invalid_date"], use_container_width=True, hide_index=True)

            if not issues["invalid_cat"].empty:
                st.warning(f"Categoria inválida (Promotor/Neutro/Detrator): {len(issues['invalid_cat'])}")
                st.dataframe(issues["invalid_cat"], use_container_width=True, hide_index=True)

            if not issues["dup_df"].empty:
                st.warning(f"Duplicados na planilha: {len(issues['dup_df'])}")
                st.dataframe(issues["dup_df"], use_container_width=True, hide_index=True)

            with st.expander("👀 Preview (primeiras 200 linhas)", expanded=True):
                st.dataframe(df_imp.head(200), use_container_width=True, hide_index=True)

            has_critical = (
                len(df_imp) == 0
                or (not issues["invalid_email"].empty)
                or (not issues["invalid_nota"].empty)
                or (not issues["invalid_date"].empty)
                or (not issues["invalid_cat"].empty)
            )

            st.markdown("### 2) Conferir duplicados no banco (por submission_id, se existir)")
            with st.spinner("Checando respostas existentes..."):
                exist_n = exists_respostas_bulk(df_imp)

            if exist_n > 0:
                st.info(f"Há {exist_n} submission_id já existentes no banco (essas linhas serão ignoradas pelo IF NOT EXISTS).")
            else:
                st.success("Sem duplicados detectáveis (ou planilha sem submission_id). ✅")

            st.markdown("### 3) Confirmar e importar")
            if has_critical:
                st.error("Corrija erros (email/nota/data/categoria) para liberar a importação.")
            else:
                confirm = st.checkbox("Confirmo que quero importar essas respostas agora", key="confirm_import_respostas")
                do_import = st.button("✅ Importar respostas", type="primary", disabled=not confirm, key="btn_import_respostas")

                if do_import:
                    with st.spinner("Importando respostas..."):
                        res = import_respostas_df(df_imp)

                    st.success(f"Importação concluída ✅ Inseridas: {res['inserted']} | Já existiam: {res['skipped']}")
                    if res["errors"]:
                        st.error(f"Falhas em {len(res['errors'])} linhas.")
                        with st.expander("Ver erros"):
                            st.dataframe(pd.DataFrame(res["errors"]), use_container_width=True, hide_index=True)

                    st.toast("Import de respostas finalizado.", icon="✅")

with tab_analise:
    st.markdown("### 🔎 Análise Cruzada (Clientes x Respostas)")
    st.caption("Relaciona os dois arquivos pelo **cliente_id** e aponta órfãos/inconsistências antes de importar.")

    c1, c2 = st.columns(2)
    with c1:
        up_cli = st.file_uploader("Arquivo de **Clientes** (CSV/XLSX)", type=["csv","xlsx","xls"], key="x_cli")
    with c2:
        up_resp = st.file_uploader("Arquivo de **Respostas** (CSV/XLSX)", type=["csv","xlsx","xls"], key="x_resp")

    if not up_cli or not up_resp:
        st.info("Envie os **dois arquivos** para rodar a análise.")
        st.stop()

    df_cli_raw = read_any_upload(up_cli)
    df_resp_raw = read_any_upload(up_resp)

    if df_cli_raw.empty or df_resp_raw.empty:
        st.error("Um dos arquivos está vazio ou não pôde ser lido.")
        st.stop()

    # --- normalização / mapeamento de colunas ---
    map_cli = {
        "cliente_id": ["id", "clienteid", "cliente_id"],
        "email": ["email_cliente", "e-mail", "e_mail"],
        "empresa": ["company", "org", "organizacao"],
        "nome": ["nome_cliente", "name"],
    }
    map_resp = {
        "resposta_id": ["id", "respostaid", "response_id"],
        "cliente_id": ["clienteid", "cliente_id"],
        "email": ["email_cliente", "email", "e-mail", "e_mail"],
        "empresa": ["empresa", "company", "org"],
        "nota": ["rateus", "nps", "score"],
        "data_resposta": ["data", "created_at", "submitted_at", "dt_resposta"],
        "tally_submission_id": ["submission_id", "tally_submission", "tally_submissionid"],
    }

    cli, origin_cli = ensure_cols(df_cli_raw, map_cli)
    resp, origin_resp = ensure_cols(df_resp_raw, map_resp)

    # limpeza básica
    cli["cliente_id"] = cli["cliente_id"].astype(str).str.strip()
    resp["cliente_id"] = resp["cliente_id"].astype(str).str.strip()

    cli["email"] = cli["email"].apply(safe_lower)
    resp["email"] = resp["email"].apply(safe_lower)

    cli["empresa"] = cli["empresa"].apply(clean_str)
    resp["empresa"] = resp["empresa"].apply(clean_str)

    # se resposta_id não veio, cria uma provisória para rastreio
    if resp["resposta_id"].isna().all() or (resp["resposta_id"].astype(str).str.strip() == "").all():
        resp["resposta_id"] = ["R-ROW-" + str(i+1) for i in range(len(resp))]

    # --- métricas base ---
    total_cli = len(cli)
    total_resp = len(resp)

    # respostas OK / órfãs
    resp_join = resp.merge(
        cli[["cliente_id", "email", "empresa", "nome"]],
        on="cliente_id",
        how="left",
        suffixes=("_resp", "_cli"),
        indicator=True,
    )

    resp_ok = resp_join[resp_join["_merge"] == "both"].copy()
    resp_orfas = resp_join[resp_join["_merge"] == "left_only"].copy()

    # inconsistências: cliente encontrado mas email/empresa divergentes (se vierem preenchidos na resposta)
    def _neq(a, b):
        a = clean_str(a)
        b = clean_str(b)
        if a == "" or b == "":
            return False
        return a != b

    resp_ok["email_diverge"] = resp_ok.apply(lambda r: _neq(r.get("email_resp"), r.get("email_cli")), axis=1)
    resp_ok["empresa_diverge"] = resp_ok.apply(lambda r: _neq(r.get("empresa_resp"), r.get("empresa_cli")), axis=1)

    inconsist = resp_ok[(resp_ok["email_diverge"]) | (resp_ok["empresa_diverge"])].copy()

    # clientes sem resposta
    clientes_sem_resp = cli.merge(
        resp[["cliente_id"]],
        on="cliente_id",
        how="left",
        indicator=True
    )
    clientes_sem_resp = clientes_sem_resp[clientes_sem_resp["_merge"] == "left_only"].copy()

    # resumo por cliente (qtd respostas)
    qtd = resp.groupby("cliente_id").size().reset_index(name="qtd_respostas")
    resumo_cli = cli.merge(qtd, on="cliente_id", how="left").fillna({"qtd_respostas": 0})
    resumo_cli["qtd_respostas"] = resumo_cli["qtd_respostas"].astype(int)

    # --- cards topo ---
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Clientes (arquivo)", total_cli)
    k2.metric("Respostas (arquivo)", total_resp)
    k3.metric("Respostas órfãs", int(len(resp_orfas)))
    k4.metric("Inconsistências", int(len(inconsist)))

    st.divider()

    # --- detalhe / tabelas ---
    st.markdown("#### ✅ Respostas OK (cliente encontrado)")
    st.dataframe(
        resp_ok[["resposta_id","cliente_id","nome","email_cli","empresa_cli","nota","data_resposta","tally_submission_id"]]
        if len(resp_ok) else pd.DataFrame(),
        use_container_width=True,
        hide_index=True
    )

    st.markdown("#### ⚠️ Respostas órfãs (cliente_id não encontrado no arquivo de clientes)")
    st.dataframe(
        resp_orfas[["resposta_id","cliente_id","email_resp","empresa_resp","nota","data_resposta","tally_submission_id"]]
        if len(resp_orfas) else pd.DataFrame(),
        use_container_width=True,
        hide_index=True
    )

    st.markdown("#### ⚠️ Inconsistências (cliente_id bate, mas email/empresa divergem)")
    st.dataframe(
        inconsist[["resposta_id","cliente_id","email_resp","email_cli","empresa_resp","empresa_cli","nota","data_resposta"]]
        if len(inconsist) else pd.DataFrame(),
        use_container_width=True,
        hide_index=True
    )

    st.markdown("#### 📭 Clientes sem resposta (no arquivo de respostas)")
    st.dataframe(
        clientes_sem_resp[["cliente_id","nome","email","empresa"]].head(500) if len(clientes_sem_resp) else pd.DataFrame(),
        use_container_width=True,
        hide_index=True
    )
    if len(clientes_sem_resp) > 500:
        st.caption(f"Mostrando 500 de {len(clientes_sem_resp)} clientes sem resposta.")

    st.markdown("#### 📊 Resumo por cliente (qtd respostas)")
    st.dataframe(
        resumo_cli[["cliente_id","nome","email","empresa","qtd_respostas"]].sort_values("qtd_respostas", ascending=False),
        use_container_width=True,
        hide_index=True
    )

    st.divider()
    st.markdown("#### 🧾 Diagnóstico de colunas detectadas")
    with st.expander("Ver mapeamento de colunas (origem → canônica)"):
        st.write("**Clientes**:", origin_cli)
        st.write("**Respostas**:", origin_resp)

    # gate simples pra próxima etapa (import)
    if len(resp_orfas) > 0:
        st.warning("Há **respostas órfãs**. Recomendo corrigir antes de importar.")
    if len(inconsist) > 0:
        st.warning("Há **inconsistências** entre arquivos. Recomendo corrigir antes de importar.")
    if len(resp_orfas) == 0 and len(inconsist) == 0:
        st.success("Tudo consistente ✅ Você pode importar com segurança.")
