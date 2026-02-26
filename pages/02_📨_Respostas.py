import streamlit as st
import pandas as pd
from sqlalchemy import text
from ui import sidebar_logo
from db import get_engine

sidebar_logo()

CATS = ["Promotor", "Neutro", "Detrator"]

def read_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        rows = result.fetchall()
        cols = list(result.keys())
    return pd.DataFrame(rows, columns=cols)

def exec_sql(sql: str, params: dict | None = None) -> None:
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})

def load_respostas(empresa, categoria, q, dt_ini, dt_fim, incluir_excluidas, topn):
    where = []
    params = {}

    if not incluir_excluidas:
        where.append("deleted_at IS NULL")

    if (empresa or "").strip():
        where.append("empresa = :empresa")
        params["empresa"] = empresa.strip()

    if categoria in CATS:
        where.append("categoria = :categoria")
        params["categoria"] = categoria

    if dt_ini:
        where.append("data_resposta >= :dt_ini")
        params["dt_ini"] = str(dt_ini)

    if dt_fim:
        where.append("data_resposta <= :dt_fim")
        params["dt_fim"] = str(dt_fim)

    if (q or "").strip():
        where.append("(LOWER(email) LIKE :like OR LOWER(motivo) LIKE :like OR CAST(cliente_id AS NVARCHAR(100)) LIKE :like_id)")
        params["like"] = f"%{q.strip().lower()}%"
        params["like_id"] = f"%{q.strip()}%"

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    sql = f"""
    SELECT TOP ({int(topn)})
    resposta_id, cliente_id, email, empresa, data_resposta, nota,
    categoria, motivo, canal,
    jira_issue_key, jira_issue_url,
    deleted_at, created_at
    FROM dbo.nps_respostas
    {where_sql}
    ORDER BY data_resposta DESC, created_at DESC;
    """

    df = read_df(sql, params)

    def cat_badge(c):
        return "🟩 Promotor" if c == "Promotor" else ("🟨 Neutro" if c == "Neutro" else ("🟥 Detrator" if c == "Detrator" else "⬜"))
    df["categoria_badge"] = df["categoria"].apply(cat_badge) if "categoria" in df.columns else ""
    df["status"] = df["deleted_at"].apply(lambda x: "🟢 Ativa" if pd.isna(x) else "⚫ Excluído")
    return df

def load_one(resposta_id):
    sql = """
    SELECT TOP 1
      resposta_id, cliente_id, email, empresa, perfil_decisor, segmento,
      data_resposta, nota, categoria, motivo, canal,
      tally_form_id, tally_submission_id,
      deleted_at, created_at
    FROM dbo.nps_respostas
    WHERE resposta_id = :resposta_id;
    """
    df = read_df(sql, {"resposta_id": resposta_id})
    if df.empty:
        return None
    return df.iloc[0].to_dict()

def update_resposta(resposta_id, nota, categoria, motivo, canal):
    sql = """
    UPDATE dbo.nps_respostas
    SET nota=:nota, categoria=:categoria, motivo=:motivo, canal=:canal
    WHERE resposta_id=:resposta_id;
    """
    exec_sql(sql, {
        "resposta_id": resposta_id,
        "nota": int(nota),
        "categoria": categoria,
        "motivo": (motivo or "").strip() or None,
        "canal": (canal or "").strip() or None,
    })

def soft_delete(resposta_id):
    exec_sql("UPDATE dbo.nps_respostas SET deleted_at = SYSUTCDATETIME() WHERE resposta_id=:resposta_id;", {"resposta_id": resposta_id})

def restore(resposta_id):
    exec_sql("UPDATE dbo.nps_respostas SET deleted_at = NULL WHERE resposta_id=:resposta_id;", {"resposta_id": resposta_id})

st.set_page_config(page_title="Respostas", layout="wide")
st.title("📩 Respostas")

left, right = st.columns([3,1])
with right:
    if st.button("🏠 Home", use_container_width=True, key="resp_home"):
        st.switch_page("Home.py")

with st.sidebar:
    st.header("Filtros")
    empresa = st.text_input("Empresa", "", key="resp_empresa")
    categoria = st.selectbox("Categoria", ["Todos"] + CATS, index=0, key="resp_cat")
    q = st.text_input("Busca (email/motivo/cliente_id)", "", key="resp_q")
    dt_ini = st.date_input("De", value=None, key="resp_dt_ini")
    dt_fim = st.date_input("Até", value=None, key="resp_dt_fim")
    incluir_excluidas = st.checkbox("Incluir excluídas", value=False, key="resp_inc_exc")
    topn = st.slider("Limite", 50, 2000, 300, step=50, key="resp_topn")

cat_param = "" if categoria == "Todos" else categoria
df = load_respostas(empresa, cat_param, q, dt_ini, dt_fim, incluir_excluidas, int(topn))
cat_param = "" if categoria == "Todos" else categoria
df = load_respostas(empresa, cat_param, q, dt_ini, dt_fim, incluir_excluidas, int(topn))

def truncate_motivo(text, limit=120):
    if text is None:
        return ""
    text = str(text).strip()
    return text[:limit] + "..." if len(text) > limit else text

if not df.empty:
    df["motivo_resumo"] = df["motivo"].apply(truncate_motivo) if "motivo" in df.columns else ""
else:
    df["motivo_resumo"] = ""

k1, k2, k3 = st.columns(3)
k1.metric("Respostas na lista", int(len(df)))
k2.metric("Ativas", int((df["status"] == "🟢 Ativa").sum()) if "status" in df else 0)
k3.metric("Excluídas", int((df["status"] == "⚫ Excluído").sum()) if "status" in df else 0)

st.subheader("Lista")
cols = [
    "resposta_id",
    "data_resposta",
    "empresa",
    "email",
    "nota",
    "categoria_badge",
    "motivo_resumo",
    "jira_issue_url",
    "status",
    "created_at"
]
cols = [c for c in cols if c in df.columns]
st.dataframe(
    df[cols],
    use_container_width=True,
    hide_index=True,
    column_config={
        "jira_issue_url": st.column_config.LinkColumn(
            "Jira",
            display_text="🔗 Abrir",
        )
    }
)

st.divider()

st.subheader("Ações")
ids = df["resposta_id"].tolist() if not df.empty else []

def format_option(resposta_id):
    row = df[df["resposta_id"] == resposta_id].iloc[0]
    email = row.get("email") or "-"
    empresa = row.get("empresa") or "-"
    return f"{empresa} • {email} • {resposta_id[-12:]}"

selected = (
    st.selectbox(
        "Selecionar resposta",
        ids,
        format_func=format_option,
        key="resp_selected"
    )
    if ids else None
)
if not ids:
    st.info("Sem respostas para selecionar com os filtros atuais.")
    st.stop()

item = load_one(selected)
if not item:
    st.error("Resposta não encontrada.")
    st.stop()

if item.get("deleted_at") is None:
    st.success("ATIVA")
else:
    st.warning("EXCLUÍDA")

tab_view, tab_edit, tab_delete = st.tabs(["🔎 Ver", "✏️ Editar", "🗑️ Excluir/Reativar"])

with tab_view:
    st.json(item)

with tab_edit:
    c1, c2, c3 = st.columns(3)
    nota_val = int(item["nota"]) if item.get("nota") is not None else 0
    nota = c1.number_input("nota", 0, 10, value=nota_val, key="resp_edit_nota")
    idx = CATS.index(item["categoria"]) if item.get("categoria") in CATS else 0
    categoria_new = c2.selectbox("categoria", CATS, index=idx, key="resp_edit_cat")
    canal = c3.text_input("canal", value=item.get("canal") or "", key="resp_edit_canal")

    motivo = st.text_area("motivo", value=item.get("motivo") or "", height=120, key="resp_edit_motivo")

    if st.button("Salvar alterações", type="primary", key="resp_edit_save"):
        try:
            update_resposta(selected, nota, categoria_new, motivo, canal)
            st.success("Atualizado ✅")
            st.toast("Resposta atualizada.", icon="✅")
            st.rerun()
        except Exception as e:
            st.error(f"Erro ao atualizar: {e}")

with tab_delete:
    if item.get("deleted_at") is None:
        st.warning("Exclusão lógica: define deleted_at e remove da visão padrão.")
        confirm = st.checkbox("Confirmo exclusão lógica", key="resp_del_confirm")
        if st.button("Excluir logicamente", disabled=not confirm, key="resp_del_btn"):
            try:
                soft_delete(selected)
                st.success("Excluída ✅")
                st.rerun()
            except Exception as e:
                st.error(f"Erro: {e}")
    else:
        if st.button("Reativar", key="resp_restore_btn"):
            try:
                restore(selected)
                st.success("Reativada ✅")
                st.rerun()
            except Exception as e:
                st.error(f"Erro: {e}")