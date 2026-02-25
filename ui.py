import streamlit as st
import os

def configurar_layout():
    """Configura o título da aba, ícone e layout do app."""
    st.set_page_config(
        page_title="NPS Admin",
        page_icon="📊",
        layout="wide"
    )

def sidebar_logo():
    """Renderiza o logo no topo do menu lateral."""
    logo_path = "assets/logo.png"
    
    if os.path.exists(logo_path):
        # st.logo coloca a imagem no topo da sidebar automaticamente
        st.logo(logo_path, size="large")
    else:
        st.sidebar.warning(f"⚠️ Logo não encontrado em: {logo_path}")

def sidebar_info():
    """Renderiza as informações de texto na sidebar."""
    with st.sidebar:
        st.markdown("---") # Linha divisória para estética
        st.markdown("### NPS Admin")
        st.markdown("👋 Bem-vindo ao painel de administração do NPS!")
        st.markdown("Use as páginas laterais para navegar entre clientes, respostas e importação.")