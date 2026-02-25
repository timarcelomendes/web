import streamlit as st
import subprocess

st.title("ODBC Debug")

st.code(subprocess.getoutput("odbcinst -j"), language="text")
st.code(subprocess.getoutput("odbcinst -q -d || true"), language="text")
st.code(subprocess.getoutput("ls -la /opt/microsoft/msodbcsql18/lib64/ 2>/dev/null || echo 'msodbcsql18 não encontrado'"), language="text")
st.code(subprocess.getoutput("find / -name 'libmsodbcsql-18.*.so*' 2>/dev/null | head"), language="text")