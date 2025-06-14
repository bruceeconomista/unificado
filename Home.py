import streamlit as st
from streamlit_lottie import st_lottie
import requests

st.set_page_config(page_title="Diagnóstico Empresarial", layout="wide")

# ===== Função para carregar animação =====
def load_lottie_url(url):
    r = requests.get(url)
    if r.status_code != 200:
        return None
    return r.json()

# ===== Animação Lottie (exemplo do LottieFiles) =====
lottie_hero = load_lottie_url("https://assets1.lottiefiles.com/packages/lf20_tutvdkg0.json")

# ===== HERO SECTION =====
st.markdown("""
<style>
.hero {
    text-align: center;
    padding: 2rem 1rem;
    background: linear-gradient(90deg, #f0f2f6 0%, #ffffff 100%);
    border-radius: 12px;
    margin-bottom: 3rem;
}
h1 {
    font-size: 2.8rem;
}
</style>
""", unsafe_allow_html=True)

with st.container():
    col1, col2 = st.columns([3, 2])
    with col1:
        #st.markdown('<div class="hero">', unsafe_allow_html=True)
        st.markdown("### 🚀 Bem-vindo à plataforma")
        st.markdown("## **Diagnóstico Empresarial & Pesquisa de Mercado**")
        st.markdown("Soluções inteligentes para descobrir oportunidades, analisar perfis empresariais e crescer com dados.")
        st.markdown("</div>", unsafe_allow_html=True)
        st.markdown("#### 👇 Escolha um produto para começar")
    with col2:
        if lottie_hero:
            st_lottie(lottie_hero, speed=1, height=300, key="hero")

# ===== FUNCIONALIDADES =====
st.markdown("## ⚙️ Funcionalidades")
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown("### 📊 Diagnóstico Empresarial")
    st.markdown("- Upload de CNPJs\n- Enriquecimento de dados\n- Visualização gráfica\n- Mapa de Oportunidades")

with col2:
    st.markdown("### 🔍 Pesquisa de Mercado")
    st.markdown("- Filtros avançados\n- Análise de crescimento\n- Comparação geográfica por CEP ou bairro")

with col3:
    st.markdown("### 📄 Relatórios Profissionais")
    st.markdown("- Exportação para Excel\n- PDF com gráficos\n- Preparado para tomada de decisão")

st.markdown("---")

# ===== CALL TO ACTION BUTTONS =====
st.markdown("### ✅ Pronto para começar?")
col_btn1, col_btn2 = st.columns(2)
with col_btn1:
    if st.button("📥 Iniciar Diagnóstico Empresarial"):
        st.switch_page("pages/1_Upload_de_CNPJs.py")# Nome exato da página no menu
with col_btn2:
    if st.button("🔍 Ir para Pesquisa de Mercado"):
        st.switch_page("pages/Pesquisa de Mercado.py")

# ===== FOOTER =====
st.markdown("---")
st.caption("© 2025 Sua Empresa • Desenvolvido com ❤️ em Streamlit • Design inspirado na Datlo")
