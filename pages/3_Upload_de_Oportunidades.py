
# CÓDIGO FINAL COM TODAS AS ETAPAS, BOTÕES, ESTILO MODERNO E CORREÇÕES

import streamlit as st
import pandas as pd
from io import BytesIO
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.types import String
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from unidecode import unidecode

st.set_page_config(layout="wide", page_title="Diagnóstico e Oportunidades")
st.title("📊 Diagnóstico e Mapa de Oportunidades")

DATABASE_URL = 

# Funções auxiliares

def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

def buscar_dados_enriquecidos(cnpjs):
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        query = text("""
            SELECT *
            FROM visao_empresa_completa
            WHERE cnpj = ANY(:cnpjs)
        """).bindparams(cnpjs=ARRAY(String))
        df = pd.read_sql(query, conn, params={"cnpjs": cnpjs})
    return df

# Inicialização
for key in ["df_oportunidades", "df_coords", "df_coords_tipo"]:
    if key not in st.session_state:
        st.session_state[key] = None

st.header("3️⃣ Upload do Universo de Oportunidades e Coordenadas")

# Botão para refazer uploads
if st.button("🔄 Refazer Uploads"):
    st.session_state.df_oportunidades = None
    st.session_state.df_coords = None
    st.session_state.df_coords_tipo = None
    st.rerun()

# Se já estiverem carregados
if st.session_state.df_oportunidades is not None and st.session_state.df_coords is not None:
    st.success("Arquivos carregados anteriormente.")
    st.subheader("📄 Oportunidades")
    st.dataframe(st.session_state.df_oportunidades)

    st.subheader("📍 Coordenadas")
    st.dataframe(st.session_state.df_coords)

    st.markdown("---")
    if st.button("➡️ Ir para Mapa de Oportunidades"):
        st.switch_page("pages/4_Mapa_de_Oportunidades.py")

else:
    oportunidades_file = st.file_uploader("Upload do universo total de oportunidades", type=["csv", "xlsx"], key="upload_oportunidades")
    if oportunidades_file:
        df = pd.read_csv(oportunidades_file, dtype=str) if oportunidades_file.name.endswith(".csv") else pd.read_excel(oportunidades_file, dtype=str)
        st.session_state.df_oportunidades = df
        st.success("Base de oportunidades carregada.")
        st.dataframe(df.head())

    coords_file = st.file_uploader("Upload do arquivo com coordenadas (por CEP ou por Bairro)", type=["csv", "xlsx"], key="upload_coords")
    if coords_file:
        try:
            if coords_file.name.endswith(".csv"):
                df = pd.read_csv(coords_file, dtype=str)
            else:
                df = pd.read_excel(coords_file, dtype=str)
        except Exception as e:
            st.error(f"Erro ao carregar o arquivo de coordenadas: {e}")
            st.stop()

        df.columns = df.columns.str.strip().str.lower()
        df.rename(columns={'município': 'municipio'}, inplace=True)

        # Diagnóstico: CEP ou Bairro?
        if 'cep' in df.columns and {'latitude', 'longitude'}.issubset(df.columns):
            tipo_coord = 'cep'
        elif {'uf', 'municipio', 'bairro', 'latitude', 'longitude'}.issubset(df.columns):
            tipo_coord = 'bairro'
        else:
            st.error("O arquivo de coordenadas deve conter colunas para CEP ou para Bairro com latitude e longitude.")
            st.stop()

        st.session_state.df_coords = df
        st.session_state.df_coords_tipo = tipo_coord  # salvar o tipo de coordenada

        st.success(f"Base de coordenadas por {tipo_coord.upper()} carregada.")
        st.dataframe(df.head())

    if st.session_state.df_oportunidades is not None and st.session_state.df_coords is not None:
        st.markdown("---")
        if st.button("➡️ Ir para Mapa de Oportunidades"):
            st.switch_page("pages/4_Mapa_de_Oportunidades.py")
