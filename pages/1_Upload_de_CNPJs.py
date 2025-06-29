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
            SELECT v.*
            FROM visao_empresa_agrupada_base v
            JOIN unnest(:cnpjs) WITH ORDINALITY AS temp(cnpj) ON v.cnpj = temp.cnpj
        """)
        df = pd.read_sql(query, conn, params={"cnpjs": list(cnpjs)})
    return df

# Inicialização de estados
for key in ["df_cnpjs", "df_oportunidades", "df_coords", "etapa"]:
    if key not in st.session_state:
        st.session_state[key] = None

if st.session_state.etapa is None:
    st.session_state.etapa = "etapa1"

def etapa1():
    st.header("1️⃣ Upload de CNPJs e Enriquecimento de Dados")

    # Botão para refazer, limpando os dados
    if st.button("🔄 Refazer Dados Enriquecidos"):
        st.session_state.df_cnpjs = None
        st.session_state.df_importado = None

    # ✅ Se dados enriquecidos já estiverem carregados, mostra direto
    if st.session_state.get("df_cnpjs") is not None:
        st.success(f"{len(st.session_state.df_cnpjs)} registros carregados.")
        st.dataframe(st.session_state.df_cnpjs)

        excel_data = to_excel(st.session_state.df_cnpjs)
        st.download_button("📥 Baixar Excel com Dados Enriquecidos", excel_data, "dados_enriquecidos.xlsx")

        st.markdown("---")
        if st.button("📊 Ir para Análise Gráfica"):
            st.session_state.dados_cliente = st.session_state.df_cnpjs  # 🔁 reforça persistência
            st.switch_page("pages/2_Analise_Grafica.py")

    else:
        # Usa o df importado se já tiver salvo, senão permite upload
        df_importado = st.session_state.get("df_importado")

        if df_importado is None:
            file = st.file_uploader("Importe sua lista de CNPJs (CSV ou Excel)", type=["csv", "xlsx"])
            if file:
                try:
                    df_importado = pd.read_csv(file, sep=';', dtype=str) if file.name.endswith(".csv") else pd.read_excel(file, dtype=str)
                    if "cnpj" not in df_importado.columns:
                        st.error("O arquivo precisa ter uma coluna chamada 'cnpj'.")
                        return

                    st.session_state.df_importado = df_importado  # 🔒 SALVA PARA MANUTER APÓS RERUN

                except Exception as e:
                    st.error(f"Erro ao ler o arquivo: {e}")
                    return

        # Se o df_importado está definido, mostra botão para buscar dados
        if df_importado is not None:
            cnpjs_lista = df_importado["cnpj"].dropna().astype(str).tolist()
            st.success(f"{len(cnpjs_lista)} CNPJs carregados.")

            if st.button("🔍 Buscar Dados Enriquecidos"):
                df_enriquecido = buscar_dados_enriquecidos(cnpjs_lista)
                if df_enriquecido.empty:
                    st.warning("Nenhum dado encontrado.")
                else:
                    st.session_state.df_cnpjs = df_enriquecido
                    st.session_state.cliente_carregado = True
                    st.session_state.dados_cliente = df_enriquecido
                    st.rerun()  # Recarrega a tela já com os dados prontos

# Rodar etapa
etapa1()
