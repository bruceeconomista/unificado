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

DATABASE_URL = "postgresql+psycopg2://postgres:0804Bru%21%40%23%24@localhost:5432/empresas"

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

# Inicialização de estados
for key in ["df_cnpjs", "df_oportunidades", "df_coords", "etapa"]:
    if key not in st.session_state:
        st.session_state[key] = None

if st.session_state.etapa is None:
    st.session_state.etapa = "etapa1"



def etapa4():
    st.header("4️⃣ Mapa de Oportunidades por Bairro")

    from unidecode import unidecode

    def normalizar_bairro(bairro):
        return unidecode(bairro.upper().split('/')[0].strip())

    df_cliente = st.session_state.df_cnpjs
    df_universo = st.session_state.df_oportunidades
    df_coords = st.session_state.df_coords

    if df_cliente is None or df_universo is None or df_coords is None:
        st.warning("Carregue todas as etapas anteriores.")
        return

    # CNPJs únicos de cliente e universo
    cnpjs_cliente = set(df_cliente['cnpj'].astype(str).str.zfill(14))
    df_universo['cnpj'] = df_universo['cnpj'].astype(str).str.zfill(14)

    # Apenas os CNPJs que não estão na base do cliente
    df_oportunidades = df_universo[~df_universo['cnpj'].isin(cnpjs_cliente)].copy()

    if df_oportunidades.empty:
        st.warning("Nenhuma oportunidade identificada. Todas já estão atendidas.")
        return

    # Padroniza campos
    df_oportunidades['uf'] = df_oportunidades['uf'].str.upper().str.strip()
    df_oportunidades['municipio'] = df_oportunidades['municipio'].str.upper().str.strip()
    df_oportunidades['bairro'] = df_oportunidades['bairro'].str.upper().str.strip()
    df_coords['uf'] = df_coords['uf'].str.upper().str.strip()
    df_coords['municipio'] = df_coords['municipio'].str.upper().str.strip()
    df_coords['bairro'] = df_coords['bairro'].str.upper().str.strip()

    # Agrupa por localização
    grupo = df_oportunidades.groupby(['uf', 'municipio', 'bairro']).size().reset_index(name='qtd_oportunidades')

    # Aplica normalização ao nome do bairro antes do merge
    grupo['bairro'] = grupo['bairro'].apply(normalizar_bairro)
    df_coords['bairro'] = df_coords['bairro'].apply(normalizar_bairro)

    # Cria chaves de comparação para diagnóstico
    grupo['chave'] = grupo['uf'] + ' | ' + grupo['municipio'] + ' | ' + grupo['bairro']
    df_coords['chave'] = df_coords['uf'] + ' | ' + df_coords['municipio'] + ' | ' + df_coords['bairro']

    bairros_com_oportunidade = set(grupo['chave'])
    bairros_com_coordenadas = set(df_coords['chave'])
    sem_coords = bairros_com_oportunidade - bairros_com_coordenadas
    st.warning(f"{len(sem_coords)} bairros com oportunidades não possuem coordenadas no arquivo enviado.")
    st.write("Exemplos de bairros sem coordenada:")
    st.write(list(sem_coords)[:10])

    # Merge com coordenadas
    df_mapa = pd.merge(grupo, df_coords, on=['uf', 'municipio', 'bairro'], how='left')
    df_mapa['latitude'] = pd.to_numeric(df_mapa['latitude'], errors='coerce')
    df_mapa['longitude'] = pd.to_numeric(df_mapa['longitude'], errors='coerce')
    df_mapa.dropna(subset=['latitude', 'longitude'], inplace=True)

    if df_mapa.empty:
        st.warning("Nenhuma coordenada válida encontrada para as oportunidades.")
        return

    # Geração do mapa interativo
    fig = px.scatter_mapbox(
        df_mapa,
        lat='latitude',
        lon='longitude',
        size='qtd_oportunidades',
        color='qtd_oportunidades', #quando tiver vários bairros ele gerará por gradiente de cores
        color_continuous_scale=px.colors.sequential.Plasma_r, # <--- ADICIONADO AQUI para usar um gradiente de vermelho
        #color_discrete_sequence=["red"],
        hover_name='bairro',
        mapbox_style="open-street-map",
        size_max=50,
        zoom=4,
        height=700
    )
    fig.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        dragmode='zoom',
        mapbox={"zoom": 4, "center": {"lat": -15.8, "lon": -47.9}},
        uirevision=True
    )

    config = {"scrollZoom": True}
    st.plotly_chart(fig, use_container_width=True, config=config)
    st.success(f"Mapa gerado com {len(df_mapa)} bairros e {df_oportunidades.shape[0]} oportunidades não atendidas.")

    # Botão para exportar oportunidades não atendidas
    st.download_button(
        label="📥 Baixar Oportunidades Não Atendidas",
        data=to_excel(df_oportunidades),
        file_name="oportunidades_nao_atendidas.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )



# Rodar etapa
etapa4()