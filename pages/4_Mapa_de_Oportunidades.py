
import streamlit as st
import pandas as pd
from io import BytesIO
import plotly.express as px

st.set_page_config(layout="wide", page_title="Diagnóstico e Oportunidades")
st.title("📍 Mapa de Oportunidades")

# Função de exportação
def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# Verificações de carregamento
df_cliente = st.session_state.get("df_cnpjs")
df_universo = st.session_state.get("df_oportunidades")
df_coords = st.session_state.get("df_coords")
tipo_coords = st.session_state.get("df_coords_tipo")

if df_cliente is None or df_universo is None or df_coords is None or tipo_coords is None:
    st.warning("⚠️ Carregue os dados na etapa 3 antes de acessar o mapa.")
    st.stop()

# Preparação
df_universo['cnpj'] = df_universo['cnpj'].astype(str).str.zfill(14)
df_cliente['cnpj'] = df_cliente['cnpj'].astype(str).str.zfill(14)
df_oportunidades = df_universo[~df_universo['cnpj'].isin(df_cliente['cnpj'])].copy()

if df_oportunidades.empty:
    st.warning("Todas as oportunidades já estão atendidas.")
    st.stop()

if tipo_coords == "cep":
    df_oportunidades['cep'] = df_oportunidades['cep'].astype(str).str.zfill(8)
    grupo = df_oportunidades.groupby('cep').size().reset_index(name='qtd_oportunidades')
    df_coords['cep'] = df_coords['cep'].astype(str).str.zfill(8)
    df_mapa = pd.merge(grupo, df_coords[['cep', 'latitude', 'longitude']], on='cep', how='left')

    hover = 'cep'

elif tipo_coords == "bairro":
    from unidecode import unidecode
    def normalizar_bairro(bairro):
        return unidecode(str(bairro).upper().split('/')[0].strip())

    for col in ['uf', 'municipio', 'bairro']:
        df_oportunidades[col] = df_oportunidades[col].str.upper().str.strip()
        df_coords[col] = df_coords[col].str.upper().str.strip()

    df_oportunidades['bairro'] = df_oportunidades['bairro'].apply(normalizar_bairro)
    df_coords['bairro'] = df_coords['bairro'].apply(normalizar_bairro)

    grupo = df_oportunidades.groupby(['uf', 'municipio', 'bairro']).size().reset_index(name='qtd_oportunidades')
    df_mapa = pd.merge(grupo, df_coords[['uf', 'municipio', 'bairro', 'latitude', 'longitude']], on=['uf', 'municipio', 'bairro'], how='left')

    df_mapa['chave'] = df_mapa['uf'] + ' | ' + df_mapa['municipio'] + ' | ' + df_mapa['bairro']
    hover = 'chave'

else:
    st.error("Tipo de coordenada desconhecido.")
    st.stop()

# Conversão e filtragem
df_mapa['latitude'] = pd.to_numeric(df_mapa['latitude'], errors='coerce')
df_mapa['longitude'] = pd.to_numeric(df_mapa['longitude'], errors='coerce')
df_mapa.dropna(subset=['latitude', 'longitude'], inplace=True)

# Diagnóstico
if df_mapa.empty:
    st.warning("Nenhuma coordenada válida encontrada. Não foi possível gerar o mapa.")
    st.stop()

fig = px.scatter_map(
    df_mapa,
    lat='latitude',
    lon='longitude',
    size='qtd_oportunidades',
    color='qtd_oportunidades',
    color_continuous_scale=px.colors.sequential.Plasma_r,
    hover_name=hover,
    map_style="open-street-map",
    size_max=50,
    zoom=4,
    height=700
)

st.plotly_chart(fig, use_container_width=True)

st.success(f"Mapa gerado com {len(df_mapa)} localizações e {df_oportunidades.shape[0]} oportunidades não atendidas.")

st.download_button(
    label="📥 Baixar Oportunidades Não Atendidas",
    data=to_excel(df_oportunidades),
    file_name="oportunidades_nao_atendidas.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)
