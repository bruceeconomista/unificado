
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

if df_universo is None or df_coords is None or tipo_coords is None:
    st.warning("⚠️ Carregue os dados na etapa 3 antes de acessar o mapa.")
    st.stop()

# Preparação
df_universo['cnpj'] = df_universo['cnpj'].astype(str).str.zfill(14)
if df_cliente is not None:
    df_cliente['cnpj'] = df_cliente['cnpj'].astype(str).str.zfill(14)
    df_oportunidades = df_universo[~df_universo['cnpj'].isin(df_cliente['cnpj'])].copy()
else:
    df_oportunidades = df_universo.copy()

if df_oportunidades.empty:
    st.warning("Todas as oportunidades já estão atendidas.")
    st.stop()

if tipo_coords == "cep":
    df_oportunidades['cep'] = df_oportunidades['cep'].astype(str).str.zfill(8)

    #grupo = df_oportunidades.groupby('cep').size().reset_index(name='qtd_oportunidades') - TRECHO SUBSTITUÍDO POR

    df_oportunidades['capital_social'] = pd.to_numeric(df_oportunidades['capital_social'], errors='coerce').fillna(0)
    grupo = df_oportunidades.groupby('cep').agg(
    qtd_oportunidades=('cnpj', 'count'),
    capital_total=('capital_social', 'sum')
    ).reset_index()

    # FIM DA SUBSTITUIÇÃO

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

    df_oportunidades['capital_social'] = pd.to_numeric(df_oportunidades['capital_social'], errors='coerce').fillna(0)

    grupo = df_oportunidades.groupby(['uf', 'municipio', 'bairro']).agg(
        qtd_oportunidades=('cnpj', 'count'),
        capital_total=('capital_social', 'sum')
    ).reset_index()

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

# Seletor interativo
visualizacao = st.radio(
    "🔍 Visualizar oportunidades por:",
    ["Quantidade de Empresas", "Capital Social"]
)

# Configuração do mapa dependendo da escolha
if visualizacao == "Quantidade de Empresas":
    fig = px.scatter_mapbox(
        df_mapa,
        lat='latitude',
        lon='longitude',
        size='qtd_oportunidades',
        color='qtd_oportunidades',
        color_continuous_scale=px.colors.sequential.Plasma_r,
        hover_name=hover,
        mapbox_style="open-street-map",
        template="plotly_dark",
        size_max=50,
        zoom=4,
        height=700
    )
    fig.update_traces(
        hovertemplate="<b>%{hovertext}</b><br>" +
                      "Qtd Oportunidades: %{marker.size:,}<br><extra></extra>"
    )

    
elif visualizacao == "Capital Social":
    import numpy as np
    df_mapa['capital_total_log'] = df_mapa['capital_total'].apply(lambda x: np.log1p(x))

    fig = px.scatter_mapbox(
        df_mapa,
        lat='latitude',
        lon='longitude',
        size='capital_total_log',
        color='capital_total_log',
        color_continuous_scale=px.colors.sequential.Plasma_r,
        hover_name=hover,
        mapbox_style="open-street-map",
        template="plotly_dark",
        size_max=50,
        zoom=4,
        height=700,
        custom_data=['capital_total']
    )

    fig.update_traces(
        hovertemplate="<b>%{hovertext}</b><br>" +
                      "Capital Total: R$ %{customdata[0]:,.2f}<br><extra></extra>"
    )

fig.update_layout(
    dragmode="zoom",
    uirevision="keep",
    mapbox=dict(accesstoken=None)
)

st.plotly_chart(fig, use_container_width=True, config={"scrollZoom": True})


st.success(f"Mapa gerado com {len(df_mapa)} localizações e {df_oportunidades.shape[0]} oportunidades não atendidas.")

st.download_button(
    label="📥 Baixar Oportunidades Não Atendidas",
    data=to_excel(df_oportunidades),
    file_name="oportunidades_nao_atendidas.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)
