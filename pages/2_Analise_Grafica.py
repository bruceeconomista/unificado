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

# Importações para PDF
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_CENTER
from PIL import Image as PILImage # Renomeado para evitar conflito com Image do ReportLab

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

# --- Nova Função para Gerar PDF ---
def gerar_pdf_analise(df, pdf_buffer):
    doc = SimpleDocTemplate(pdf_buffer, pagesize=A4)
    styles = getSampleStyleSheet()

    # Estilo para o título principal
    title_style = ParagraphStyle(
        name='TitleStyle',
        parent=styles['h1'],
        fontSize=20,
        alignment=TA_CENTER,
        spaceAfter=24
    )
    # Estilo para subtítulos das seções
    section_title_style = ParagraphStyle(
        name='SectionTitleStyle',
        parent=styles['h2'],
        fontSize=16,
        spaceAfter=12,
        spaceBefore=18
    )

    story = []

    # Título geral do PDF
    story.append(Paragraph("Diagnóstico Empresarial Consolidado", title_style))
    story.append(Spacer(1, 0.2 * inch))

    # Definir as abas e os títulos para o PDF
    tabs_info = [
        ("CNAE Principal", "Análise por CNAE Principal"),
        ("CNAE Secundário", "Análise por CNAE Secundário"),
        ("Porte", "Distribuição por Porte da Empresa"),
        ("Localização", "Distribuição por Bairros"),
        ("Capital", "Distribuição de Capital Social"),
        ("Idade", "Idade das Empresas"),
        ("Qualificação", "Qualificação dos Sócios"),
        ("Faixa Etária", "Faixa Etária dos Sócios")
    ]

    # Processar cada aba e adicionar ao PDF
    # Preparar df para gráficos de capital e idade que precisam de tratamento prévio
    df['capital_social'] = pd.to_numeric(df['capital_social'], errors='coerce').fillna(0)
    df['data_inicio_atividade'] = pd.to_datetime(df['data_inicio_atividade'], errors='coerce')
    hoje = pd.Timestamp.today()
    df['idade'] = (hoje - df['data_inicio_atividade']).dt.days // 365
    bins_idade = [0, 1, 2, 3, 5, 10, float('inf')]
    labels_idade = ["≤1", "1-2", "2-3", "3-5", "5-10", ">10"]
    df['faixa_idade'] = pd.cut(df['idade'], bins=bins_idade, labels=labels_idade)

    bins_capital = [0, 10000, 50000, 100000, 300000, 500000, 1_000_000, float('inf')]
    labels_capital = ["<10k", "10-50k", "50-100k", "100-300k", "300-500k", "500k-1M", ">1M"]
    df['faixa_capital'] = pd.cut(df['capital_social'], bins=bins_capital, labels=labels_capital)


    for i, (tab_name, section_title) in enumerate(tabs_info):
        fig = None
        # Adicionar título da seção
        story.append(Paragraph(section_title, section_title_style))
        story.append(Spacer(1, 0.1 * inch))

        if tab_name == "CNAE Principal":
            top = df['cnae_principal'].value_counts().head(10).reset_index()
            top.columns = ['CNAE', 'Quantidade']
            fig = px.bar(top, x='CNAE', y='Quantidade', color='Quantidade', template='plotly_dark')
        elif tab_name == "CNAE Secundário":
            sec = df['cnae_secundario'].value_counts().head(10).reset_index()
            sec.columns = ['CNAE Secundário', 'Quantidade']
            fig = px.bar(sec, x='CNAE Secundário', y='Quantidade', color='Quantidade', template='plotly_dark')
        elif tab_name == "Porte":
            porte = df['porte_empresa'].value_counts().reset_index()
            porte.columns = ['Porte', 'Total']
            fig = px.pie(porte, names='Porte', values='Total', hole=0.5, template='seaborn')
        elif tab_name == "Localização":
            bairros = df['bairro'].value_counts()
            top_bairros = bairros.head(9)
            outros = bairros.iloc[9:].sum()
            final = pd.concat([top_bairros, pd.Series({'Outros': outros})]).reset_index()
            final.columns = ['Bairro', 'Total']
            fig = px.pie(final, names='Bairro', values='Total', title='Bairros', template='seaborn')
        elif tab_name == "Capital":
            cap = df['faixa_capital'].value_counts().sort_index().reset_index()
            cap.columns = ['Faixa de Capital', 'Quantidade']
            fig = px.bar(cap, x='Faixa de Capital', y='Quantidade', color='Quantidade', template='plotly_dark')
        elif tab_name == "Idade":
            idade = df['faixa_idade'].value_counts().sort_index().reset_index()
            idade.columns = ['Faixa de Idade', 'Quantidade']
            fig = px.bar(idade, x='Faixa de Idade', y='Quantidade', color='Quantidade', template='plotly_dark')
        elif tab_name == "Qualificação":
            if 'qualificacao_socio' in df.columns:
                q = df['qualificacao_socio'].value_counts().reset_index()
                q.columns = ['Qualificação', 'Total']
                fig = px.bar(q, x='Qualificação', y='Total', color='Total', template='plotly_dark')
        elif tab_name == "Faixa Etária":
            if 'faixa_etaria_socio' in df.columns:
                fe = df['faixa_etaria_socio'].value_counts().reset_index()
                fe.columns = ['Faixa Etária', 'Total']
                fig = px.bar(fe, x='Faixa Etária', y='Total', color='Total', template='plotly_dark')
        
        if fig:
            # Salvar o gráfico como imagem em memória e adicionar ao PDF
            img_buffer = BytesIO()
            fig.write_image(img_buffer, format='png', width=800, height=450, scale=2) # Aumentar a resolução
            img_buffer.seek(0)
            
            # Usar PIL para redimensionar se necessário e então ReportLab Image
            img = PILImage.open(img_buffer)
            # Calcular a proporção para caber na largura da página (A4 tem ~595pt de largura)
            # Definir uma largura máxima para a imagem (ex: 7 polegadas = 7 * 72 pts = 504 pts)
            max_width_pts = 7 * inch
            aspect_ratio = img.height / img.width
            img_width = min(img.width, max_width_pts)
            img_height = img_width * aspect_ratio
            
            # Adicionar imagem ao story
            story.append(Image(img_buffer, width=img_width, height=img_height))
            story.append(Spacer(1, 0.2 * inch)) # Espaço após cada gráfico
            story.append(Spacer(1, 0.2 * inch)) # Espaço extra entre seções
            if i < len(tabs_info) - 1: # Adicionar quebra de página se não for o último gráfico
                # story.append(PageBreak()) # Use PageBreak para garantir nova página para cada seção
                pass # Removido PageBreak para que fiquem um abaixo do outro conforme solicitado

    doc.build(story)

# Inicialização de estados
for key in ["df_cnpjs", "df_oportunidades", "df_coords", "etapa"]:
    if key not in st.session_state:
        st.session_state[key] = None

if st.session_state.etapa is None:
    st.session_state.etapa = "etapa1"



def etapa2():
    st.header("2️⃣ Análise Gráfica dos Dados Enriquecidos")
    df = st.session_state.df_cnpjs
    if df is None:
        st.warning("Nenhum dado carregado.")
        return

    # Certifique-se de que o DataFrame df é tratado antes de ser usado nos gráficos e no PDF
    df['capital_social'] = pd.to_numeric(df['capital_social'], errors='coerce').fillna(0)
    df['data_inicio_atividade'] = pd.to_datetime(df['data_inicio_atividade'], errors='coerce')
    
    # Pré-cálculos para faixas de idade e capital para que o PDF e as abas usem os mesmos dados
    hoje = pd.Timestamp.today()
    df['idade'] = (hoje - df['data_inicio_atividade']).dt.days // 365
    bins_idade = [0, 1, 2, 3, 5, 10, float('inf')]
    labels_idade = ["≤1", "1-2", "2-3", "3-5", "5-10", ">10"]
    df['faixa_idade'] = pd.cut(df['idade'], bins=bins_idade, labels=labels_idade)

    bins_capital = [0, 10000, 50000, 100000, 300000, 500000, 1_000_000, float('inf')]
    labels_capital = ["<10k", "10-50k", "50-100k", "100-300k", "300-500k", "500k-1M", ">1M"]
    df['faixa_capital'] = pd.cut(df['capital_social'], bins=bins_capital, labels=labels_capital)


    # --- Exibição das abas no Streamlit ---
    tabs = st.tabs(["CNAE Principal", "CNAE Secundário", "Porte", "Localização", "Capital", "Idade", "Qualificação", "Faixa Etária"])

    with tabs[0]:
        top = df['cnae_principal'].value_counts().head(10).reset_index()
        top.columns = ['CNAE', 'Quantidade']
        st.plotly_chart(px.bar(top, x='CNAE', y='Quantidade', color='Quantidade', template='plotly_dark'))

    with tabs[1]:
        sec = df['cnae_secundario'].value_counts().head(10).reset_index()
        sec.columns = ['CNAE Secundário', 'Quantidade']
        st.plotly_chart(px.bar(sec, x='CNAE Secundário', y='Quantidade', color='Quantidade', template='plotly_dark'))

    with tabs[2]:
        porte = df['porte_empresa'].value_counts().reset_index()
        porte.columns = ['Porte', 'Total']
        fig = px.pie(porte, names='Porte', values='Total', hole=0.5, template='seaborn')
        st.plotly_chart(fig)

    with tabs[3]:
        bairros = df['bairro'].value_counts()
        top_bairros = bairros.head(9)
        outros = bairros.iloc[9:].sum()
        final = pd.concat([top_bairros, pd.Series({'Outros': outros})]).reset_index()
        final.columns = ['Bairro', 'Total']
        st.plotly_chart(px.pie(final, names='Bairro', values='Total', title='Bairros', template='seaborn'))

    with tabs[4]:
        cap = df['faixa_capital'].value_counts().sort_index().reset_index()
        cap.columns = ['Faixa de Capital', 'Quantidade']
        st.plotly_chart(px.bar(cap, x='Faixa de Capital', y='Quantidade', color='Quantidade', template='plotly_dark'))

    with tabs[5]:
        idade = df['faixa_idade'].value_counts().sort_index().reset_index()
        idade.columns = ['Faixa de Idade', 'Quantidade']
        st.plotly_chart(px.bar(idade, x='Faixa de Idade', y='Quantidade', color='Quantidade', template='plotly_dark'))

    with tabs[6]:
        if 'qualificacao_socio' in df.columns:
            q = df['qualificacao_socio'].value_counts().reset_index()
            q.columns = ['Qualificação', 'Total']
            st.plotly_chart(px.bar(q, x='Qualificação', y='Total', color='Total', template='plotly_dark'))

    with tabs[7]:
        if 'faixa_etaria_socio' in df.columns:
            fe = df['faixa_etaria_socio'].value_counts().reset_index()
            fe.columns = ['Faixa Etária', 'Total']
            st.plotly_chart(px.bar(fe, x='Faixa Etária', y='Total', color='Total', template='plotly_dark'))

    st.markdown("---")

    # --- Botão para Exportar PDF ---
    if st.session_state.df_cnpjs is not None:
        pdf_buffer = BytesIO()
        gerar_pdf_analise(st.session_state.df_cnpjs.copy(), pdf_buffer) # Passar uma cópia para evitar modificações indesejadas
        pdf_buffer.seek(0)
        st.download_button(
            label="Download Análise Gráfica (PDF)",
            data=pdf_buffer,
            file_name="diagnostico_empresarial_consolidado.pdf",
            mime="application/pdf"
        )

    if st.button("➡️ Ir para Upload de Oportunidades"):
        st.switch_page("pages/3_Upload_de_Oportunidades.py")

# ETAPA 3
# Rodar etapa
etapa2()