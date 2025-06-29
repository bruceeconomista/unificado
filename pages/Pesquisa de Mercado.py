import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from unidecode import unidecode
import datetime
import plotly.express as px
import re
from collections import Counter

# --- Configuração inicial ---
st.set_page_config(layout="wide", page_title="Consulta Avançada de CNPJs + Pesquisa de Mercado")
st.title("🔍 Consulta Avançada de Empresas com Filtros SQL")

# --- Conexão com banco de dados ---
DATABASE_URL = "postgresql+psycopg2://postgres:0804Bru%21%40%23%24@localhost:5432/empresas"
TABELA = "visao_empresa_agrupada_base"


def get_database_engine_for_app(db_url):
    try:
        engine_ = create_engine(db_url)
        with engine_.connect() as conn:
            conn.execute(text("SELECT 1"))
        st.success("✅ Conexão com o banco de dados estabelecida com sucesso!")
        return engine_
    except Exception as e:
        st.error("❌ ERRO CRÍTICO: Não foi possível conectar ao banco de dados.")
        st.error(f"   Detalhes: {e}")
        st.stop()

engine = get_database_engine_for_app(DATABASE_URL)

# --- Initialize session_state ---
for key in ['df_cnpjs', 'resumo_crescimento', 'df_oportunidades', 'df_coords']:
    if key not in st.session_state:
        st.session_state[key] = None
if 'query_sql_display' not in st.session_state:
    st.session_state['query_sql_display'] = ""
if 'query_sql_display_crescimento' not in st.session_state:
    st.session_state['query_sql_display_crescimento'] = ""

# --- Funções cacheadas ---
@st.cache_data(ttl=300)
def process_dataframe_for_analysis(df_input):
    if df_input is None or df_input.empty:
        return pd.DataFrame()
    df = df_input.copy()
    df['capital_social'] = pd.to_numeric(df.get('capital_social', 0), errors='coerce').fillna(0)
    df['data_inicio_atividade'] = pd.to_datetime(df.get('data_inicio_atividade', '1900-01-01'), errors='coerce')
    hoje = pd.Timestamp.today()
    df['idade'] = (hoje - df['data_inicio_atividade']).dt.days // 365

    bins_idade = [0,1,2,3,5,10,float('inf')]
    labels_idade = ["≤1","1-2","2-3","3-5","5-10",">10"]
    df['faixa_idade'] = pd.cut(df['idade'], bins=bins_idade, labels=labels_idade, right=False)

    bins_capital = [0,50000,100000,500000,1000000,5000000,float('inf')]
    labels_capital = ["Até 50k","50k-100k","100k-500k","500k-1M","1M-5M","Acima de 5M"]
    df['faixa_capital'] = pd.cut(df['capital_social'], bins=bins_capital, labels=labels_capital, right=False)

    if 'bairro' in df.columns:
        df['bairro_normalizado'] = df['bairro'].apply(lambda b: unidecode(str(b).upper().split('/')[0].strip()))
    return df

@st.cache_data(ttl=300)
def get_word_counts(df, column_name):
    if df.empty or column_name not in df.columns:
        return pd.DataFrame()
    stop_words = set(unidecode(w.lower()) for w in [
        "e","de","do","da","dos","das","o","a","os","as","um","uma","uns","umas",
        "para","com","sem","em","no","na","nos","nas","ao","aos","por","pelo",
        "pela","pelos","pelas","ou","nem","mas","mais","menos","desde","até",
        "após","entre","contra","servicos","comercio","industria","vendas",
        "consultoria","digital","online","brasil","grupo","nova","importacao"
    ])
    def clean(text):
        text = unidecode(str(text).lower())
        text = re.sub(r'[^\w\s]', '', text)
        text = re.sub(r'\d+', '', text)
        return [w for w in text.split() if w not in stop_words and len(w)>1]
    all_words = df[column_name].apply(clean).explode().dropna()
    if all_words.empty:
        return pd.DataFrame()
    dfc = pd.DataFrame(Counter(all_words).items(), columns=['Palavra','Frequência'])
    return dfc.sort_values('Frequência', ascending=False)

@st.cache_data(ttl=300)
def get_column_counts(df, column_name):
    if df.empty or column_name not in df.columns:
        return pd.DataFrame()
    counts = df[column_name].value_counts().reset_index()
    alias = {
        'uf': 'UF', 'municipio':'Município', 'bairro_normalizado':'Bairro',
        'porte_empresa':'Porte da Empresa','situacao_cadastral':'Situação Cadastral',
        'faixa_capital':'Capital Social','faixa_idade':'Idade',
        'qualificacao_socio':'Qualificação','faixa_etaria_socio':'Faixa Etária'
    }
    counts.columns = [alias.get(column_name, column_name), 'Total']
    return counts

# NOVA FUNÇÃO: Processa CNAEs para a pesquisa de mercado (contando principais e secundários)
@st.cache_data(ttl=300)
def get_cnae_counts_for_market_research(df_input):
    if df_input is None or df_input.empty:
        return pd.DataFrame()
    
    all_cnaes = []
    # Processa CNAE Principal
    if 'cnae_principal' in df_input.columns and not df_input['cnae_principal'].empty:
        all_cnaes.extend(df_input['cnae_principal'].dropna().astype(str).tolist())

    # Processa CNAEs Secundários
    if 'cnae_secundario' in df_input.columns and not df_input['cnae_secundario'].empty:
        exploded_cnaes = df_input['cnae_secundario'].dropna().astype(str).apply(lambda x: x.split('; ')).explode()
        all_cnaes.extend(exploded_cnaes.tolist())
    
    if not all_cnaes:
        return pd.DataFrame()

    cnae_counts = Counter(all_cnaes)
    df_cnaes = pd.DataFrame(cnae_counts.items(), columns=['CNAE', 'Total'])
    return df_cnaes.sort_values('Total', ascending=False)

def montar_sql(f, limit):
    where = ["situacao_cadastral = 'ATIVA'"]

    def ilike_clauses(col, termos):
        return [f"{col} ILIKE unaccent('%{t}%')" for t in termos if t.strip()]

    def in_clause(col, lista):
        l = [f"'{x.replace(chr(39), chr(39)*2)}'" for x in lista if x.strip()]
        return f"{col} IN ({','.join(l)})" if l else ""

    # Sempre começar com UF e Município
    if f.get('uf_selecionada'):
        where.append(in_clause('uf_normalizado', f['uf_selecionada']))
    if f.get('municipio_termos'):
        termos = f['municipio_termos']
        clauses = ilike_clauses('municipio_normalizado', termos)
        if clauses:
            where.append(f"({' OR '.join(clauses)})")

    # Filtros por texto (com campos normalizados)
    texto_fields = {
        'razao_social': 'razao_social_normalizado',
        'nome_fantasia': 'nome_fantasia_normalizado',
        'cnae_principal': 'cnae_principal_normalizado',
        'natureza_juridica': 'natureza_juridica',
        'bairro': 'bairro_normalizado',
        'ddd': 'ddd1',
        'logradouro': 'logradouro',
        'qualificacao_socio': 'qualificacao_socio',
        'faixa_etaria_socio': 'faixa_etaria_socio'
    }

    for key, column in texto_fields.items():
        termos = f.get(f"{key}_termos", [])
        clauses = ilike_clauses(column, termos)
        if clauses:
            where.append(f"({' OR '.join(clauses)})")

    # Filtros por valores fixos
    if f.get('porte_selecionado'):
        where.append(in_clause('porte_empresa', f['porte_selecionado']))

    if f.get('opcao_simples') in ['S', 'N']:
        where.append(f"opcao_simples = '{f['opcao_simples']}'")
    if f.get('opcao_mei') in ['S', 'N']:
        where.append(f"opcao_mei = '{f['opcao_mei']}'")

    # Filtros por capital
    if f.get('capital_social_min') is not None:
        where.append(f"capital_social >= {f['capital_social_min']}")
    if f.get('capital_social_max') is not None:
        where.append(f"capital_social <= {f['capital_social_max']}")

    # Filtros por datas
    if f.get('data_abertura_apos'):
        dt_str = f['data_abertura_apos'].strftime("%Y-%m-%d")
        where.append(f"data_inicio_atividade >= '{dt_str}'")
    if f.get('data_abertura_antes'):
        dt_str = f['data_abertura_antes'].strftime("%Y-%m-%d")
        where.append(f"data_inicio_atividade <= '{dt_str}'")

    # Filtros por idade
    for key in ['idade_min', 'idade_max']:
        if f.get(key) is not None:
            op = ">=" if 'min' in key else "<="
            where.append(f"DATE_PART('year', AGE(CURRENT_DATE, data_inicio_atividade)) {op} {f[key]}")

    # Filtro por código CNAE direto (agora já está na tabela)
    if f.get('cod_cnae_termos'):
        termos_cnae = [t.strip() for t in f['cod_cnae_termos'] if t.strip()]
        if termos_cnae:
            termos_str = ','.join(f"'{t}'" for t in termos_cnae)
            where.append(f"(cod_cnae_principal IN ({termos_str}) OR cod_cnae_secundario ILIKE ANY (ARRAY[{termos_str}]))")

    sql = f"""
    SELECT *
    FROM {TABELA}
    """

    if where:
        sql += " WHERE " + " AND ".join(where)

    sql += f" LIMIT {limit};"
    return sql

# --- opções fixes ---
@st.cache_data(ttl=3600)
def get_uf_options(_): return ["AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"]
@st.cache_data(ttl=3600)
def get_porte_options(): return ["ME","EPP","DEMAIS"]

@st.cache_data(ttl=300)
def run_query(sql, _engine):
    try:
        df = pd.read_sql(text(sql), engine.connect())
        return df
    except Exception as e:
        st.error(f"Erro ao executar consulta: {e}")
        return pd.DataFrame()

def etapa2():
    st.header("2️⃣ Análise Gráfica")
    df = process_dataframe_for_analysis(st.session_state.df_cnpjs)
    if df.empty:
        st.warning("Nenhum dado carregado.")
        return

    tab_titles = [
        "Palavras Chave (Nome Fantasia)",
        "Localização",
        "CNAE (Principal e Secundário)",
        "Porte",
        "Situação Cadastral",
        "Capital Social",
        "Idade da Empresa",
        "Qualificação do Sócio",
        "Faixa Etária do Sócio"
    ]
    tabs = st.tabs(tab_titles)

    with tabs[0]:
        st.subheader("📊 Análise de Palavras-Chave no Nome Fantasia")
        df_top_words = get_word_counts(df, 'nome_fantasia')

        if not df_top_words.empty:
            top_n = st.slider("Número de palavras para exibir:", min_value=10, max_value=50, value=20, key="top_words_slider_analise_grafica")
            
            fig_words = px.bar(
                df_top_words.head(top_n),
                x='Palavra',
                y='Frequência',
                title=f'Top {top_n} Palavras Mais Frequentes no Nome Fantasia',
                labels={'Palavra': 'Contagem de Palavras'},
                color='Frequência',
                color_continuous_scale=px.colors.sequential.Viridis
            )
            fig_words.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig_words, use_container_width=True)

            st.markdown(f"**Observação:** Esta análise exibe as {top_n} palavras mais comuns nos nomes fantasia dos seus clientes, excluindo termos genéricos e sufixos legais. Isso pode ajudar a identificar tendências e segmentos de mercado para novas prospecções.")
            
            with st.expander("Ver dados brutos das palavras-chave"):
                st.dataframe(df_top_words.head(top_n), use_container_width=True)
        else:
            st.info("Nenhum nome fantasia válido encontrado para análise de palavras-chave após a limpeza ou coluna não disponível.")
            if 'nome_fantasia' not in df.columns:
                 st.warning("Coluna 'nome_fantasia' não encontrada no DataFrame.")

    with tabs[1]:
        st.subheader("📍 Análise por Localização")
        loc_tabs = st.tabs(["Por UF", "Por Município", "Por Bairro"])

        with loc_tabs[0]:
            df_uf_counts = get_column_counts(df, 'uf')
            if not df_uf_counts.empty:
                fig_uf = px.bar(df_uf_counts, x='UF', y='Total', title='Empresas por UF', color='Total', template='plotly_dark')
                st.plotly_chart(fig_uf, use_container_width=True)
            else:
                st.info("Coluna 'uf' não encontrada ou está vazia. Não é possível gerar o gráfico de UF.")

        with loc_tabs[1]:
            df_municipio_counts = get_column_counts(df, 'municipio')
            if not df_municipio_counts.empty:
                top_municipios_n = st.slider("Número de municípios para exibir:", min_value=10, max_value=50, value=20, key="top_municipios_slider_analise_grafica")
                top_municipios = df_municipio_counts.head(top_municipios_n)
                outros_municipios = df_municipio_counts['Total'].iloc[top_municipios_n:].sum()
                
                if outros_municipios > 0:
                    final_municipios_data = pd.concat([top_municipios, pd.DataFrame([['Outros', outros_municipios]], columns=['Município', 'Total'])])
                else:
                    final_municipios_data = top_municipios

                fig_municipio = px.pie(
                    final_municipios_data,
                    names='Município',
                    values='Total',
                    title=f'Empresas por Município (Top {top_municipios_n} + Outros)',
                    template='seaborn'
                )
                st.plotly_chart(fig_municipio, use_container_width=True)
            else:
                st.info("Coluna 'municipio' não encontrada ou está vazia. Não é possível gerar o gráfico de Município.")

        with loc_tabs[2]:
            df_bairro_counts = get_column_counts(df, 'bairro_normalizado')
            if not df_bairro_counts.empty:
                top_bairros_n = st.slider("Número de bairros para exibir:", min_value=10, max_value=50, value=20, key="top_bairros_slider_analise_grafica")
                top_bairros = df_bairro_counts.head(top_bairros_n)
                outros_bairros = df_bairro_counts['Total'].iloc[top_bairros_n:].sum()
                
                if outros_bairros > 0:
                    final_bairros_data = pd.concat([top_bairros, pd.DataFrame([['Outros', outros_bairros]], columns=['Bairro', 'Total'])])
                else:
                    final_bairros_data = top_bairros
                
                fig_bairro = px.pie(
                    final_bairros_data,
                    names='Bairro',
                    values='Total',
                    title=f'Empresas por Bairro (Top {top_bairros_n} + Outros)',
                    template='seaborn'
                )
                st.plotly_chart(fig_bairro, use_container_width=True)
            else:
                st.info("Coluna 'bairro' não encontrada ou está vazia. Não é possível gerar o gráfico de Bairro.")

    with tabs[2]: # This is the CNAE tab in the existing `etapa2`
        st.subheader("📊 Análise de CNAEs (Principal e Secundário)")

        cnae_type = st.radio(
            "Selecione o tipo de CNAE para analisar:",
            ('CNAE Principal', 'CNAEs Secundários', 'Ambos'),
            key="cnae_type_radio"
        )

        all_cnaes = []
        if cnae_type == 'CNAE Principal' or cnae_type == 'Ambos':
            if 'cnae_principal' in df.columns and not df['cnae_principal'].empty:
                df['cnae_principal'] = df['cnae_principal'].astype(str)
                all_cnaes.extend(df['cnae_principal'].tolist())
            else:
                st.info("Coluna 'cnae_principal' não encontrada ou está vazia.")

        if cnae_type == 'CNAEs Secundários' or cnae_type == 'Ambos':
            if 'cnae_secundario' in df.columns and not df['cnae_secundario'].empty:
                exploded_cnaes = df['cnae_secundario'].dropna().astype(str).apply(lambda x: x.split('; ')).explode()
                all_cnaes.extend(exploded_cnaes.tolist())
            else:
                st.info("Coluna 'cnae_secundario' não encontrada ou está vazia.")
        
        if all_cnaes:
            cnae_counts = Counter(all_cnaes)
            top_n_cnae = st.slider("Número de CNAEs para exibir:", min_value=10, max_value=50, value=20, key="top_cnaes_slider_horizontal")
            top_cnaes = cnae_counts.most_common(top_n_cnae)
            df_top_cnaes = pd.DataFrame(top_cnaes, columns=['CNAE', 'Frequência'])

            df_top_cnaes = df_top_cnaes.sort_values('Frequência', ascending=False)

            fig_cnaes = px.bar(
                df_top_cnaes,
                x='Frequência',
                y='CNAE',
                orientation='h',
                title=f'Top {top_n_cnae} CNAEs Mais Frequentes ({cnae_type})',
                labels={'CNAE': 'CNAE (Descrição Completa)', 'Frequência': 'Contagem'},
                color='Frequência',
                color_continuous_scale=px.colors.sequential.Plasma,
                hover_data=[]
            )
            fig_cnaes.update_layout(yaxis={'categoryorder':'total descending'})

            st.plotly_chart(fig_cnaes, use_container_width=True)

            st.markdown(f"**Observação:** Esta análise exibe os {top_n_cnae} CNAEs mais comuns (principais e/ou secundários, dependendo da sua seleção). Os rótulos completos são visíveis diretamente no gráfico.")

            with st.expander("Ver dados brutos dos CNAEs"):
                st.dataframe(df_top_cnaes, use_container_width=True)
        else:
            st.info("Nenhum CNAE válido encontrado para análise. Verifique as colunas 'cnae_principal' e 'cnae_secundario'.")

    with tabs[3]:
        st.subheader("📊 Análise por Porte da Empresa")
        df_porte_counts = get_column_counts(df, 'porte_empresa')
        if not df_porte_counts.empty:
            fig_porte = px.pie(df_porte_counts, names='Porte da Empresa', values='Total', title='Empresas por Porte', template='seaborn')
            st.plotly_chart(fig_porte, use_container_width=True)
        else:
            st.info("Coluna 'porte_empresa' não encontrada ou está vazia.")

    with tabs[4]:
        st.subheader("📊 Análise por Situação Cadastral")
        df_situacao_counts = get_column_counts(df, 'situacao_cadastral')
        if not df_situacao_counts.empty:
            fig_situacao = px.bar(df_situacao_counts, x='Situação Cadastral', y='Total', color='Total', template='plotly_dark')
            st.plotly_chart(fig_situacao, use_container_width=True)
        else:
            st.info("Coluna 'situacao_cadastral' não encontrada ou está vazia.")

    with tabs[5]:
        st.subheader("📊 Análise por Faixa de Capital Social")
        df_cap_counts = get_column_counts(df, 'faixa_capital')
        if not df_cap_counts.empty:
            ordered_categories = ["Até 50k", "50k-100k", "100k-500k", "500k-1M", "1M-5M", "Acima de 5M"]
            df_cap_counts['Capital Social'] = pd.Categorical(df_cap_counts['Capital Social'], categories=ordered_categories, ordered=True)
            df_cap_counts = df_cap_counts.sort_values('Capital Social')

            st.plotly_chart(px.bar(df_cap_counts, x='Capital Social', y='Total', color='Total', title='Empresas por Faixa de Capital Social', template='plotly_dark'))
        else:
            st.info("Não foi possível gerar o gráfico de Capital Social. Verifique se a coluna 'capital_social' existe e possui dados válidos.")

    with tabs[6]:
        st.subheader("📊 Análise por Faixa de Idade da Empresa")
        df_idade_counts = get_column_counts(df, 'faixa_idade')
        if not df_idade_counts.empty:
            ordered_categories_idade = ["≤1", "1-2", "2-3", "3-5", "5-10", ">10"]
            df_idade_counts['Idade'] = pd.Categorical(df_idade_counts['Idade'], categories=ordered_categories_idade, ordered=True)
            df_idade_counts = df_idade_counts.sort_values('Idade')

            st.plotly_chart(px.bar(df_idade_counts, x='Idade', y='Total', color='Total', title='Empresas por Faixa de Idade', template='plotly_dark'))
        else:
            st.info("Não foi possível gerar o gráfico de Idade da Empresa. Verifique se a coluna 'data_inicio_atividade' existe e possui dados válidos.")

    with tabs[7]:
        st.subheader("📊 Análise por Qualificação do Sócio")
        df_q_counts = get_column_counts(df, 'qualificacao_socio')
        if not df_q_counts.empty:
            st.plotly_chart(px.bar(df_q_counts, x='Qualificação', y='Total', color='Total', title='Qualificação do Sócio', template='plotly_dark'))
        else:
            st.info("Coluna 'qualificacao_socio' não encontrada ou está vazia.")

    with tabs[8]:
        st.subheader("📊 Análise por Faixa Etária do Sócio")
        df_fe_counts = get_column_counts(df, 'faixa_etaria_socio')
        if not df_fe_counts.empty:
            st.plotly_chart(px.bar(df_fe_counts, x='Faixa Etária', y='Total', color='Total', title='Faixa Etária do Sócio', template='plotly_dark'))
        else:
            st.info("Coluna 'faixa_etaria_socio' não encontrada ou está vazia.")


# --- Layout do aplicativo ---
tab_consulta, tab_analise_grafica, tab_pesquisa_mercado = st.tabs(["Consulta Avançada de Empresas", "Análise Gráfica dos Dados Enriquecidos", "Pesquisa de Mercado (Novas Empresas)"])

with tab_consulta:
    st.header("📋 Filtros de Consulta")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("Filtros de Texto e CNAE")
        razao_social_input = st.text_input("Razão Social (termos separados por vírgula)", help="Ex: 'MERCADO, TRANSPORTES'", key="razao_social_input")
        nome_fantasia_input = st.text_input("Nome Fantasia (termos separados por vírgula)", help="Ex: 'mercado, padaria'", key="nome_fantasia_input")
        cnaes_input = st.text_input("CNAE (código ou descrição, termos separados por vírgula)", help="Ex: '4711, comércio varejista'", key="cnaes_input")
        cod_cnae_input = st.text_input("Códigos CNAE (somente numéricos, separados por vírgula)",help="Ex: '4711-3/02, 5611-2/01'",key="cod_cnae_input") #INCLUSÃO FILTRO CNAE
        natureza_juridica_input = st.text_input("Natureza Jurídica (código ou descrição, termos separados por vírgula)", help="Ex: '206-2, Sociedade Empresária Limitada'", key="natureza_juridica_input")
        opcao_simples = st.selectbox("Optante pelo Simples?",options=["", "S", "N"],index=0,help="Deixe em branco para não filtrar",key="opcao_simples")
        opcao_mei = st.selectbox("Optante pelo MEI?",options=["", "S", "N"],index=0,help="Deixe em branco para não filtrar",key="opcao_mei")

    with col2:
        st.subheader("Filtros de Localização")
        
        ufs = get_uf_options(engine)
        uf_selecionada = st.multiselect("UF", options=ufs, key="uf_selecionada")

        municipio_input = st.text_input("Município (termos separados por vírgula)", help="Ex: 'São Paulo, Rio de Janeiro'", key="municipio_input")

        bairro_input = st.text_input("Bairro (termos separados por vírgula)", help="Ex: 'Centro, Copacabana'", key="bairro_input")
        
        ddd_input = st.text_input("DDD (termos separados por vírgula)", help="Ex: '11, 21'", key="ddd_input")

        logradouro_input = st.text_input("Tipo de Logradouro (termos separados por vírgula)", help="Ex: 'Rua, Avenida'", key="logradouro_input")


    with col3:
        st.subheader("Filtros Numéricos e Outros")
        col_cap_min, col_cap_max = st.columns(2)
        with col_cap_min:
            capital_social_min = st.number_input("Capital Social Mínimo", min_value=0.0, value=None, format="%.2f", key="capital_social_min")
        with col_cap_max:
            capital_social_max = st.number_input("Capital Social Máximo", min_value=0.0, value=None, format="%.2f", key="capital_social_max")

        col_data_abertura_min, col_data_abertura_max = st.columns(2)
        with col_data_abertura_min:
            data_abertura_apos = st.date_input("Data Abertura Após", value=None, min_value=datetime.date(1900, 1, 1), key="data_abertura_apos")
        with col_data_abertura_max:
            data_abertura_antes = st.date_input("Data Abertura Antes", value=None, key="data_abertura_antes")

        col_idade_min, col_idade_max = st.columns(2)
        with col_idade_min:
            idade_min = st.number_input("Idade Mínima (anos)", min_value=0, value=None, format="%d", key="idade_min")
        with col_idade_max:
            idade_max = st.number_input("Idade Máxima (anos)", min_value=0, value=None, format="%d", key="idade_max")

        portes = get_porte_options()
        porte_selecionado = st.multiselect("Porte da Empresa", options=portes, key="porte_selecionado")

        qualificacao_socio_input = st.text_input("Qualificação Sócio (termos separados por vírgula)", help="Ex: 'Sócio-Administrador, Presidente'", key="qualificacao_socio_input")

        faixa_etaria_socio_input = st.text_input("Faixa Etária Sócio (termos separados por vírgula)", help="Ex: 'Entre 21 a 30 anos, Entre 31 a 40 anos'", key="faixa_etaria_socio_input")


    limit_resultados = st.slider("Número Máximo de Resultados (LIMIT)", min_value=1000, max_value=100000, value=5000, step=1000, key="limit_resultados")

    if st.button("🔎 Realizar Consulta", key="btn_realizar_consulta"):
        filtros = {
            'razao_social_termos': [t.strip() for t in razao_social_input.split(',') if t.strip()],
            'nome_fantasia_termos': [t.strip() for t in nome_fantasia_input.split(',') if t.strip()],
            'cnaes_termos': [t.strip() for t in cnaes_input.split(',') if t.strip()],
            'cod_cnae_termos': [t.strip() for t in cod_cnae_input.split(',') if t.strip()], #ADICIONADO
            'natureza_juridica_termos': [t.strip() for t in natureza_juridica_input.split(',') if t.strip()],
            'opcao_simples': opcao_simples,
            'opcao_mei': opcao_mei,
            'uf_selecionada': uf_selecionada,
            'municipio_termos': [t.strip() for t in municipio_input.split(',') if t.strip()],
            'bairro_termos': [t.strip() for t in bairro_input.split(',') if t.strip()],
            'ddd_termos': [t.strip() for t in ddd_input.split(',') if t.strip()],
            'logradouro_termos': [t.strip() for t in logradouro_input.split(',') if t.strip()],
            'capital_social_min': capital_social_min,
            'capital_social_max': capital_social_max,
            'data_abertura_apos': data_abertura_apos,
            'data_abertura_antes': data_abertura_antes,
            'idade_min': idade_min,
            'idade_max': idade_max,
            'porte_selecionado': porte_selecionado,
            'qualificacao_socio_termos': [t.strip() for t in qualificacao_socio_input.split(',') if t.strip()],
            'faixa_etaria_socio_termos': [t.strip() for t in faixa_etaria_socio_input.split(',') if t.strip()]
        }

        sql_final = montar_sql(filtros, limit_resultados)
        st.session_state.query_sql_display = sql_final
        
        with st.spinner("Buscando dados no banco de dados..."):
            df_resultados = run_query(sql_final, engine)
            st.session_state.df_cnpjs = df_resultados
            st.success(f"Consulta concluída! {len(df_resultados)} resultados encontrados.")

        # Limpar o cache das funções dependentes do DataFrame principal
        process_dataframe_for_analysis.clear()
        get_word_counts.clear()
        get_column_counts.clear()
        get_cnae_counts_for_market_research.clear() # Limpa o cache da nova função também

    if st.session_state.query_sql_display:
        with st.expander("Ver a query SQL gerada", expanded=False):
            st.code(st.session_state.query_sql_display, language="sql")

    if st.session_state.df_cnpjs is not None and not st.session_state.df_cnpjs.empty:
        st.markdown("### 📋 Resultados da Consulta")
        #st.dataframe(st.session_state.df_cnpjs, use_container_width=True) --se ativo mostra todas as colunas do Dataframe
        colunas_para_omitir = [
            'razao_social_normalizado',
            'nome_fantasia_normalizado',
            'municipio_normalizado',
            'bairro_normalizado',
            'uf_normalizado',
            'cnae_principal_normalizado',
            'cnae_secundario_normalizado'  # adicione outras colunas se necessário
        ]

        df_visivel = st.session_state.df_cnpjs.drop(columns=[c for c in colunas_para_omitir if c in st.session_state.df_cnpjs.columns])

        st.dataframe(df_visivel, use_container_width=True)


        total_cnpjs_distintos = st.session_state.df_cnpjs['cnpj'].nunique()
        st.markdown(f"**🔢 Total de CNPJs distintos encontrados:** {total_cnpjs_distintos:,}")

        st.download_button(
            label="📥 Baixar Resultados (.csv)",
            data=st.session_state.df_cnpjs.to_csv(index=False).encode("utf-8"),
            file_name="resultados_consulta_cnpj.csv",
            mime="text/csv",
            key="download_resultados_consulta"
        )
    elif st.session_state.df_cnpjs is not None and st.session_state.df_cnpjs.empty and st.session_state.query_sql_display:
        st.info("Nenhum resultado encontrado para os filtros selecionados.")


with tab_analise_grafica:
    etapa2()

def validate_n_meses(n_meses_str):
    try:
        n_meses = int(n_meses_str)
        if 1 <= n_meses <= 120:
            return n_meses
        else:
            st.error("Por favor, insira um número entre 1 e 120 para os meses de análise.")
            return None
    except ValueError:
        st.error("Por favor, insira um número válido para os meses de análise.")
        return None

# --- Início da seção da aba de Pesquisa de Mercado ---
# Certifique-se de que 'tab_pesquisa_mercado' está definido corretamente,
# por exemplo, como uma das abas retornadas por st.tabs().
# Ex: with tabs[X]: onde X é o índice da sua aba "Pesquisa de Mercado"
# Substitua 'tab_pesquisa_mercado' pelo nome real da sua aba/bloco.
# Se estiver no final do seu '2_Analise_Grafica.py' e for a última parte,
# pode remover o 'with tab_pesquisa_mercado:' e colocar o conteúdo diretamente.

# Adaptei para um placeholder que você deve ajustar ao seu layout:
# Se 'Pesquisa de Mercado' é uma aba:
# with tabs[índice_da_sua_aba_pesquisa_de_mercado]:
# Ou se é um bloco independente:
# st.header("📈 Gerar Pesquisa de Mercado") # Exemplo, ajuste como preferir
# Para este exemplo, vou manter o 'with' para indicar que é um bloco contido.

# Se 'tab_pesquisa_mercado' é uma variável que contém um objeto de aba (por exemplo, de st.tabs()):
# with tab_pesquisa_mercado:
# Ou se for um módulo separado ou apenas um cabeçalho, ajuste conforme seu código.
# Estou usando um placeholder 'minha_tab_pesquisa_mercado' para ilustrar a estrutura.
# Por favor, ajuste para como sua aba 'Pesquisa de Mercado' está sendo acessada.
# Por exemplo, se for `with tabs[8]:` (assumindo que seja a nona aba), use `with tabs[8]:`

# Para garantir que o código seja copiado e colado facilmente, vou remover
# o 'with tab_pesquisa_mercado:' e assumir que o usuário o inserirá no contexto correto
# (provavelmente dentro de uma função `def etapaX():` ou diretamente no script principal)

with tab_pesquisa_mercado:
    st.header("📈 Pesquisa de Mercado (Novas Empresas)")

    st.sidebar.markdown("---")
    st.sidebar.subheader("Filtros Dedicados para Pesquisa de Mercado")

    n_meses_analise_str = st.sidebar.text_input(
        "Meses para Análise de Crescimento (1-120):",
        value="24",
        key="input_meses_analise_pesquisa"
    )
    n_meses_analise = validate_n_meses(n_meses_analise_str)

    filtro_uf_pesquisa = st.sidebar.multiselect(
        "Filtrar por UF:",
        options=st.session_state.df_cnpjs['uf'].unique().tolist() if 'df_cnpjs' in st.session_state and st.session_state.df_cnpjs is not None and 'uf' in st.session_state.df_cnpjs.columns else [],
        key="filtro_uf_pesquisa"
    )
    filtro_municipio_pesquisa = st.sidebar.multiselect(
        "Filtrar por Município:",
        options=st.session_state.df_cnpjs['municipio'].unique().tolist() if 'df_cnpjs' in st.session_state and st.session_state.df_cnpjs is not None and 'municipio' in st.session_state.df_cnpjs.columns else [],
        key="filtro_municipio_pesquisa"
    )
    filtro_nome_fantasia_pesquisa = st.sidebar.text_input(
        "Palavras-chave no Nome Fantasia (opcional):",
        help="Separe múltiplos termos com vírgula.",
        key="filtro_nome_fantasia_pesquisa"
    )

    coluna_agrupamento = "bairro"
    agrupar_por_analise = "Bairro"

    if st.sidebar.button("🚀 Gerar Pesquisa de Mercado", key="btn_gerar_pesquisa"):
        if n_meses_analise is None:
            st.error("Por favor, corrija os erros nos campos de filtro.")
        elif not filtro_uf_pesquisa:
            st.error("Por favor, selecione pelo menos um estado (UF).")
        elif not filtro_municipio_pesquisa:
            st.error("Por favor, selecione pelo menos um município.")
        else:
            st.session_state.resumo_crescimento = None
            st.session_state.df_oportunidades = None

            data_limite = datetime.date.today() - datetime.timedelta(days=n_meses_analise * 30)

            dedicated_filters_sql_clause = []

            if filtro_uf_pesquisa:
                ufs_str = "', '".join(filtro_uf_pesquisa)
                dedicated_filters_sql_clause.append(f"uf_normalizado IN ('{ufs_str}')")

            if filtro_municipio_pesquisa:
                municipios_str = "', '".join(filtro_municipio_pesquisa)
                dedicated_filters_sql_clause.append(f"municipio_normalizado IN ('{municipios_str}')")

            if filtro_nome_fantasia_pesquisa:
                nomes_fantasia_termos = [f"%{termo.strip()}%" for termo in filtro_nome_fantasia_pesquisa.split(',')]
                termos_ilike = [f"nome_fantasia_normalizado ILIKE '{termo}'" for termo in nomes_fantasia_termos if termo.strip()]
                dedicated_filters_sql_clause.append(f"({' OR '.join(termos_ilike)})")

            final_dedicated_filter_sql = ""
            if dedicated_filters_sql_clause:
                final_dedicated_filter_sql = " AND " + " AND ".join(dedicated_filters_sql_clause)

            sql_crescimento = f"""
            SELECT
                uf_normalizado AS uf,
                municipio_normalizado AS municipio,
                {coluna_agrupamento}_normalizado AS {coluna_agrupamento},
                COUNT(cnpj) AS total_empresas
            FROM
                {TABELA}
            WHERE
                data_inicio_atividade >= '{data_limite.strftime('%Y-%m-%d')}'
                AND situacao_cadastral = 'ATIVA'
                AND {coluna_agrupamento}_normalizado IS NOT NULL
                {final_dedicated_filter_sql}
            GROUP BY
                {coluna_agrupamento}_normalizado, uf_normalizado, municipio_normalizado
            ORDER BY
                total_empresas DESC
            LIMIT 10000;
            """

            st.session_state.query_sql_display_crescimento = sql_crescimento

            with st.spinner(f"Analisando crescimento nos últimos {n_meses_analise} meses..."):
                try:
                    raw_df_crescimento = run_query(sql_crescimento, engine)
                    st.session_state.resumo_crescimento = raw_df_crescimento
                    st.success("Análise de crescimento concluída!")
                except Exception as e:
                    st.error(f"Erro ao gerar a pesquisa de mercado: {e}")
                    st.session_state.resumo_crescimento = pd.DataFrame()

    if 'query_sql_display_crescimento' in st.session_state and st.session_state.query_sql_display_crescimento:
        with st.expander("Ver a query SQL de Crescimento gerada", expanded=False):
            st.code(st.session_state.query_sql_display_crescimento, language="sql")

    if 'resumo_crescimento' in st.session_state and st.session_state.resumo_crescimento is not None and not st.session_state.resumo_crescimento.empty:
        plot_col_name = "bairro"
        value_col_name = "total_empresas"

        st.markdown(f"### 📊 Top 30 Novas Empresas por {agrupar_por_analise} (últimos {n_meses_analise} meses)")

        resumo_plot_df = st.session_state.resumo_crescimento.head(30)

        fig_crescimento = px.bar(
            resumo_plot_df,
            x=value_col_name,
            y=plot_col_name,
            orientation='h',
            title=f"Top 30 Novas Empresas por {agrupar_por_analise} (últimos {n_meses_analise} meses)",
            labels={value_col_name: "Número de Novas Empresas", plot_col_name: agrupar_por_analise},
            hover_data=[]
        )
        fig_crescimento.update_layout(yaxis={'categoryorder': 'total descending'})
        fig_crescimento.update_xaxes(automargin=True)

        st.plotly_chart(fig_crescimento, use_container_width=True)

        total_empresas_encontradas = resumo_plot_df[value_col_name].sum()
        st.markdown(f"**🔢 Total de novas empresas encontradas:** {total_empresas_encontradas:,}")

        with st.expander("📍 Resultado Detalhado da Pesquisa de Mercado", expanded=False):
            st.markdown(f"### 📊 Crescimento por {agrupar_por_analise} (últimos {n_meses_analise} meses)")
            st.dataframe(st.session_state.resumo_crescimento, use_container_width=True)
            st.download_button(
                label=f"📥 Baixar Crescimento por {agrupar_por_analise} (.csv)",
                data=st.session_state.resumo_crescimento.to_csv().encode("utf-8"),
                file_name=f"crescimento_empresas_{agrupar_por_analise}.csv",
                mime="text/csv",
                key="download_resultados_crescimento"
            )
    elif 'resumo_crescimento' in st.session_state and st.session_state.resumo_crescimento is not None and st.session_state.resumo_crescimento.empty:
        st.info(f"Nenhuma nova empresa encontrada para os filtros e período selecionados ({n_meses_analise} meses).")
    else:
        st.info("Gere a pesquisa de mercado usando os filtros na barra lateral para ver os resultados aqui.")
