import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from unidecode import unidecode
import datetime
from io import BytesIO
import plotly.express as px
import plotly.graph_objects as go

# Configurações iniciais
st.set_page_config(layout="wide", page_title="Consulta Avançada de CNPJs + Pesquisa de Mercado")
st.title("🔍 Consulta Avançada de Empresas com Filtros SQL")

# Conexão com banco de dados
# ATENÇÃO: Substitua pelos seus dados de conexão reais
DATABASE_URL = "postgresql+psycopg2://postgres:0804Bru%21%40%23%24@localhost:5432/empresas"
engine = create_engine(DATABASE_URL)
TABELA = "visao_empresa_completa"

# Função para montar a query SQL principal
def montar_sql(filtros, limit):
    where = ["situacao_cadastral = 'ATIVA'"]

    def clausulas_ilike(campo, termos):
        termos_validos = [t.strip() for t in termos if t.strip()]
        if not termos_validos:
            return []
        return [f"unaccent({campo}) ILIKE unaccent('%{t}%')" for t in termos_validos]

    def clausula_in(campo, lista):
        condicoes = [f"{campo} = '{x}'" for x in lista if x.strip()]
        if not condicoes:
            return ""
        return f"({' OR '.join(condicoes)})"

    # Processamento de campos de texto (nome_fantasia, municipio, etc.)
    for campo in ['nome_fantasia', 'municipio', 'cnae_principal', 'cnae_secundario', 'bairro', 'nome_socio_razao_social']:
        termos_para_ilike = [t.strip() for t in filtros.get(campo, []) if t.strip()]
        if termos_para_ilike:
            where.append(f"({' OR '.join(clausulas_ilike(campo, termos_para_ilike))})")

    # Processamento de campos multi-selecionados (UF, Porte, etc.)
    for campo in ['uf', 'porte_empresa', 'opcao_simples', 'opcao_mei', 'qualificacao_socio', 'faixa_etaria_socio']:
        lista_para_in = [item for item in filtros.get(campo, []) if item != "TODOS" and item.strip()]
        if lista_para_in:
            where.append(clausula_in(campo, lista_para_in))

    # Capital social: Inclui se os valores forem diferentes dos defaults
    if filtros.get("capital_min") is not None and filtros.get("capital_max") is not None:
         if filtros["capital_min"] != 0.0 or filtros["capital_max"] != 1000000.0:
            where.append(f"capital_social BETWEEN {filtros['capital_min']} AND {filtros['capital_max']}")

    # Data de Início de Atividade: Sempre incluído, já que st.date_input sempre tem um valor
    if filtros.get("data_inicio_inicio") and filtros.get("data_inicio_fim"):
        ini = filtros["data_inicio_inicio"].strftime('%Y-%m-%d')
        fim = filtros["data_inicio_fim"].strftime('%Y-%m-%d')
        where.append(f"data_inicio_atividade BETWEEN '{ini}' AND '{fim}'")
    
    if not where:
        where.append("1=1") # Fallback para evitar query WHERE vazia

    sql = f"SELECT * FROM {TABELA} WHERE {' AND '.join(where)} LIMIT {limit}"
    return sql

# --- FUNÇÃO ÚNICA PARA EXECUTAR CONSULTA E ANÁLISE ---
def executar_tudo():
    # 1. Coletar todos os valores dos widgets do Streamlit via session_state
    current_filtros = {
        "nome_fantasia": st.session_state.get("nome_fantasia_input", "").upper().split(','),
        "cnae_principal": st.session_state.get("cnae_principal_input", "").upper().split(','),
        "cnae_secundario": st.session_state.get("cnae_secundario_input", "").upper().split(','),
        "porte_empresa": st.session_state.get("porte_empresa_select", ["TODOS"]),
        "opcao_simples": st.session_state.get("opcao_simples_select", []),
        "opcao_mei": st.session_state.get("opcao_mei_select", []),
        "nome_socio_razao_social": st.session_state.get("nome_socio_razao_social_input", "").upper().split(','),
        "qualificacao_socio": st.session_state.get("qualificacao_socio_select", []),
        "faixa_etaria_socio": st.session_state.get("faixa_etaria_socio_select", []),
        "data_inicio_inicio": st.session_state.get("data_inicio_inicio_input", datetime.date(2000, 1, 1)),
        "data_inicio_fim": st.session_state.get("data_inicio_fim_input", datetime.date.today()),
        "capital_min": st.session_state.get("capital_min_input", 0.0),
        "capital_max": st.session_state.get("capital_max_input", 1000000.0),
        "uf": st.session_state.get("uf_select", ["TODOS"]),
        "municipio": st.session_state.get("municipio_input", "").upper().split(','),
        "bairro": st.session_state.get("bairro_input", "").upper().split(','),
    }
    
    current_limit = st.session_state.get("limit_input", 1000)
    n_meses_analise = st.session_state.get("n_meses_input", 6)
    agrupar_por_analise = st.session_state.get("agrupar_por_input", "bairro")

    # 2. Montar e Executar a Consulta SQL Principal
    sql_query = montar_sql(current_filtros, current_limit)
    st.session_state.df = pd.DataFrame() # Limpa o df anterior
    st.session_state.df_resumo_crescimento = pd.DataFrame() # Limpa o resumo de crescimento
    st.session_state.executar = False # Reseta o estado de execução

    with st.spinner("Buscando dados e realizando análises..."):
        with engine.connect() as conn:
            try:
                df_principal = pd.read_sql(text(sql_query), conn)
                st.session_state.df = df_principal.copy() # Armazena uma cópia limpa no session_state
                st.session_state.sql_query = sql_query
                st.session_state.executar = True
                st.success("Consulta executada e dados carregados com sucesso!")

                # 3. Realizar Análise de Crescimento sobre o df_principal
                if not df_principal.empty:
                    df_crescimento_base = df_principal.copy() # Cópia para operações de crescimento
                    
                    data_limite_crescimento = datetime.date.today() - pd.DateOffset(months=n_meses_analise)
                    
                    # Filtra o DataFrame principal para a análise de crescimento
                    df_crescimento_filtrado = df_crescimento_base[
                        pd.to_datetime(df_crescimento_base["data_inicio_atividade"], errors="coerce") >= data_limite_crescimento
                    ].copy() # Crie uma cópia após filtrar para otimizar as operações subsequentes

                    if not df_crescimento_filtrado.empty:
                        if agrupar_por_analise not in df_crescimento_filtrado.columns:
                            st.warning(f"A coluna '{agrupar_por_analise}' não está presente nos dados filtrados para agrupamento de crescimento. A análise de crescimento pode estar incompleta.")
                        else:
                            df_crescimento_filtrado["ano_mes"] = pd.to_datetime(df_crescimento_filtrado["data_inicio_atividade"], errors="coerce").dt.to_period("M").astype(str)
                            df_crescimento_filtrado = df_crescimento_filtrado.dropna(subset=['ano_mes'])

                            resumo_crescimento = df_crescimento_filtrado.groupby([agrupar_por_analise, "ano_mes"]).size().reset_index(name="qtd")
                            
                            # Para garantir que todos os meses no intervalo sejam considerados, mesmo que sem dados
                            hoje_period = pd.to_datetime(datetime.date.today()).to_period('M')
                            start_period = pd.to_datetime(data_limite_crescimento).to_period('M')
                            all_months_in_range = pd.period_range(start=start_period, end=hoje_period, freq='M').astype(str).tolist()

                            resumo_pivot = resumo_crescimento.pivot_table(index=agrupar_por_analise, columns="ano_mes", values="qtd", fill_value=0).astype(int)
                            
                            # Adicionar colunas de meses ausentes com 0
                            for month in all_months_in_range:
                                if month not in resumo_pivot.columns:
                                    resumo_pivot[month] = 0
                            
                            # Ordenar as colunas de meses cronologicamente
                            resumo_pivot = resumo_pivot[sorted([col for col in resumo_pivot.columns if col in all_months_in_range])] # Apenas meses relevantes

                            resumo_pivot["total_empresas"] = resumo_pivot.sum(axis=1)
                            resumo_ordenado_crescimento = resumo_pivot[["total_empresas"]].sort_values(by="total_empresas", ascending=False)
                            
                            st.session_state.df_resumo_crescimento = resumo_ordenado_crescimento
                            st.session_state.agrupar_por_analise = agrupar_por_analise
                            st.session_state.n_meses_analise = n_meses_analise
                            st.success("Análise de crescimento concluída!")
                    else:
                        st.info("Nenhum registro encontrado para a análise de crescimento com os filtros e período selecionados.")
                else:
                    st.info("Nenhum registro encontrado na consulta principal para realizar análises.")

            except Exception as e:
                st.error(f"Erro ao executar a consulta SQL: {e}")
                st.code(sql_query, language="sql")
                st.session_state.executar = False
                st.session_state.df = pd.DataFrame() # Limpa o DataFrame em caso de erro
                st.session_state.df_resumo_crescimento = pd.DataFrame() # Limpa o df de crescimento também

# Estado inicial para df e executar (permanece o mesmo)
if 'executar' not in st.session_state:
    st.session_state.executar = False
if 'df' not in st.session_state:
    st.session_state.df = pd.DataFrame()
if 'df_resumo_crescimento' not in st.session_state:
    st.session_state.df_resumo_crescimento = pd.DataFrame()


# --- FORMULÁRIO PRINCIPAL ---
with st.form("formulario_filtros"):
    st.subheader("📁 Dados Cadastrais da Empresa")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.text_input("Nome Fantasia", key="nome_fantasia_input")
    with col2:
        st.text_input("CNAE Principal", key="cnae_principal_input")
    with col3:
        st.text_input("CNAE Secundário", key="cnae_secundario_input")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.multiselect("Porte da Empresa", ["TODOS", "NÃO INFORMADO", "MICRO EMPRESA", "EMPRESA DE PEQUENO PORTE", "DEMAIS"], default=["TODOS"], key="porte_empresa_select")
    with col2:
        st.multiselect("Opção Simples", ["S", "N"], key="opcao_simples_select")
    with col3:
        st.multiselect("Opção MEI", ["S", "N"], key="opcao_mei_select")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.text_input("Nome do Sócio ou Razão Social", key="nome_socio_razao_social_input")
    with col2:
        st.multiselect("Qualificação do Sócio", ["Sócio-Administrador", "Sócio", "Presidente", "Diretor","Administrador", "Procurador", "Tesoureiro","Sócio Pessoa Jurídica Domiciliado no Brasil","Produtor Rural"], key="qualificacao_socio_select")
    with col3:
        st.multiselect("Faixa Etária do Sócio", [
                "Não se aplica", "Entre 0 a 12 anos", "Entre 13 a 20 anos", "Entre 21 a 30 anos",
                "Entre 31 a 40 anos", "Entre 41 a 50 anos", "Entre 51 a 60 anos", "Entre 61 a 70 anos",
                "Entre 71 a 80 anos", "Maior de 80 anos"], key="faixa_etaria_socio_select")

    col1, col2, col3, col4 = st.columns(4)
    hoje = datetime.date.today()
    with col1:
        st.date_input("Data Inicial", value=datetime.date(2000, 1, 1), max_value=hoje, key="data_inicio_inicio_input")
    with col2:
        st.date_input("Data Final", value=hoje, max_value=hoje, key="data_inicio_fim_input")
    with col3:
        st.number_input("Capital Social Mínimo", min_value=0.0, value=0.0, key="capital_min_input")
    with col4:
        st.number_input("Capital Social Máximo", min_value=0.0, value=1000000.0, key="capital_max_input")

    st.subheader("🌎 Dados de Localidade")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.multiselect("UF", ["TODOS", "SC", "RS", "SP", "RJ"], default=["TODOS"], key="uf_select")
    with col2:
        st.text_input("Município", key="municipio_input")
    with col3:
        st.text_input("Bairro", key="bairro_input")

    st.subheader("📌 Tipos de Busca")
    col1, col2, col3 = st.columns(3) # Três colunas para os campos de busca/análise
    with col1:
        st.number_input("Quantidade máxima de registros (LIMIT)", min_value=1000, max_value=100000, value=1000, key="limit_input")
    with col2:
        st.number_input("Analisar crescimento nos últimos N meses:", min_value=3, max_value=60, value=6, step=1, key="n_meses_input")
    with col3:
        st.selectbox("Agrupar Análise de Crescimento por", ["bairro", "cep"], key="agrupar_por_input") # Agrupar por agora para a análise de crescimento

    st.form_submit_button(
        "🚀 Executar Consulta e Análise Completa",
        on_click=executar_tudo # Um único botão para tudo
    )

# --- EXIBIÇÃO DOS RESULTADOS PRINCIPAIS (MESMO CÓDIGO ANTERIOR) ---
if st.session_state.executar and not st.session_state.df.empty:
    df = st.session_state.df
    st.markdown("---")
    with st.expander("📄 Registros Encontrados", expanded=True):
        st.code(st.session_state.sql_query, language="sql")
        st.success(f"{len(df)} registros encontrados.")
        st.dataframe(df)
        st.download_button(
            label="📥 Baixar Resultados (.csv)",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name="resultados_consulta_principal.csv",
            mime="text/csv",
            key="download_resultados_consulta"
        )

    with st.expander("📊 Análise Gráfica dos Registros Principais"):
        aba = st.tabs(["CNAE Principal", "CNAE Secundário", "Localização", "Capital Social", "Idade da Empresa", "Qualificação do Sócio", "Faixa Etária do Sócio"])
        with aba[0]:
            if 'cnae_principal' in df.columns and not df['cnae_principal'].empty:
                cnae = df['cnae_principal'].value_counts().nlargest(15).reset_index()
                cnae.columns = ['CNAE', 'Quantidade']
                st.plotly_chart(px.bar(cnae, x='CNAE', y='Quantidade', title='Top 15 CNAEs Principais'))
            else:
                st.info("Nenhum dado de CNAE Principal para análise.")
        with aba[1]:
            if 'cnae_secundario' in df.columns and not df['cnae_secundario'].empty:
                cnae2 = df['cnae_secundario'].value_counts().nlargest(15).reset_index()
                cnae2.columns = ['CNAE', 'Quantidade']
                st.plotly_chart(px.bar(cnae2, x='CNAE', y='Quantidade', title='Top 15 CNAEs Secundários'))
            else:
                st.info("Nenhum dado de CNAE Secundário para análise.")
        with aba[2]:
            # --- ALTERAÇÃO AQUI: GRÁFICO DE BARRAS PARA LOCALIZAÇÃO ---
            if 'bairro' in df.columns and not df['bairro'].empty:
                # Obtenha os 15 bairros com mais registros
                bairro_counts = df['bairro'].value_counts()
                top_n_bairros = 15 # Podemos tornar isso um input do usuário se desejar

                # Verifique se há mais de N bairros para agrupar o restante como "Outros"
                if len(bairro_counts) > top_n_bairros:
                    top_bairros_df = bairro_counts.nlargest(top_n_bairros).reset_index()
                    top_bairros_df.columns = ['Localização', 'Quantidade']
                    
                    # Calcule a soma dos "Outros"
                    outros_count = bairro_counts.nsmallest(len(bairro_counts) - top_n_bairros).sum()
                    outros_df = pd.DataFrame([{'Localização': 'Outros', 'Quantidade': outros_count}])
                    
                    # Concatene o top N com "Outros"
                    plot_df = pd.concat([top_bairros_df, outros_df], ignore_index=True)
                else:
                    # Se houver N ou menos bairros, mostre todos
                    plot_df = bairro_counts.reset_index()
                    plot_df.columns = ['Localização', 'Quantidade']

                fig_loc = px.bar(
                    plot_df.sort_values('Quantidade', ascending=False), # Ordena para melhor visualização
                    x='Localização',
                    y='Quantidade',
                    title=f'Top {top_n_bairros} Localidades (Bairros) com Mais Empresas'
                )
                st.plotly_chart(fig_loc)
            else:
                st.info("Nenhum dado de Localização (Bairro) para análise.")
        with aba[3]:
            if 'capital_social' in df.columns and not df['capital_social'].empty:
                def faixa_capital(valor):
                    if valor < 10000: return 'Abaixo de 10 mil'
                    elif valor < 50000: return '10 a 50 mil'
                    elif valor < 100000: return '50 a 100 mil'
                    elif valor < 300000: return '100 a 300 mil'
                    elif valor < 500000: return '300 a 500 mil'
                    elif valor < 1000000: return '500 mil a 1 mi'
                    else: return 'Acima de 1 mi'
                df['faixa_capital'] = df['capital_social'].apply(faixa_capital)
                faixas = df['faixa_capital'].value_counts().reset_index()
                faixas.columns = ['Faixa', 'Quantidade']
                
                ordem_faixas_capital = [
                    'Abaixo de 10 mil',
                    '10 a 50 mil',
                    '50 a 100 mil',
                    '100 a 300 mil',
                    '300 a 500 mil',
                    '500 mil a 1 mi',
                    'Acima de 1 mi'
                ]
                fig_capital = px.bar(
                    faixas,
                    x='Faixa',
                    y='Quantidade',
                    title='Distribuição do Capital Social',
                    category_orders={"Faixa": ordem_faixas_capital} # Aplica a ordem personalizada
                )
                st.plotly_chart(fig_capital)
            else:
                st.info("Nenhum dado de Capital Social para análise.")
                
        with aba[4]:
            if 'data_inicio_atividade' in df.columns and not df['data_inicio_atividade'].empty:
                hoje_pandas = pd.to_datetime("today")
                def faixa_idade(data):
                    if pd.isnull(data): return 'Desconhecida'
                    idade = (hoje_pandas - data).days / 365.25
                    if idade <= 1: return 'Até 1 ano'
                    elif idade <= 2: return '1 a 2 anos'
                    elif idade <= 3: return '2 a 3 anos'
                    elif idade <= 5: return '3 a 5 anos'
                    elif idade <= 10: return '5 a 10 anos'
                    else: return '10+ anos'
                df['idade_empresa'] = pd.to_datetime(df['data_inicio_atividade'], errors='coerce').apply(faixa_idade)
                idade_df = df['idade_empresa'].value_counts().reset_index()
                idade_df.columns = ['Faixa', 'Quantidade']
                st.plotly_chart(px.bar(idade_df.sort_values(by='Faixa'), x='Faixa', y='Quantidade'))
            else:
                st.info("Nenhum dado de Data de Início de Atividade para análise.")
        with aba[5]:
            if 'qualificacao_socio' in df.columns and not df['qualificacao_socio'].empty:
                q_df = df['qualificacao_socio'].value_counts().reset_index()
                q_df.columns = ['Qualificação', 'Quantidade']
                st.plotly_chart(px.bar(q_df, x='Qualificação', y='Quantidade'))
            else:
                st.info("Nenhum dado de Qualificação do Sócio para análise.")
        with aba[6]:
            if 'faixa_etaria_socio' in df.columns and not df['faixa_etaria_socio'].empty:
                f_df = df['faixa_etaria_socio'].value_counts().reset_index()
                f_df.columns = ['Faixa Etária', 'Quantidade']
                st.plotly_chart(px.bar(f_df, x='Faixa Etária', y='Quantidade'))
            else:
                st.info("Nenhum dado de Faixa Etária do Sócio para análise.")

# --- EXIBIÇÃO DA ANÁLISE DE CRESCIMENTO (AGORA BASEADA EM UMA ÚNICA CONSULTA) ---
if st.session_state.executar and not st.session_state.df_resumo_crescimento.empty:
    resumo_ordenado = st.session_state.df_resumo_crescimento
    agrupar_por_analise = st.session_state.agrupar_por_analise
    n_meses_analise = st.session_state.n_meses_analise

    st.markdown("---")
    st.subheader("📈 Estudo de Crescimento de Empresas")

    fig_crescimento = px.bar(
        resumo_ordenado.reset_index().head(30),
        x=agrupar_por_analise,
        y="total_empresas",
        title=f"Top 30 Locais com Mais Novas Empresas por {agrupar_por_analise.upper()} (últimos {n_meses_analise} meses)",
        labels={"total_empresas": "Número de Novas Empresas"}
    )
    st.plotly_chart(fig_crescimento, use_container_width=True)

    with st.expander("📍 Resultado Detalhado da Pesquisa de Mercado", expanded=False):
        st.markdown(f"### 📊 Crescimento por {agrupar_por_analise.upper()} (últimos {n_meses_analise} meses)")
        # A query SQL já foi exibida acima, não precisa duplicar
        st.dataframe(resumo_ordenado.head(30), use_container_width=True)
        st.download_button(
            label=f"📥 Baixar Crescimento por {agrupar_por_analise.upper()} (.csv)",
            data=resumo_ordenado.to_csv().encode("utf-8"),
            file_name=f"crescimento_empresas_{agrupar_por_analise}.csv",
            mime="text/csv",
            key="download_resultados_crescimento"
        )
else:
    # Mensagem informativa se a análise de crescimento ainda não foi gerada ou está vazia
    if st.session_state.executar and st.session_state.df.empty:
        st.info("Nenhum dado disponível para análise de crescimento, pois a consulta principal não retornou registros.")
    elif st.session_state.executar:
        st.info("Nenhum dado de crescimento encontrado com os filtros e período selecionados.")