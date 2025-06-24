import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from io import BytesIO
import re
from collections import Counter
from unidecode import unidecode
from datetime import datetime, timedelta

st.set_page_config(layout="wide", page_title="IA de Geração de Leads")
st.title("🤖 IA Generator: Encontre Novos Leads")

DATABASE_URL = "postgresql+psycopg2://postgres:0804Bru%21%40%23%24@localhost:5432/empresas"

PONTUACAO_PARAMETROS = {
    'nome_fantasia': 10,
    'uf': 5,
    'municipio': 5,
    'bairro': 10,
    'cod_cnae_principal': 10,
    'cod_cnae_secundario': 10,
    'data_inicio_atividade': 5,
    'capital_social': 5,
    'porte_empresa': 5,
    'natureza_juridica': 5,
    'opcao_simples': 5,
    'opcao_mei': 5,
    'situacao_cadastral': 0,
    'ddd1': 10,
    'nome_socio_razao_social': 0,
    'qualificacao_socio': 5,
    'faixa_etaria_socio': 5
}

# Adicione @st.cache_data às funções que processam df_clientes
@st.cache_data
def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# MODIFICADO: Função para incluir opção de NULL/Vazio separadamente e garantir que não são cortados pelo top_n
@st.cache_data
def get_unique_values(df, column, top_n=None, include_null=False, include_empty=False):
    if column not in df.columns:
        return []

    temp_series = df[column].astype(str) # Garante que tudo é string
    
    unique_values_list = []

    # Adiciona valores não nulos/vazios
    actual_values = temp_series[~temp_series.isna() & (temp_series.str.strip() != "")].tolist()
    if actual_values:
        counts = pd.Series(actual_values).value_counts()
        # Adiciona os top_n valores reais primeiro
        unique_values_list.extend(counts.index.tolist())
    
    # Adiciona (Nulo) e (Vazio) *sempre* se as flags estiverem ativas e houver ocorrências
    if include_null and df[column].isna().any():
        unique_values_list.append("(Nulo)")
    if include_empty and (df[column].astype(str).str.strip() == "").any(): # Verifica strings vazias
        unique_values_list.append("(Vazio)")

    # Aplica top_n APENAS no final, se especificado, depois de adicionar os especiais
    if top_n and len(unique_values_list) > top_n:
        # Prioriza manter (Nulo) e (Vazio) se presentes e os top_n
        filtered_list = unique_values_list[:top_n]
        if "(Nulo)" in unique_values_list and "(Nulo)" not in filtered_list:
            filtered_list.append("(Nulo)")
        if "(Vazio)" in unique_values_list and "(Vazio)" not in filtered_list:
            filtered_list.append("(Vazio)")
        return list(dict.fromkeys(filtered_list)) # Remove duplicatas e mantém ordem
    return unique_values_list

# MODIFICADO: Função para incluir opção de NULL/Vazio para palavras-chave (nome fantasia) e garantir que não são cortados pelo top_n
@st.cache_data
def get_top_n_words(df, column, top_n, stop_words, include_null=False, include_empty=False):
    if column not in df.columns:
        return []

    all_words = []
    
    def clean_and_tokenize(text):
        if pd.isna(text) or str(text).strip() == "":
            return []
        text = unidecode(str(text).strip().lower())
        text = re.sub(r'[^\w\s]', '', text)
        text = re.sub(r'\d+', '', text)
        words = [word for word in text.split() if word not in stop_words and len(word) > 1]
        return words

    # Processa apenas os valores válidos para extrair palavras
    # Filtra explicitamente NaN e strings vazias antes de aplicar a função de limpeza
    temp_series_non_null_empty = df[column][df[column].notna() & (df[column].astype(str).str.strip() != "")].apply(clean_and_tokenize).explode()
    all_words.extend(temp_series_non_null_empty.dropna().tolist())

    # Contagem de palavras reais
    word_counts = Counter(all_words)
    common_words = [word for word, count in word_counts.most_common(top_n)]

    # Adiciona (Nulo) e (Vazio) se as flags estiverem ativas e houver ocorrências
    if include_null and df[column].isna().any():
        if "(Nulo)" not in common_words: # Evita adicionar duplicatas se já estiver nos top_n
            common_words.append("(Nulo)")
    if include_empty and (df[column].astype(str).str.strip() == "").any():
        if "(Vazio)" not in common_words: # Evita adicionar duplicatas
            common_words.append("(Vazio)")

    return common_words

# MODIFICADO: Função para incluir opção de NULL/Vazio para CNAEs e garantir que não são cortados pelo top_n
@st.cache_data
def get_top_n_cnaes(df, cnae_type, top_n, include_null=False, include_empty=False):
    all_cnaes_info = []

    def add_cnaes_from_columns(codes_col, descriptions_col):
        if codes_col in df.columns and descriptions_col in df.columns:
            # Filtra apenas linhas onde o código CNAE não é NaN e não é string vazia para processamento
            valid_cnae_rows = df[df[codes_col].notna() & (df[codes_col].astype(str).str.strip() != "")]
            for _, row in valid_cnae_rows.iterrows():
                codes_str = str(row[codes_col]).strip()
                descriptions_str = str(row[descriptions_col]).strip()
                
                codes = [c.strip() for c in codes_str.split('; ') if c.strip()]
                descriptions = [d.strip() for d in descriptions_str.split('; ') if d.strip()]
                
                for i in range(min(len(codes), len(descriptions))):
                    code = codes[i]
                    description = descriptions[i]
                    if code and description:
                        all_cnaes_info.append((code, description))
    
    if cnae_type == 'principal' or cnae_type == 'ambos':
        add_cnaes_from_columns('cnae_principal_cod', 'cnae_principal')

    if cnae_type == 'secundario' or cnae_type == 'ambos':
        add_cnaes_from_columns('cnae_secundario_cod', 'cnae_secundario')
    
    if not all_cnaes_info:
        # Se não há CNAEs reais, mas pedimos Nulo/Vazio, ainda assim os adicionamos
        common_cnaes = []
    else:
        cnae_pair_counts = Counter(all_cnaes_info)
        common_cnaes = [(code, desc) for (code, desc), freq in cnae_pair_counts.most_common(top_n)]

    # Adiciona (Nulo) e (Vazio) se as flags estiverem ativas e houver ocorrências
    # Verifica a coluna de código correspondente na DF original
    if cnae_type == 'principal' or cnae_type == 'ambos':
        if include_null and df['cnae_principal_cod'].isna().any():
            if ("(Nulo)", "(Nulo)") not in common_cnaes:
                common_cnaes.append(("(Nulo)", "(Nulo)"))
        if include_empty and (df['cnae_principal_cod'].astype(str).str.strip() == "").any():
            if ("(Vazio)", "(Vazio)") not in common_cnaes:
                common_cnaes.append(("(Vazio)", "(Vazio)"))

    if cnae_type == 'secundario' or cnae_type == 'ambos':
        if include_null and df['cnae_secundario_cod'].isna().any():
            if ("(Nulo)", "(Nulo)") not in common_cnaes:
                common_cnaes.append(("(Nulo)", "(Nulo)"))
        if include_empty and (df['cnae_secundario_cod'].astype(str).str.strip() == "").any():
            if ("(Vazio)", "(Vazio)") not in common_cnaes:
                common_cnaes.append(("(Vazio)", "(Vazio)"))
    
    return common_cnaes

# --- FUNÇÃO PRINCIPAL DE GERAÇÃO DA QUERY SQL ---
# CORRIGIDO: Reintroduz os JOINs para tb_cnae e usa os aliases corretos para os códigos CNAE.
# MODIFICADO: Lógica para incluir NULL e Vazio separadamente
def generate_sql_query(params, excluded_cnpjs_set=None):
    base_query = "SELECT vec.*"
    joins = []
    conditions = ["vec.situacao_cadastral = 'ATIVA'"]
    query_params = {}
    param_counter = 0

    col_map = {
        'nome_fantasia': 'vec.nome_fantasia',
        'uf': 'vec.uf',
        'municipio': 'vec.municipio',
        'bairro': 'vec.bairro',
        'cod_cnae_principal': 'tc_principal.cod_cnae',
        'cod_cnae_secundario': 'tc_secundaria.cod_cnae',
        'data_inicio_atividade': 'vec.data_inicio_atividade',
        'capital_social': 'vec.capital_social',
        'porte_empresa': 'vec.porte_empresa',
        'natureza_juridica': 'vec.natureza_juridica',
        'opcao_simples': 'vec.opcao_simples',
        'opcao_mei': 'vec.opcao_mei',
        'situacao_cadastral': 'vec.situacao_cadastral',
        'ddd1': 'vec.ddd1',
        'nome_socio_razao_social': 'vec.nome_socio',
        'qualificacao_socio': 'vec.qualificacao_socio',
        'faixa_etaria_socio': 'vec.faixa_etaria_socio'
    }

    # Adiciona JOINs e SELECTs para CNAE apenas se houver critérios de CNAE selecionados
    # ou se forem os únicos parâmetros ativos (mesmo que sejam nulos/vazios)
    if ('cod_cnae_principal' in params and params['cod_cnae_principal']) or \
       ('cod_cnae_secundario' in params and params['cod_cnae_secundario']):
        # Evita adicionar os mesmos joins duas vezes se ambos CNAEs forem selecionados
        if "LEFT JOIN tb_cnae tc_principal ON unaccent(upper(vec.cnae_principal)) = unaccent(upper(tc_principal.descricao))" not in joins:
            base_query += ", tc_principal.cod_cnae AS cod_cnae_principal_found"
            joins.append("LEFT JOIN tb_cnae tc_principal ON unaccent(upper(vec.cnae_principal)) = unaccent(upper(tc_principal.descricao))")
        if "LEFT JOIN tb_cnae tc_secundaria ON unaccent(upper(vec.cnae_secundario)) = unaccent(upper(tc_secundaria.descricao))" not in joins:
            base_query += ", tc_secundaria.cod_cnae AS cod_cnae_secundario_found"
            joins.append("LEFT JOIN tb_cnae tc_secundaria ON unaccent(upper(vec.cnae_secundario)) = unaccent(upper(tc_secundaria.descricao))")


    for param, value in params.items():
        # AQUI É ONDE A LÓGICA DEVE GARANTIR QUE VALORES VAZIOS/NULOS DE PARAMETROS DESMARCADOS NÃO ENTRAM
        if value is None or (isinstance(value, list) and not value) or (isinstance(value, tuple) and not all(value)):
            continue # Ignora parâmetros que estão vazios ou None
            
        if param != 'situacao_cadastral': 
            col_name = col_map.get(param)
            param_conditions = []
            
            # --- Lógica para campos que usam get_unique_values ---
            if param in ['uf', 'municipio', 'bairro', 'natureza_juridica',
                            'qualificacao_socio', 'faixa_etaria_socio', 'ddd1',
                            'porte_empresa', 'opcao_simples', 'opcao_mei']:

                actual_values = [v for v in value if v != "(Nulo)" and v != "(Vazio)"]
                include_null_condition = "(Nulo)" in value
                include_empty_condition = "(Vazio)" in value

                if actual_values:
                    param_name_base = f"{param}_list_{param_counter}"
                    placeholders_cond = []
                    for i, val in enumerate(actual_values):
                        ph_name = f"{param_name_base}_{i}"
                        placeholders_cond.append(f"unaccent(upper({col_name})) = unaccent(upper(trim(TRAILING ' ' FROM :{ph_name})))")
                        query_params[ph_name] = val.replace("'", "''")
                    param_conditions.append(f"({' OR '.join(placeholders_cond)})")
                    param_counter += 1
                
                if include_null_condition:
                    param_conditions.append(f"{col_name} IS NULL")
                if include_empty_condition:
                    param_conditions.append(f"{col_name} = ''")

                if param_conditions:
                    conditions.append(f"({' OR '.join(param_conditions)})")

            # --- Lógica para nome_fantasia (usa ILIKE para palavras-chave) ---
            elif param == 'nome_fantasia':
                actual_words = [w for w in value if w != "(Nulo)" and w != "(Vazio)"]
                include_null_condition = "(Nulo)" in value
                include_empty_condition = "(Vazio)" in value

                word_search_conditions = []
                for word in actual_words:
                    param_name = f"nf_k_{param_counter}"
                    word_search_conditions.append(f"unaccent(upper({col_name})) ILIKE unaccent(upper(trim(TRAILING ' ' FROM :{param_name})))")
                    query_params[param_name] = f'%{word.replace("'", "''")}%'
                    param_counter += 1
                
                if include_null_condition:
                    word_search_conditions.append(f"{col_name} IS NULL")
                if include_empty_condition:
                    word_search_conditions.append(f"{col_name} = ''")
                
                if word_search_conditions:
                    conditions.append(f"({' OR '.join(word_search_conditions)})")

            # --- Lógica para CNAE Principal ---
            elif param == 'cod_cnae_principal':
                codes_list = [code for code, _ in value if code != "(Nulo)" and code != "(Vazio)"]
                include_null_cnae_p = any(code == "(Nulo)" for code, _ in value)
                include_empty_cnae_p = any(code == "(Vazio)" for code, _ in value)
                
                main_cnae_conditions = []
                cnae_principal_col_alias = col_map.get('cod_cnae_principal') 
                for code in codes_list:
                    param_name = f"cnae_pr_code_{code.replace('.', '_')}_{param_counter}"
                    # Usa o alias do JOIN para os códigos CNAE válidos
                    main_cnae_conditions.append(f"{cnae_principal_col_alias} = :{param_name}")
                    query_params[param_name] = code.replace("'", "''")
                    param_counter += 1
                
                # Para NULL e Vazio, usa a coluna original da vec
                if include_null_cnae_p:
                    main_cnae_conditions.append(f"vec.cnae_principal IS NULL") 
                if include_empty_cnae_p:
                    main_cnae_conditions.append(f"vec.cnae_principal = ''") 

                if main_cnae_conditions:
                    conditions.append(f"({' OR '.join(main_cnae_conditions)})")

            # --- Lógica para CNAE Secundário ---
            elif param == 'cod_cnae_secundario':
                codes_list = [code for code, _ in value if code != "(Nulo)" and code != "(Vazio)"]
                include_null_cnae_s = any(code == "(Nulo)" for code, _ in value)
                include_empty_cnae_s = any(code == "(Vazio)" for code, _ in value)

                secondary_cnae_like_conditions = []
                cnae_sec_col_alias = col_map.get('cod_cnae_secundario') 
                for code in codes_list:
                    param_name = f"cnae_sec_code_{code.replace('.', '_')}_{param_counter}"
                    # Usa ILIKE para CNAE secundário nos valores reais
                    secondary_cnae_like_conditions.append(f"{cnae_sec_col_alias} ILIKE :{param_name}")
                    query_params[param_name] = f'%{code.replace("'", "''")}%'
                    param_counter += 1
                
                # Para NULL e Vazio, usa a coluna original da vec
                if include_null_cnae_s:
                    secondary_cnae_like_conditions.append(f"vec.cnae_secundario IS NULL") 
                if include_empty_cnae_s:
                    secondary_cnae_like_conditions.append(f"vec.cnae_secundario = ''") 

                if secondary_cnae_like_conditions:
                    conditions.append(f"({' OR '.join(secondary_cnae_like_conditions)})")
            
            # --- Lógica para Data Início Atividade ---
            elif param == 'data_inicio_atividade':
                col_name = col_map.get(param)
                start_date, end_date = value
                param_name_start = f"start_date_{param_counter}"
                param_name_end = f"end_date_{param_counter}"
                conditions.append(f"{col_name} BETWEEN :{param_name_start} AND :{param_name_end}")
                query_params[param_name_start] = start_date
                query_params[param_name_end] = end_date
                param_counter += 1

            # --- Lógica para Capital Social ---
            elif param == 'capital_social':
                col_name = col_map.get(param)
                min_val, max_val = value
                param_name_min = f"min_capital_{param_counter}"
                param_name_max = f"max_capital_{param_counter}"
                conditions.append(f"{col_name} >= :{param_name_min} AND {col_name} <= :{param_name_max}")
                query_params[param_name_min] = min_val
                query_params[param_name_max] = max_val
                param_counter += 1
            
            # --- Lógica para Nome Sócio / Razão Social (busca por similaridade) ---
            elif param == 'nome_socio_razao_social':
                col_name = col_map.get(param)
                actual_name_parts = [p for p in value if p != "(Nulo)" and p != "(Vazio)"]
                include_null_condition = "(Nulo)" in value
                include_empty_condition = "(Vazio)" in value

                sub_conditions = []
                for name_part in actual_name_parts:
                    param_name = f"socio_name_{param_counter}"
                    sub_conditions.append(f"unaccent(upper({col_name})) ILIKE unaccent(upper(trim(TRAILING ' ' FROM :{param_name})))")
                    query_params[param_name] = f'%{name_part.replace("'", "''")}%'
                    param_counter += 1
                
                if include_null_condition:
                    sub_conditions.append(f"{col_name} IS NULL")
                if include_empty_condition:
                    sub_conditions.append(f"{col_name} = ''")
                
                if sub_conditions:
                    conditions.append(f"({' OR '.join(sub_conditions)})")

    # Adiciona a condição de exclusão de CNPJs já atendidos
    if excluded_cnpjs_set:
        param_name_excluded_cnpjs = f"excluded_cnpjs_{param_counter}"
        # Para o SQLAlchemy, passar uma tupla diretamente é a forma mais robusta de lidar com IN
        conditions.append(f"vec.cnpj NOT IN ({', '.join([f':{param_name_excluded_cnpjs}_{i}' for i in range(len(excluded_cnpjs_set))])})")
        for i, c in enumerate(list(excluded_cnpjs_set)):
            query_params[f"{param_name_excluded_cnpjs}_{i}"] = c
        param_counter += 1

    final_query_sql = f"{base_query} FROM visao_empresa_completa vec {' '.join(joins)} WHERE {' AND '.join(conditions)}"

    return text(final_query_sql), query_params

def calculate_score(params):
    score = 0
    for param, value in params.items():
        if value:
            score += PONTUACAO_PARAMETROS.get(param, 0)
    return score

def ensure_leads_table_exists(df_to_save, table_name='tb_leads_gerados', engine=None):
    if engine is None:
        engine = create_engine(DATABASE_URL)
    
    inspector = inspect(engine)
    
    if not inspector.has_table(table_name):
        st.info(f"Tabela '{table_name}' não encontrada. Criando a tabela...")
        try:
            df_temp = df_to_save.copy()
            df_temp['pontuacao'] = 0
            df_temp['data_geracao'] = datetime.now()
            df_temp['cliente_referencia'] = 'dummy_client'

            expected_cols_from_view = [
                'cnpj', 'razao_social', 'nome_fantasia', 'cod_cnae_principal', 'cnae_principal', 'cod_cnae_secundario', 'cnae_secundario',
                'logradouro', 'numero', 'complemento', 'bairro', 'municipio', 'uf', 'cep',
                'ddd1', 'telefone1', 'email', 'data_inicio_atividade', 'capital_social',
                'porte_empresa', 'natureza_juridica', 'opcao_simples', 'opcao_mei',
                'situacao_cadastral', 'nome_socio', 'qualificacao_socio', 'faixa_etaria_socio'
            ]
            for col in expected_cols_from_view:
                if col not in df_temp.columns:
                    if col.startswith('cod_cnae_'):
                        df_temp[col] = pd.Series(dtype=str)
                    elif 'capital_social' in col:
                        df_temp[col] = pd.Series(dtype=float)
                    elif 'data_inicio_atividade' in col:
                        df_temp[col] = pd.Series(dtype='datetime64[ns]')
                    else:
                        df_temp[col] = pd.Series(dtype='object')

            for col in ['cnpj', 'razao_social', 'nome_fantasia', 'cod_cnae_principal', 'cnae_principal', 'cod_cnae_secundario', 'cnae_secundario',
                        'logradouro', 'numero', 'complemento', 'bairro', 'municipio', 'uf', 'cep',
                        'ddd1', 'telefone1', 'email', 'porte_empresa', 'natureza_juridica',
                        'opcao_simples', 'opcao_mei', 'situacao_cadastral', 'nome_socio',
                        'qualificacao_socio', 'faixa_etaria_socio', 'cliente_referencia']:
                if col in df_temp.columns:
                    # Melhor forma de tratar NaN para strings antes de to_sql:
                    # Converte para string e depois para None (para SQL NULL)
                    df_temp[col] = df_temp[col].astype(str).replace({pd.NA: None, 'nan': None, '':None}).apply(lambda x: x if x is not None else None)
            
            if 'data_inicio_atividade' in df_temp.columns:
                df_temp['data_inicio_atividade'] = pd.to_datetime(df_temp['data_inicio_atividade'], errors='coerce').dt.date
            if 'capital_social' in df_temp.columns:
                df_temp['capital_social'] = pd.to_numeric(df_temp['capital_social'], errors='coerce')


            df_temp.head(0).to_sql(table_name, con=engine, if_exists='append', index=False)
            st.success(f"Tabela '{table_name}' criada com colunas iniciais.")

        except Exception as e:
            st.error(f"Erro ao criar a tabela '{table_name}' com Pandas `to_sql`: {e}")
            st.warning("Verifique se as colunas no DataFrame correspondem aos tipos de dados esperados pelo PostgreSQL.")
            raise
    
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            columns = inspector.get_columns(table_name)
            id_column_exists = any(col['name'] == 'id' for col in columns)

            current_pk_name = None
            pk_column_names = []
            
            pk_constraints = conn.execute(text(f"""
                SELECT conname, pg_get_constraintdef(oid)
                FROM pg_constraint
                WHERE conrelid = '{table_name}'::regclass AND contype = 'p';
            """)).fetchall()

            if pk_constraints:
                current_pk_name = pk_constraints[0][0] 
                pk_def = pk_constraints[0][1] 
                match = re.search(r'PRIMARY KEY \((.*?)\)', pk_def)
                if match:
                    pk_column_names = [col.strip() for col in match.group(1).split(',')]
            
            if not id_column_exists:
                st.info(f"Coluna 'id' não encontrada na tabela '{table_name}'. Adicionando como SERIAL PRIMARY KEY...")
                if current_pk_name:
                    st.warning(f"Removendo chave primária existente '{current_pk_name}' para adicionar 'id' como PK.")
                    conn.execute(text(f"ALTER TABLE {table_name} DROP CONSTRAINT {current_pk_name};"))
                conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN id SERIAL PRIMARY KEY;"))
                st.success(f"Coluna 'id' adicionada como SERIAL PRIMARY KEY na tabela '{table_name}'.")
            else:
                if current_pk_name and 'id' not in pk_column_names:
                    st.warning(f"Chave primária existente '{current_pk_name}' não está em 'id'. Removendo para definir 'id' como PK.")
                    conn.execute(text(f"ALTER TABLE {table_name} DROP CONSTRAINT {current_pk_name};"))
                    conn.execute(text(f"ALTER TABLE {table_name} ADD PRIMARY KEY (id);"))
                    st.success(f"Coluna 'id' agora é PRIMARY KEY na tabela '{table_name}'.")
                elif current_pk_name and 'id' in pk_column_names and len(pk_column_names) == 1:
                    st.info(f"Coluna 'id' já existe e já é PRIMARY KEY na tabela '{table_name}'.")
                else:
                    try:
                        conn.execute(text(f"ALTER TABLE {table_name} ADD PRIMARY KEY (id);"))
                        st.success(f"Coluna 'id' agora é PRIMARY KEY na tabela '{table_name}'.")
                    except Exception as pk_e:
                        if "already exists" in str(pk_e) or "already a primary key" in str(pk_e):
                            st.info(f"Coluna 'id' já existe e já é PRIMARY KEY na tabela '{table_name}'.")
                        else:
                            st.warning(f"Não foi possível garantir que 'id' é PRIMARY KEY (pode já ser ou outro erro): {pk_e}")
            
            trans.commit()
        except Exception as e:
            trans.rollback()
            st.error(f"Erro ao configurar SERIAL PRIMARY KEY ou PK em 'id' na tabela {table_name}: {e}")
            raise

# --- Início da UI Streamlit ---

if 'df_cnpjs' not in st.session_state or st.session_state.df_cnpjs is None:
    st.warning("Nenhum dado de cliente carregado. Por favor, carregue e enriqueça os dados na Etapa 1.")
    st.info("Você será redirecionado para a Etapa 1.")
    st.stop()

df_clientes = st.session_state.df_cnpjs # Esta é a base de clientes existente do seu cliente!
cnpjs_para_excluir = set(df_clientes['cnpj'].dropna().astype(str).tolist()) if 'cnpj' in df_clientes.columns else set()


st.markdown("## ⚙️ Configuração dos Parâmetros de Busca")

st.subheader("Informações do Cliente")
cliente_referencia = st.text_input("Nome ou ID do Cliente para esta Geração de Leads:", key="cliente_referencia_input")
if not cliente_referencia:
    st.warning("Por favor, insira um nome ou ID para o cliente antes de gerar leads.")

# Reinicia ia_params a cada execução para garantir que apenas parâmetros ativos sejam considerados.
ia_params = {}
current_score = 0

col1, col2 = st.columns(2)

with col1:
    st.subheader("Critérios de Identificação de Perfil")

    use_nome_fantasia = st.checkbox("Incluir Palavras-Chave (Nome Fantasia)", value=True)
    if use_nome_fantasia:
        col_nf_null, col_nf_empty = st.columns(2)
        with col_nf_null:
            include_null_nf = st.checkbox("Incluir Nulo (Nome Fantasia)?", key="ia_nf_null")
        with col_nf_empty:
            include_empty_nf = st.checkbox("Incluir Vazio (Nome Fantasia)?", key="ia_nf_empty")
        
        top_n_nf = st.slider("Top N palavras mais frequentes:", min_value=1, max_value=50, value=10, key="ia_top_nf")
        stop_words = set(unidecode(word.lower()) for word in [
            "e", "de", "do", "da", "dos", "das", "o", "a", "os", "as", "um", "uma", "uns", "umas",
            "para", "com", "sem", "em", "no", "na", "nos", "nas", "ao", "aos", "à", "às",
            "por", "pelo", "pela", "pelos", "pelas", "ou", "nem", "mas", "mais", "menos",
            "desde", "até", "após", "entre", "sob", "sobre", "ante", "após", "contra",
            "desde", "durante", "entre", "mediante", "perante", "salvo", "sem", "sob", "sobre", "trás",
            "s.a", "sa", "ltda", "me", "eireli", "epp", "s.a.", "ltda.", "me.", "eireli.", "epp.",
            "sa.", "ltda.", "me.", "eireli.", "epp.", "s/a", "comercio", "servicos", "serviços", "brasil", "brasileira"              
        ])
        top_nf_words = get_top_n_words(df_clientes, 'nome_fantasia', top_n_nf, stop_words, include_null=include_null_nf, include_empty=include_empty_nf)
        if top_nf_words:
            ia_params['nome_fantasia'] = st.multiselect("Palavras-chave selecionadas:", options=top_nf_words, default=top_nf_words, key="ia_nf_select")
        else:
            st.info("Nenhuma palavra-chave relevante encontrada no nome fantasia dos clientes.")
            ia_params['nome_fantasia'] = [] # Garante que seja uma lista vazia se não houver palavras
    else:
        ia_params['nome_fantasia'] = [] # Explicitamente define como vazio se desmarcado

    use_uf = st.checkbox("Incluir UF", value=True)
    if use_uf:
        col_uf_null, col_uf_empty = st.columns(2)
        with col_uf_null:
            include_null_uf = st.checkbox("Incluir UF Nula?", key="ia_uf_null")
        with col_uf_empty:
            include_empty_uf = st.checkbox("Incluir UF Vazia?", key="ia_uf_empty")

        top_n_uf = st.slider("Top N UFs mais frequentes:", min_value=1, max_value=27, value=5, key="ia_top_uf")
        top_ufs = get_unique_values(df_clientes, 'uf', top_n_uf, include_null=include_null_uf, include_empty=include_empty_uf)
        if top_ufs:
            ia_params['uf'] = st.multiselect("UFs selecionadas:", options=top_ufs, default=top_ufs, key="ia_uf_select")
        else:
            st.info("Nenhuma UF relevante encontrada.")
            ia_params['uf'] = []
    else:
        ia_params['uf'] = [] # Explicitamente define como vazio se desmarcado

    use_municipio = st.checkbox("Incluir Município", value=True)
    if use_municipio:
        col_mun_null, col_mun_empty = st.columns(2)
        with col_mun_null:
            include_null_municipio = st.checkbox("Incluir Município Nulo?", key="ia_municipio_null")
        with col_mun_empty:
            include_empty_municipio = st.checkbox("Incluir Município Vazio?", key="ia_municipio_empty")

        top_n_municipio = st.slider("Top N Municípios mais frequentes:", min_value=1, max_value=50, value=10, key="ia_top_municipio")
        top_municipios = get_unique_values(df_clientes, 'municipio', top_n_municipio, include_null=include_null_municipio, include_empty=include_empty_municipio)
        if top_municipios:
            ia_params['municipio'] = st.multiselect("Municípios selecionados:", options=top_municipios, default=top_municipios, key="ia_municipio_select")
        else:
            st.info("Nenhum Município relevante encontrado.")
            ia_params['municipio'] = []
    else:
        ia_params['municipio'] = [] # Explicitamente define como vazio se desmarcado

    use_bairro = st.checkbox("Incluir Bairro", value=False)
    if use_bairro:
        col_bairro_null, col_bairro_empty = st.columns(2)
        with col_bairro_null:
            include_null_bairro = st.checkbox("Incluir Bairro Nulo?", key="ia_bairro_null")
        with col_bairro_empty:
            include_empty_bairro = st.checkbox("Incluir Bairro Vazio?", key="ia_bairro_empty")

        top_n_bairro = st.slider("Top N Bairros mais frequentes:", min_value=1, max_value=50, value=10, key="ia_top_bairro")
        df_temp_bairro = df_clientes.copy()
        def normalizar_bairro_ia(bairro):
            if pd.isna(bairro): return None
            return unidecode(str(bairro).upper().split('/')[0].strip())
        df_temp_bairro['bairro_normalizado_ia'] = df_temp_bairro['bairro'].apply(normalizar_bairro_ia)
        top_bairros = get_unique_values(df_temp_bairro, 'bairro_normalizado_ia', top_n_bairro, include_null=include_null_bairro, include_empty=include_empty_bairro)
        if top_bairros:
            ia_params['bairro'] = st.multiselect("Bairros selecionados:", options=top_bairros, default=top_bairros, key="ia_bairro_select")
        else:
            st.info("Nenhum bairro relevante encontrado nos dados dos clientes.")
            ia_params['bairro'] = []
    else:
        ia_params['bairro'] = [] # Explicitamente define como vazio se desmarcado

    use_cnae_principal = st.checkbox("Incluir CNAE Principal", value=True)
    if use_cnae_principal:
        col_cnae_p_null, col_cnae_p_empty = st.columns(2)
        with col_cnae_p_null:
            include_null_cnae_p = st.checkbox("Incluir CNAE Principal Nulo?", key="ia_cnae_p_null")
        with col_cnae_p_empty:
            include_empty_cnae_p = st.checkbox("Incluir CNAE Principal Vazio?", key="ia_cnae_p_empty")

        top_n_cnae_principal = st.slider("Top N CNAEs Principais mais frequentes:", min_value=1, max_value=50, value=10, key="ia_top_cnae_principal")
        top_cnaes_principal_pairs = get_top_n_cnaes(df_clientes, 'principal', top_n_cnae_principal, include_null=include_null_cnae_p, include_empty=include_empty_cnae_p)
        if top_cnaes_principal_pairs:
            options_display = [f"{code} - {desc}" for code, desc in top_cnaes_principal_pairs]
            selected_options = st.multiselect("CNAEs Principais selecionados:", options=options_display, default=options_display, key="ia_cnae_principal_select")
            ia_params['cod_cnae_principal'] = []
            for opt in selected_options:
                if opt == "(Nulo) - (Nulo)":
                    ia_params['cod_cnae_principal'].append(("(Nulo)", "(Nulo)"))
                elif opt == "(Vazio) - (Vazio)":
                    ia_params['cod_cnae_principal'].append(("(Vazio)", "(Vazio)"))
                else:
                    ia_params['cod_cnae_principal'].append((opt.split(' - ')[0], opt.split(' - ')[1]))
        else:
            st.info("Nenhum CNAE Principal relevante encontrado.")
            ia_params['cod_cnae_principal'] = []
    else:
        ia_params['cod_cnae_principal'] = [] # Explicitamente define como vazio se desmarcado

    use_cnae_secundario = st.checkbox("Incluir CNAE Secundário", value=False)
    if use_cnae_secundario:
        col_cnae_s_null, col_cnae_s_empty = st.columns(2)
        with col_cnae_s_null:
            include_null_cnae_s = st.checkbox("Incluir CNAE Secundário Nulo?", key="ia_cnae_s_null")
        with col_cnae_s_empty:
            include_empty_cnae_s = st.checkbox("Incluir CNAE Secundário Vazio?", key="ia_cnae_s_empty")

        top_n_cnae_secundario = st.slider("Top N CNAEs Secundários mais frequentes:", min_value=1, max_value=50, value=10, key="ia_top_cnae_secundario")
        top_cnaes_secundario_pairs = get_top_n_cnaes(df_clientes, 'secundario', top_n_cnae_secundario, include_null=include_null_cnae_s, include_empty=include_empty_cnae_s)
        if top_cnaes_secundario_pairs:
            options_display = [f"{code} - {desc}" for code, desc in top_cnaes_secundario_pairs]
            selected_options = st.multiselect("CNAEs Secundários selecionados:", options=options_display, default=options_display, key="ia_cnae_secundario_select")
            ia_params['cod_cnae_secundario'] = []
            for opt in selected_options:
                if opt == "(Nulo) - (Nulo)":
                    ia_params['cod_cnae_secundario'].append(("(Nulo)", "(Nulo)"))
                elif opt == "(Vazio) - (Vazio)":
                    ia_params['cod_cnae_secundario'].append(("(Vazio)", "(Vazio)"))
                else:
                    ia_params['cod_cnae_secundario'].append((opt.split(' - ')[0], opt.split(' - ')[1]))
        else:
            st.info("Nenhum CNAE Secundário relevante encontrado.")
            ia_params['cod_cnae_secundario'] = []
    else:
        ia_params['cod_cnae_secundario'] = [] # Explicitamente define como vazio se desmarcado

    st.subheader("Data de Início de Atividade")
    use_data_inicio = st.checkbox("Incluir Período de Início de Atividade", value=True)
    if use_data_inicio:
        min_date_client = df_clientes['data_inicio_atividade'].min() if 'data_inicio_atividade' in df_clientes.columns and not df_clientes['data_inicio_atividade'].empty else datetime(1900, 1, 1).date()
        max_date_client = df_clientes['data_inicio_atividade'].max() if 'data_inicio_atividade' in df_clientes.columns and not df_clientes['data_inicio_atividade'].empty else datetime.now().date()

        if isinstance(min_date_client, pd.Timestamp):
            min_date_client = min_date_client.date()
        if isinstance(max_date_client, pd.Timestamp):
            max_date_client = max_date_client.date()
        
        min_calendar_date = datetime(1900, 1, 1).date()
        max_calendar_date = datetime.now().date()

        col_start_date, col_end_date = st.columns(2)
        with col_start_date:
            start_date = st.date_input(
                "Data de Início (De):",
                value=min_date_client,
                min_value=min_calendar_date,
                max_value=max_calendar_date,
                key="ia_start_date_input"
            )
        with col_end_date:
            end_date = st.date_input(
                "Data de Início (Até):",
                value=max_calendar_date,
                min_value=min_calendar_date,
                max_value=max_calendar_date,
                key="ia_end_date_input"
            )

        if start_date > end_date:
            st.error("A 'Data de Início (De)' não pode ser posterior à 'Data de Início (Até)'. Por favor, ajuste o período.")
            ia_params['data_inicio_atividade'] = None # Define como None se houver erro ou desativado
        else:
            ia_params['data_inicio_atividade'] = (start_date, end_date)
    else:
        ia_params['data_inicio_atividade'] = None # Explicitamente define como None se desmarcado


    st.subheader("Capital Social")
    use_capital_social = st.checkbox("Incluir Faixa de Capital Social", value=True)
    if use_capital_social:
        min_capital_client = df_clientes['capital_social'].min() if 'capital_social' in df_clientes.columns and not df_clientes['capital_social'].empty else 0.0
        max_capital_client = df_clientes['capital_social'].max() if 'capital_social' in df_clientes.columns and not df_clientes['capital_social'].empty else 10000000.0
        
        if min_capital_client == max_capital_client and min_capital_client > 0:
            min_capital_client = max(0.0, min_capital_client * 0.9)
            max_capital_client = max_capital_client * 1.1

        col_min_capital, col_max_capital = st.columns(2)
        with col_min_capital:
            min_val = st.number_input(
                "Capital Social (Mínimo):",
                min_value=0.0,
                value=float(min_capital_client),
                step=1000.0,
                format="%.2f",
                key="ia_min_capital_input"
            )
        with col_max_capital:
            max_val = st.number_input(
                "Capital Social (Máximo):",
                min_value=0.0,
                value=float(max_capital_client),
                step=1000.0,
                format="%.2f",
                key="ia_max_capital_input"
            )

        if min_val > max_val:
            st.error("O Capital Social Mínimo não pode ser maior que o Capital Social Máximo.")
            ia_params['capital_social'] = None # Define como None se houver erro ou desativado
        else:
            ia_params['capital_social'] = (min_val, max_val)
        st.info(f"Faixa Selecionada: R$ {min_val:,.2f} a R$ {max_val:,.2f}")
    else:
        ia_params['capital_social'] = None # Explicitamente define como None se desmarcado


    use_porte_empresa = st.checkbox("Incluir Porte da Empresa", value=True)
    if use_porte_empresa:
        col_porte_null, col_porte_empty = st.columns(2)
        with col_porte_null:
            include_null_porte = st.checkbox("Incluir Porte da Empresa Nulo?", key="ia_porte_null")
        with col_porte_empty:
            include_empty_porte = st.checkbox("Incluir Porte da Empresa Vazio?", key="ia_porte_empty")
        
        options_porte = ["MICRO EMPRESA", "EMPRESA DE PEQUENO PORTE", "DEMAIS"]
        if include_null_porte:
            options_porte.append("(Nulo)")
        if include_empty_porte:
            options_porte.append("(Vazio)")

        # Pega os portes únicos da DF e adiciona (Nulo) e (Vazio) se aplicável
        unique_portes_from_df = df_clientes['porte_empresa'].dropna().unique().tolist()
        default_selected_portes = [p for p in unique_portes_from_df if p in options_porte]
        if include_null_porte and "(Nulo)" in options_porte:
            default_selected_portes.append("(Nulo)")
        if include_empty_porte and "(Vazio)" in options_porte:
            default_selected_portes.append("(Vazio)")

        selected_portes = st.multiselect(
            "Selecione o(s) Porte(s) da Empresa:",
            options=options_porte,
            default=default_selected_portes,
            key="ia_porte_empresa_select"
        )
        if selected_portes:
            ia_params['porte_empresa'] = selected_portes
        else:
            ia_params['porte_empresa'] = []
    else:
        ia_params['porte_empresa'] = [] # Explicitamente define como vazio se desmarcado

    use_natureza_juridica = st.checkbox("Incluir Natureza Jurídica", value=False)
    if use_natureza_juridica:
        col_nj_null, col_nj_empty = st.columns(2)
        with col_nj_null:
            include_null_nj = st.checkbox("Incluir Natureza Jurídica Nula?", key="ia_nj_null")
        with col_nj_empty:
            include_empty_nj = st.checkbox("Incluir Natureza Jurídica Vazia?", key="ia_nj_empty")

        top_n_nj = st.slider("Top N Naturezas Jurídicas mais frequentes:", min_value=1, max_value=50, value=10, key="ia_top_nj")
        top_njs = get_unique_values(df_clientes, 'natureza_juridica', top_n_nj, include_null=include_null_nj, include_empty=include_empty_nj)
        if top_njs:
            ia_params['natureza_juridica'] = st.multiselect("Naturezas Jurídicas selecionadas:", options=top_njs, default=top_njs, key="ia_nj_select")
        else:
            st.info("Nenhuma Natureza Jurídica relevante encontrada.")
            ia_params['natureza_juridica'] = []
    else:
        ia_params['natureza_juridica'] = [] # Explicitamente define como vazio se desmarcado

    use_opcao_simples = st.checkbox("Incluir Opção Simples Nacional", value=False)
    if use_opcao_simples:
        col_simples_null, col_simples_empty = st.columns(2)
        with col_simples_null:
            include_null_simples = st.checkbox("Incluir Opção Simples Nacional Nula?", key="ia_simples_null")
        with col_simples_empty:
            include_empty_simples = st.checkbox("Incluir Opção Simples Nacional Vazia?", key="ia_simples_empty")
        
        simples_options = ['S', 'N']
        if include_null_simples:
            simples_options.append("(Nulo)")
        if include_empty_simples:
            simples_options.append("(Vazio)")

        # Pega opções únicas da DF e adiciona (Nulo) e (Vazio) se aplicável
        unique_simples_from_df = df_clientes['opcao_simples'].dropna().unique().tolist()
        default_selected_simples = [s for s in unique_simples_from_df if s in simples_options]
        if include_null_simples and "(Nulo)" in simples_options:
            default_selected_simples.append("(Nulo)")
        if include_empty_simples and "(Vazio)" in simples_options:
            default_selected_simples.append("(Vazio)")

        selected_opcao_simples = st.multiselect("Optante pelo Simples Nacional?", options=simples_options, default=default_selected_simples, key="ia_simples_select")
        if selected_opcao_simples:
            ia_params['opcao_simples'] = selected_opcao_simples
        else:
            ia_params['opcao_simples'] = []
    else:
        ia_params['opcao_simples'] = [] # Explicitamente define como vazio se desmarcado


    use_opcao_mei = st.checkbox("Incluir Opção MEI", value=False)
    if use_opcao_mei:
        col_mei_null, col_mei_empty = st.columns(2)
        with col_mei_null:
            include_null_mei = st.checkbox("Incluir Opção MEI Nula?", key="ia_mei_null")
        with col_mei_empty:
            include_empty_mei = st.checkbox("Incluir Opção MEI Vazia?", key="ia_mei_empty")
        
        mei_options = ['S', 'N']
        if include_null_mei:
            mei_options.append("(Nulo)")
        if include_empty_mei:
            mei_options.append("(Vazio)")

        # Pega opções únicas da DF e adiciona (Nulo) e (Vazio) se aplicável
        unique_mei_from_df = df_clientes['opcao_mei'].dropna().unique().tolist()
        default_selected_mei = [m for m in unique_mei_from_df if m in mei_options]
        if include_null_mei and "(Nulo)" in mei_options:
            default_selected_mei.append("(Nulo)")
        if include_empty_mei and "(Vazio)" in mei_options:
            default_selected_mei.append("(Vazio)")

        selected_opcao_mei = st.multiselect("Optante pelo MEI?", options=mei_options, default=default_selected_mei, key="ia_mei_select")
        if selected_opcao_mei:
            ia_params['opcao_mei'] = selected_opcao_mei
        else:
            ia_params['opcao_mei'] = []
    else:
        ia_params['opcao_mei'] = [] # Explicitamente define como vazio se desmarcado


with col2:
    st.subheader("Critérios de Contato e Sócios")

    use_ddd1 = st.checkbox("Incluir DDD de Contato", value=False)
    if use_ddd1:
        col_ddd_null, col_ddd_empty = st.columns(2)
        with col_ddd_null:
            include_null_ddd1 = st.checkbox("Incluir DDD Nulo?", key="ia_ddd1_null")
        with col_ddd_empty:
            include_empty_ddd1 = st.checkbox("Incluir DDD Vazio?", key="ia_ddd1_empty")

        top_n_ddd = st.slider("Top N DDDs mais frequentes:", min_value=1, max_value=50, value=10, key="ia_top_ddd") # Adicionei slider para top_n
        unique_ddds = get_unique_values(df_clientes, 'ddd1', top_n_ddd, include_null=include_null_ddd1, include_empty=include_empty_ddd1)
        selected_ddds = st.multiselect("DDDs de interesse (digite ou selecione):", options=unique_ddds, default=unique_ddds, key="ia_ddd1_select")
        if selected_ddds:
            ia_params['ddd1'] = selected_ddds
        else:
            st.info("Nenhum DDD relevante encontrado.")
            ia_params['ddd1'] = []
    else:
        ia_params['ddd1'] = [] # Explicitamente define como vazio se desmarcado
        
    use_nome_socio_razao = st.checkbox("Incluir Nome Sócio / Razão Social (busca por similaridade)", value=False)
    if use_nome_socio_razao:
        col_socio_null, col_socio_empty = st.columns(2)
        with col_socio_null:
            include_null_socio = st.checkbox("Incluir Nome Sócio/Razão Social Nulo?", key="ia_socio_null")
        with col_socio_empty:
            include_empty_socio = st.checkbox("Incluir Nome Sócio/Razão Social Vazio?", key="ia_socio_empty")
        
        unique_socios = df_clientes['nome_socio'].dropna().astype(str).unique().tolist() if 'nome_socio' in df_clientes.columns else []
        default_socios_input = ", ".join(unique_socios[:3]) if unique_socios else ""
        
        nomes_input = st.text_area(
            "Nomes de sócios/razão social para buscar (separados por vírgula):",
            value=default_socios_input,
            key="ia_nome_socio_razao_input",
            help="Digite nomes ou partes de nomes de sócios ou da razão social para buscar por similaridade. Separe por vírgulas."
        )
        
        names_for_param = [name.strip() for name in nomes_input.split(',') if name.strip()]
        if include_null_socio:
            names_for_param.append("(Nulo)")
        if include_empty_socio:
            names_for_param.append("(Vazio)")

        if names_for_param:
            ia_params['nome_socio_razao_social'] = names_for_param
        else:
            st.info("Digite nomes para buscar ou marque para incluir nulo/vazio.")
            ia_params['nome_socio_razao_social'] = []
    else:
        ia_params['nome_socio_razao_social'] = [] # Explicitamente define como vazio se desmarcado


    use_qualificacao_socio = st.checkbox("Incluir Qualificação do Sócio", value=False)
    if use_qualificacao_socio:
        col_qs_null, col_qs_empty = st.columns(2)
        with col_qs_null:
            include_null_qs = st.checkbox("Incluir Qualificação do Sócio Nula?", key="ia_qs_null")
        with col_qs_empty:
            include_empty_qs = st.checkbox("Incluir Qualificação do Sócio Vazia?", key="ia_qs_empty")

        top_n_qs = st.slider("Top N Qualificações de Sócio mais frequentes:", min_value=1, max_value=50, value=10, key="ia_top_qs")
        top_qss = get_unique_values(df_clientes, 'qualificacao_socio', top_n_qs, include_null=include_null_qs, include_empty=include_empty_qs)
        if top_qss:
            ia_params['qualificacao_socio'] = st.multiselect("Qualificações de sócio selecionadas:", options=top_qss, default=top_qss, key="ia_qs_select")
        else:
            st.info("Nenhuma Qualificação de Sócio relevante encontrada.")
            ia_params['qualificacao_socio'] = []
    else:
        ia_params['qualificacao_socio'] = [] # Explicitamente define como vazio se desmarcado

    use_faixa_etaria_socio = st.checkbox("Incluir Faixa Etária do Sócio", value=False)
    if use_faixa_etaria_socio:
        col_fes_null, col_fes_empty = st.columns(2)
        with col_fes_null:
            include_null_fes = st.checkbox("Incluir Faixa Etária do Sócio Nula?", key="ia_fes_null")
        with col_fes_empty:
            include_empty_fes = st.checkbox("Incluir Faixa Etária do Sócio Vazia?", key="ia_fes_empty")

        top_n_fes = st.slider("Top N Faixas Etárias de Sócio mais frequentes:", min_value=1, max_value=10, value=5, key="ia_top_fes")
        top_fess = get_unique_values(df_clientes, 'faixa_etaria_socio', top_n_fes, include_null=include_null_fes, include_empty=include_empty_fes)
        if top_fess:
            ia_params['faixa_etaria_socio'] = st.multiselect("Faixas etárias de sócio selecionadas:", options=top_fess, default=top_fess, key="ia_fes_select")
        else:
            st.info("Nenhuma Faixa Etária de Sócio relevante encontrada.")
            ia_params['faixa_etaria_socio'] = []
    else:
        ia_params['faixa_etaria_socio'] = [] # Explicitamente define como vazio se desmarcado

ia_params['situacao_cadastral'] = 'ATIVA'

st.markdown("---")
if st.button("🚀 Gerar Leads com IA"):
    if not cliente_referencia:
        st.error("Por favor, preencha o campo 'Nome ou ID do Cliente' antes de gerar leads.")
        st.stop()
    
    if not cnpjs_para_excluir:
        st.warning("Não há CNPJs carregados na base do cliente para exclusão. A busca pode incluir clientes existentes.")

    with st.spinner("Gerando leads..."):
        # Garante que ia_params só contenha o que é realmente selecionado/ativo
        final_params_for_query = {k: v for k, v in ia_params.items() if v is not None and not (isinstance(v, list) and not v) and not (isinstance(v, tuple) and not all(v))}
        final_params_for_score = {k: v for k, v in ia_params.items() if v is not None and not (isinstance(v, list) and not v) and not (isinstance(v, tuple) and not all(v))}

        score = calculate_score(final_params_for_score)
        
        sql_text_obj, query_params_dict = generate_sql_query(final_params_for_query, cnpjs_para_excluir)
        
        st.session_state.current_sql_query = sql_text_obj.text
        st.session_state.current_score = score

        engine = create_engine(DATABASE_URL)
        try:
            with engine.connect() as conn:
                df_leads_gerados = pd.read_sql(sql_text_obj, conn, params=query_params_dict)
                # Remove as colunas temporárias '_found' antes de adicionar as colunas de pontuação/data e salvar
                if 'cod_cnae_principal_found' in df_leads_gerados.columns:
                    df_leads_gerados = df_leads_gerados.drop(columns=['cod_cnae_principal_found'])
                if 'cod_cnae_secundario_found' in df_leads_gerados.columns:
                    df_leads_gerados = df_leads_gerados.drop(columns=['cod_cnae_secundario_found'])
            
            df_leads_gerados['pontuacao'] = score
            df_leads_gerados['data_geracao'] = datetime.now()
            df_leads_gerados['cliente_referencia'] = cliente_referencia

            for col in df_leads_gerados.columns:
                if df_leads_gerados[col].dtype == 'object':
                    # Trata strings vazias em Pandas como None para melhor compatibilidade com SQL NULL ao salvar
                    df_leads_gerados[col] = df_leads_gerados[col].replace('', None).astype(str).replace('None', None) 
                elif pd.api.types.is_datetime64_any_dtype(df_leads_gerados[col]):
                    df_leads_gerados[col] = df_leads_gerados[col].dt.tz_localize(None)
                elif pd.api.types.is_float_dtype(df_leads_gerados[col]):
                    df_leads_gerados[col] = df_leads_gerados[col].fillna(0.0)
                elif pd.api.types.is_integer_dtype(df_leads_gerados[col]):
                    df_leads_gerados[col] = df_leads_gerados[col].fillna(0)
                
            ensure_leads_table_exists(df_leads_gerados, 'tb_leads_gerados', engine)
            
            st.session_state.df_leads_gerados = df_leads_gerados

            st.success(f"IA concluiu a análise! {len(df_leads_gerados)} leads encontrados com pontuação de precisão: {score} pontos para o cliente: '{cliente_referencia}'.")
        except Exception as e:
            st.error(f"Erro ao buscar leads no banco de dados: {e}")
            st.exception(e)
            st.session_state.df_leads_gerados = pd.DataFrame()

if 'df_leads_gerados' in st.session_state and st.session_state.df_leads_gerados is not None:
    st.subheader("🔍 Perfil do Cliente Encontrado e SQL Gerado")
    
    st.markdown("**Pontuação de Precisão:**")
    st.metric(label="Pontuação", value=f"{st.session_state.current_score} / 100 pontos")
    st.info("Essa pontuação reflete a quantidade e relevância dos critérios utilizados na busca, servindo como um indicador do refinamento do lead.")

    st.markdown("**Critérios Utilizados:**")
    # Filtra os parâmetros para exibição, considerando apenas os que foram realmente usados na query
    used_criteria = {k: v for k, v in ia_params.items() if v is not None and not (isinstance(v, list) and not v) and not (isinstance(v, tuple) and not all(v))}
    
    if used_criteria:
        for param, value in used_criteria.items():
            if param == 'data_inicio_atividade':
                st.write(f"- **Data Início Atividade:** De {value[0].strftime('%d/%m/%Y')} a {value[1].strftime('%d/%m/%Y')}")
            elif param == 'capital_social':
                st.write(f"- **Capital Social:** Entre R$ {value[0]:,.2f} e R$ {value[1]:,.2f}")
            elif param in ['nome_fantasia', 'uf', 'municipio', 'bairro', 'natureza_juridica',
                            'qualificacao_socio', 'faixa_etaria_socio', 'ddd1',
                            'porte_empresa', 'opcao_simples', 'opcao_opcao_mei', 'nome_socio_razao_social']: # Corrigido 'opcao_opcao_mei' para 'opcao_mei'
                # Verifica se há o placeholder "(Nulo)" ou "(Vazio)" para exibir corretamente
                display_value = []
                for item in value:
                    if isinstance(item, tuple) and item[0] in ["(Nulo)", "(Vazio)"]:
                        display_value.append(item[0]) 
                    elif item in ["(Nulo)", "(Vazio)"]:
                        display_value.append(item)
                    else:
                        display_value.append(str(item))
                st.write(f"- **{param.replace('_', ' ').title()}:** {', '.join(display_value)}")
            elif param in ['cod_cnae_principal', 'cod_cnae_secundario']:
                display_cnaes = []
                for code, desc in value:
                    if code == "(Nulo)":
                        display_cnaes.append("(Nulo)")
                    elif code == "(Vazio)":
                        display_cnaes.append("(Vazio)")
                    else:
                        display_cnaes.append(f"{code} - {desc}")
                st.write(f"- **{param.replace('_', ' ').title()}:** {', '.join(display_cnaes)}")
            elif param == 'situacao_cadastral':
                st.write(f"- **{param.replace('_', ' ').title()}:** {value}")
    else:
        st.write("Nenhum critério específico selecionado. Buscando apenas empresas ATIVAS.")
    
    st.write(f"- **Cliente de Referência:** {cliente_referencia}")
    st.write(f"- **CNPJs da Base do Cliente Excluídos da Busca:** {len(cnpjs_para_excluir)}")


    st.markdown("**Query SQL Gerada (com placeholders):**")
    st.code(st.session_state.current_sql_query, language='sql')

    if not st.session_state.df_leads_gerados.empty:
        st.subheader("📈 Leads Encontrados")
        st.dataframe(st.session_state.df_leads_gerados, use_container_width=True)

        col_dl, col_save = st.columns(2)
        with col_dl:
            excel_data_leads = to_excel(st.session_state.df_leads_gerados)
            st.download_button(
                "📥 Exportar Excel com Leads",
                excel_data_leads,
                "leads_gerados_ia.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        
        with col_save:
            if st.button("💾 Salvar Leads no Banco de Dados"):
                with st.spinner("Verificando duplicidades e salvando novos leads..."):
                    try:
                        engine = create_engine(DATABASE_URL)
                        
                        existing_cnpjs_query = text(
                            f"SELECT DISTINCT cnpj FROM tb_leads_gerados WHERE cliente_referencia = :cliente_ref"
                        )
                        with engine.connect() as conn:
                            existing_cnpjs_df = pd.read_sql(existing_cnpjs_query, conn, params={'cliente_ref': cliente_referencia})
                            existing_cnpjs_set = set(existing_cnpjs_df['cnpj'].tolist())
                        
                        df_leads_gerados_filtered = st.session_state.df_leads_gerados[
                            ~st.session_state.df_leads_gerados['cnpj'].astype(str).isin(existing_cnpjs_set)
                        ].copy()
                        
                        if df_leads_gerados_filtered.empty:
                            st.info(f"Todos os {len(st.session_state.df_leads_gerados)} leads gerados já existem para o cliente '{cliente_referencia}'. Nenhum novo lead foi salvo.")
                        else:
                            for col in df_leads_gerados_filtered.columns:
                                if pd.api.types.is_numeric_dtype(df_leads_gerados_filtered[col]):
                                    df_leads_gerados_filtered[col] = df_leads_gerados_filtered[col].fillna(0)
                                elif pd.api.types.is_datetime64_any_dtype(df_leads_gerados_filtered[col]):
                                    df_leads_gerados_filtered[col] = df_leads_gerados_filtered[col].dt.tz_localize(None)
                                elif df_leads_gerados_filtered[col].dtype == 'object':
                                     df_leads_gerados_filtered[col] = df_leads_gerados_filtered[col].fillna('').replace('None', '') # Garante que NaN vire string vazia antes de salvar
                                    
                            df_leads_gerados_filtered.to_sql(
                                'tb_leads_gerados',
                                con=engine,
                                if_exists='append',
                                index=False
                            )
                            st.success(f"{len(df_leads_gerados_filtered)} novos leads salvos com sucesso na tabela 'tb_leads_gerados' para o cliente '{cliente_referencia}'.")
                            if len(st.session_state.df_leads_gerados) > len(df_leads_gerados_filtered):
                                st.info(f"{len(st.session_state.df_leads_gerados) - len(df_leads_gerados_filtered)} leads já existiam e foram ignorados.")

                    except Exception as e:
                        st.error(f"Erro ao salvar leads no banco de dados: {e}")
                        st.exception(e)
    else:
        st.info("Nenhum lead encontrado com os critérios selecionados. Tente ajustar os filtros.")

st.markdown("---")
if st.button("⬅️ Voltar para Análise Gráfica"):
    st.switch_page("pages/2_Analise_Grafica.py")