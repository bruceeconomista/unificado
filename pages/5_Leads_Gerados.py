import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from io import BytesIO
import re
from collections import Counter
from unidecode import unidecode
from datetime import datetime, timedelta

st.set_page_config(layout="wide", page_title="IA Re-Gerador de Leads")
st.title("🔄 IA Re-Gerador: Reavalie e Gere Novos Leads")

DATABASE_URL = "postgresql+psycopg2://postgres:0804Bru%21%40%23%24@localhost:5432/empresas"

# --- Inicialização de estados da sessão para o Re-Gerador (independentes) ---
if 're_gen_current_score' not in st.session_state:
    st.session_state.re_gen_current_score = 0
if 're_gen_current_sql_query' not in st.session_state:
    st.session_state.re_gen_current_sql_query = None
if 're_gen_selected_client_ref' not in st.session_state:
    st.session_state.re_gen_selected_client_ref = []
if 're_gen_df_leads_for_analysis' not in st.session_state:
    st.session_state.re_gen_df_leads_for_analysis = pd.DataFrame()
if 're_gen_df_new_leads_found' not in st.session_state:
    st.session_state.re_gen_df_new_leads_found = pd.DataFrame()

# --- Custom tags para este novo contexto ---
if 're_gen_custom_tags_nf' not in st.session_state:
    st.session_state.re_gen_custom_tags_nf = []
if 're_gen_custom_tags_uf' not in st.session_state:
    st.session_state.re_gen_custom_tags_uf = []
if 're_gen_custom_tags_municipio' not in st.session_state:
    st.session_state.re_gen_custom_tags_municipio = []
if 're_gen_custom_tags_bairro' not in st.session_state:
    st.session_state.re_gen_custom_tags_bairro = []
if 're_gen_custom_tags_cnae_principal' not in st.session_state:
    st.session_state.re_gen_custom_tags_cnae_principal = []
if 're_gen_custom_tags_cnae_secundario' not in st.session_state:
    st.session_state.re_gen_custom_tags_cnae_secundario = []
if 're_gen_custom_tags_porte_empresa' not in st.session_state:
    st.session_state.re_gen_custom_tags_porte_empresa = []
if 're_gen_custom_tags_natureza_juridica' not in st.session_state:
    st.session_state.re_gen_custom_tags_natureza_juridica = []
if 're_gen_custom_tags_opcao_simples' not in st.session_state:
    st.session_state.re_gen_custom_tags_opcao_simples = []
if 're_gen_custom_tags_opcao_mei' not in st.session_state:
    st.session_state.re_gen_custom_tags_opcao_mei = []
if 're_gen_custom_tags_ddd1' not in st.session_state:
    st.session_state.re_gen_custom_tags_ddd1 = []
if 're_gen_custom_tags_nomes_socios' not in st.session_state:
    st.session_state.re_gen_custom_tags_nomes_socios = []
if 're_gen_custom_tags_qualificacoes' not in st.session_state:
    st.session_state.re_gen_custom_tags_qualificacoes = []
if 're_gen_custom_tags_faixas_etarias' not in st.session_state:
    st.session_state.re_gen_custom_tags_faixas_etarias = []


PONTUACAO_PARAMETROS = { # Keep the same scoring
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
    'nomes_socios': 0,
    'qualificacoes': 5,
    'faixas_etarias': 5
}

# --- Funções de Caching (reutilizadas do código original) ---
@st.cache_data
def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

@st.cache_data
def get_unique_values(df, column, top_n=None, include_null=False, include_empty=False):
    if column not in df.columns:
        return []
    temp_series = df[column].astype(str)
    unique_values_list = []
    actual_values = temp_series[~temp_series.isna() & (temp_series.str.strip() != "")].tolist()
    if actual_values:
        counts = pd.Series(actual_values).value_counts()
        unique_values_list.extend(counts.index.tolist())
    if include_null and df[column].isna().any():
        unique_values_list.append("(Nulo)")
    if include_empty and (df[column].astype(str).str.strip() == "").any():
        unique_values_list.append("(Vazio)")
    if top_n and len(unique_values_list) > top_n:
        filtered_list = unique_values_list[:top_n]
        if "(Nulo)" in unique_values_list and "(Nulo)" not in filtered_list:
            filtered_list.append("(Nulo)")
        if "(Vazio)" in unique_values_list and "(Vazio)" not in filtered_list:
            filtered_list.append("(Vazio)")
        return list(dict.fromkeys(filtered_list))
    return unique_values_list

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
    temp_series_non_null_empty = df[column][df[column].notna() & (df[column].astype(str).str.strip() != "")].apply(clean_and_tokenize).explode()
    all_words.extend(temp_series_non_null_empty.dropna().tolist())
    word_counts = Counter(all_words)
    common_words = [word for word, count in word_counts.most_common(top_n)]
    if include_null and df[column].isna().any():
        if "(Nulo)" not in common_words:
            common_words.append("(Nulo)")
    if include_empty and (df[column].astype(str).str.strip() == "").any():
        if "(Vazio)" not in common_words:
            common_words.append("(Vazio)")
    return common_words

@st.cache_data
def get_top_n_cnaes(df, cnae_type, top_n, include_null=False, include_empty=False):
    all_cnaes_info = []
    def add_cnaes_from_columns(codes_col, descriptions_col):
        if codes_col in df.columns and descriptions_col in df.columns:
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
        add_cnaes_from_columns('cod_cnae_principal', 'cnae_principal')
    if cnae_type == 'secundario' or cnae_type == 'ambos':
        add_cnaes_from_columns('cod_cnae_secundario', 'cnae_secundario')
    if not all_cnaes_info:
        common_cnaes = []
    else:
        cnae_pair_counts = Counter(all_cnaes_info)
        common_cnaes = [(code, desc) for (code, desc), freq in cnae_pair_counts.most_common(top_n)]
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

# --- FUNÇÃO PRINCIPAL DE GERAÇÃO DA QUERY SQL (reutilizada e adaptada) ---
def generate_sql_query(params, excluded_cnpjs_set=None, limit=1000):
    base_query = "SELECT vea.*"
    joins = []
    query_params = {}
    param_counter = 0
    conditions = []
    conditions.append("vea.situacao_cadastral = 'ATIVA'")
    uf_param = params.get("uf", [])
    municipio_param = params.get("municipio", [])
    
    if uf_param:
        uf_cond_list = []
        for i, uf in enumerate(uf_param):
            param_name = f"uf_fixed_{i}"
            uf_cond_list.append(f"vea.uf = :{param_name}")
            query_params[param_name] = uf
        conditions.append(f"({' OR '.join(uf_cond_list)})")
    
    if municipio_param:
        municipio_cond_list = []
        for i, mun in enumerate(municipio_param):
            param_name = f"mun_fixed_{i}"
            municipio_cond_list.append(f"vea.municipio = :{param_name}")
            query_params[param_name] = mun
        conditions.append(f"({' OR '.join(municipio_cond_list)})")
 
    col_map = {
        'nome_fantasia': 'vea.nome_fantasia',
        'uf': 'vea.uf',
        'municipio': 'vea.municipio',
        'bairro': 'vea.bairro',
        'cod_cnae_principal': 'vea.cod_cnae_principal',
        'cod_cnae_secundario': 'vea.cod_cnae_secundario',
        'data_inicio_atividade': 'vea.data_inicio_atividade',
        'capital_social': 'vea.capital_social',
        'porte_empresa': 'vea.porte_empresa',
        'natureza_juridica': 'vea.natureza_juridica',
        'opcao_simples': 'vea.opcao_simples',
        'opcao_mei': 'vea.opcao_mei',
        'situacao_cadastral': 'vea.situacao_cadastral',
        'ddd1': 'vea.ddd1',
        'nomes_socios': 'vea.nomes_socios',
        'qualificacoes': 'vea.qualificacoes',
        'faixas_etarias': 'vea.faixas_etarias'
    }

    for param, value in params.items():
        if value is None or (isinstance(value, list) and not value) or (isinstance(value, tuple) and not all(value)):
            continue
            
        if param != 'situacao_cadastral': 
            col_name = col_map.get(param)
            param_conditions = []
            
            if param in ['uf', 'municipio', 'bairro', 'natureza_juridica',
                            'qualificacoes', 'faixas_etarias', 'ddd1',
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

            elif param == 'cod_cnae_principal':
                codes_list = [code for code, _ in value if code != "(Nulo)" and code != "(Vazio)"]
                include_null_cnae_p = any(code == "(Nulo)" for code, _ in value)
                include_empty_cnae_p = any(code == "(Vazio)" for code, _ in value)
                
                main_cnae_conditions = []
                cnae_principal_col_alias = col_map.get('cod_cnae_principal') 
                for code in codes_list:
                    param_name = f"cnae_pr_code_{code.replace('.', '_')}_{param_counter}"
                    main_cnae_conditions.append(f"{cnae_principal_col_alias} = :{param_name}")
                    query_params[param_name] = code.replace("'", "''")
                    param_counter += 1
                
                if include_null_cnae_p:
                    main_cnae_conditions.append(f"vea.cnae_principal IS NULL") 
                if include_empty_cnae_p:
                    main_cnae_conditions.append(f"vea.cnae_principal = ''") 

                if main_cnae_conditions:
                    conditions.append(f"({' OR '.join(main_cnae_conditions)})")

            elif param == 'cod_cnae_secundario':
                codes_list = [code for code, _ in value if code != "(Nulo)" and code != "(Vazio)"]
                include_null_cnae_s = any(code == "(Nulo)" for code, _ in value)
                include_empty_cnae_s = any(code == "(Vazio)" for code, _ in value)

                secondary_cnae_like_conditions = []
                cnae_sec_col_alias = col_map.get('cod_cnae_secundario') 
                for code in codes_list:
                    param_name = f"cnae_sec_code_{code.replace('.', '_')}_{param_counter}"
                    secondary_cnae_like_conditions.append(f"{cnae_sec_col_alias} ILIKE :{param_name}")
                    query_params[param_name] = f'%{code.replace("'", "''")}%'
                    param_counter += 1
                
                if include_null_cnae_s:
                    secondary_cnae_like_conditions.append(f"vea.cnae_secundario IS NULL") 
                if include_empty_cnae_s:
                    secondary_cnae_like_conditions.append(f"vea.cnae_secundario = ''") 

                if secondary_cnae_like_conditions:
                    conditions.append(f"({' OR '.join(secondary_cnae_like_conditions)})")
            
            elif param == 'data_inicio_atividade':
                col_name = col_map.get(param)
                start_date, end_date = value
                param_name_start = f"start_date_{param_counter}"
                param_name_end = f"end_date_{param_counter}"
                conditions.append(f"{col_name} BETWEEN :{param_name_start} AND :{param_name_end}")
                query_params[param_name_start] = start_date
                query_params[param_name_end] = end_date
                param_counter += 1

            elif param == 'capital_social':
                col_name = col_map.get(param)
                min_val, max_val = value
                param_name_min = f"min_capital_{param_counter}"
                param_name_max = f"max_capital_{param_counter}"
                conditions.append(f"{col_name} >= :{param_name_min} AND {col_name} <= :{param_name_max}")
                query_params[param_name_min] = min_val
                query_params[param_name_max] = max_val
                param_counter += 1
            
            elif param == 'nomes_socios':
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

    if excluded_cnpjs_set:
        # Ensure excluded_cnpjs_set contains string values for comparison
        excluded_cnpjs_str = [str(c) for c in list(excluded_cnpjs_set)]
        if excluded_cnpjs_str: # Only add if there are actual CNPJs to exclude
            param_name_excluded_cnpjs = f"excluded_cnpjs_{param_counter}"
            # Create a list of individual parameter placeholders for IN clause
            placeholders = [f":{param_name_excluded_cnpjs}_{i}" for i in range(len(excluded_cnpjs_str))]
            conditions.append(f"vea.cnpj NOT IN ({', '.join(placeholders)})")
            for i, c in enumerate(excluded_cnpjs_str):
                query_params[f"{param_name_excluded_cnpjs}_{i}"] = c
            param_counter += 1

    final_query_sql = f"""
        {base_query} 
        FROM visao_empresa_agrupada_base vea 
        WHERE {' AND '.join(conditions)}
        LIMIT {limit}
    """

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
            # Ensure these columns exist for table creation with correct dtypes
            if 'pontuacao' not in df_temp.columns:
                df_temp['pontuacao'] = pd.Series(dtype=int)
            if 'data_geracao' not in df_temp.columns:
                df_temp['data_geracao'] = pd.Series(dtype='datetime64[ns]')
            if 'cliente_referencia' not in df_temp.columns:
                df_temp['cliente_referencia'] = pd.Series(dtype=str)

            # Define expected columns from visao_empresa_completa that should be in tb_leads_gerados
            expected_cols_from_view = [
                'cnpj', 'razao_social', 'nome_fantasia', 'cod_cnae_principal', 'cnae_principal', 'cod_cnae_secundario', 'cnae_secundario',
                'logradouro', 'numero', 'complemento', 'bairro', 'municipio', 'uf', 'cep',
                'ddd1', 'telefone1', 'email', 'data_inicio_atividade', 'capital_social',
                'porte_empresa', 'natureza_juridica', 'opcao_simples', 'opcao_mei',
                'situacao_cadastral', 'nomes_socios', 'qualificacoes', 'faixas_etarias'
            ]
            # Add any missing expected columns with appropriate dtypes
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
            
            # Ensure correct types and handle NaN/None for object columns
            for col in ['cnpj', 'razao_social', 'nome_fantasia', 'cod_cnae_principal', 'cnae_principal', 'cod_cnae_secundario', 'cnae_secundario',
                        'logradouro', 'numero', 'complemento', 'bairro', 'municipio', 'uf', 'cep',
                        'ddd1', 'telefone1', 'email', 'porte_empresa', 'natureza_juridica',
                        'opcao_simples', 'opcao_mei', 'situacao_cadastral', 'nomes_socios',
                        'qualificacoes', 'faixas_etarias', 'cliente_referencia']:
                if col in df_temp.columns:
                    df_temp[col] = df_temp[col].astype(str).replace({pd.NA: None, 'nan': None, '':None}).apply(lambda x: x if x is not None else None)
            
            df_leads = df_leads.rename(columns={
                'cod_cnae_principal': 'cnae_principal_cod',
                'cod_cnae_secundario': 'cnae_secundario_cod'
            })
            
            # Type conversion for specific columns
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


# --- Função para buscar clientes distintos ---
@st.cache_data(ttl=3600) # Cache por 1 hora
def get_distinct_client_references():
    engine = create_engine(DATABASE_URL)
    try:
        with engine.connect() as conn:
            query = text("SELECT DISTINCT cliente_referencia FROM tb_leads_gerados ORDER BY cliente_referencia;")
            df_clients = pd.read_sql(query, conn)
            return df_clients['cliente_referencia'].dropna().tolist()
    except Exception as e:
        st.error(f"Erro ao buscar clientes de referência: {e}")
        return []

# --- Função para carregar leads existentes para um cliente_referencia ---
@st.cache_data(ttl=600) # Cache por 10 minutos
def load_leads_by_client_reference(client_refs):
    if not client_refs:
        return pd.DataFrame()
    engine = create_engine(DATABASE_URL)
    try:
        with engine.connect() as conn:
            # Using text for parameterized query to handle list of client_refs
            placeholders = ', '.join([f":client_{i}" for i in range(len(client_refs))])
            query = f"SELECT * FROM tb_leads_gerados WHERE cliente_referencia IN ({placeholders})"
            params = {f"client_{i}": ref for i, ref in enumerate(client_refs)}
            df_leads = pd.read_sql(text(query), conn, params=params)
            
            # Ensure date and capital social columns have correct dtypes
            if 'data_inicio_atividade' in df_leads.columns:
                df_leads['data_inicio_atividade'] = pd.to_datetime(df_leads['data_inicio_atividade'], errors='coerce').dt.date
            if 'capital_social' in df_leads.columns:
                df_leads['capital_social'] = pd.to_numeric(df_leads['capital_social'], errors='coerce')

            return df_leads
    except Exception as e:
        st.error(f"Erro ao carregar leads para os clientes selecionados: {e}")
        return pd.DataFrame()

# --- Início da UI Streamlit do Re-Gerador ---

st.markdown("## 1️⃣ Seleção de Clientes para Reavaliação")

distinct_clients = get_distinct_client_references()

if not distinct_clients:
    st.warning("Nenhum 'cliente_referencia' encontrado na tabela 'tb_leads_gerados'. Por favor, gere leads primeiro na página original.")
else:
    selected_client_refs = st.multiselect(
        "Selecione um ou mais 'Cliente de Referência' para analisar:",
        options=distinct_clients,
        default=st.session_state.re_gen_selected_client_ref,
        key="re_gen_client_multiselect"
    )
    if selected_client_refs:
        st.session_state.re_gen_selected_client_ref = selected_client_refs
        df_leads_for_analysis = load_leads_by_client_reference(selected_client_refs)
        st.session_state.re_gen_df_leads_for_analysis = df_leads_for_analysis

        if df_leads_for_analysis.empty:
            st.warning(f"Não foram encontrados leads para os clientes selecionados: {', '.join(selected_client_refs)}.")
        else:
            st.info(f"Carregados {len(df_leads_for_analysis)} leads para análise dos clientes selecionados.")
            st.dataframe(df_leads_for_analysis.head(), use_container_width=True) # Show a sample

            st.markdown("---")
            st.markdown("## 2️⃣ Configuração dos Parâmetros de Busca (Baseado nos Clientes Selecionados)")

            re_gen_ia_params = {}
            # Use the loaded DataFrame for analysis
            df_analysis_source = st.session_state.re_gen_df_leads_for_analysis
            
            # Get CNPJs already present for the selected client references
            # These CNPJs will be excluded from the new search IF saving for the SAME client_referencia.
            # For now, let's just get all of them. The exclusion logic will be in the save step.
            existing_cnpjs_for_selected_clients = set(df_analysis_source['cnpj'].dropna().astype(str).tolist()) if 'cnpj' in df_analysis_source.columns else set()
            st.session_state.re_gen_existing_cnpjs = existing_cnpjs_for_selected_clients

            # --- Critérios de Identificação de Perfil ---
            st.subheader("Critérios de Identificação de Perfil")

            # --- Nome Fantasia ---
            col1_nf, col2_nf, col3_nf = st.columns([1, 1, 2])
            with col1_nf:
                use_nome_fantasia = st.checkbox("Incluir Palavras-Chave (Nome Fantasia)", value=True, key="re_gen_use_nf")
            with col2_nf:
                include_null_nf = st.checkbox("Nulo?", key="re_gen_nf_null") if use_nome_fantasia else False
                include_empty_nf = st.checkbox("Vazio?", key="re_gen_nf_empty") if use_nome_fantasia else False
            with col3_nf:
                if use_nome_fantasia:
                    top_n_nf = st.slider("Top N palavras mais frequentes:", min_value=1, max_value=50, value=10, key="re_gen_top_nf")
                    stop_words = set(unidecode(word.lower()) for word in [
                        "e", "de", "do", "da", "dos", "das", "o", "a", "os", "as", "um", "uma", "uns", "umas",
                        "para", "com", "sem", "em", "no", "na", "nos", "nas", "ao", "aos", "à", "às",
                        "por", "pelo", "pela", "pelos", "pelas", "ou", "nem", "mas", "mais", "menos",
                        "desde", "até", "após", "entre", "sob", "sobre", "ante", "após", "contra",
                        "desde", "durante", "entre", "mediante", "perante", "salvo", "sem", "sob", "sobre", "trás",
                        "s.a", "sa", "ltda", "me", "eireli", "epp", "s.a.", "ltda.", "me.", "eireli.", "epp.",
                        "sa.", "ltda.", "me.", "eireli.", "epp.", "comercio", "servicos", "serviços", "brasil", "brasileira"              
                    ])
                    top_nf_words = get_top_n_words(df_analysis_source, 'nome_fantasia', top_n_nf, stop_words, include_null=include_null_nf, include_empty=include_empty_nf)
                    
                    all_nf_options = list(set(top_nf_words + st.session_state.re_gen_custom_tags_nf))
                    temp_options = [opt for opt in all_nf_options if opt not in ["(Nulo)", "(Vazio)"]]
                    temp_options.sort()
                    if "(Nulo)" in all_nf_options: temp_options.append("(Nulo)")
                    if "(Vazio)" in all_nf_options: temp_options.append("(Vazio)")

                    default_nf_selection = list(set(top_nf_words + [t for t in st.session_state.re_gen_custom_tags_nf if t in temp_options]))
                    if include_null_nf and "(Nulo)" in temp_options: default_nf_selection.append("(Nulo)")
                    if include_empty_nf and "(Vazio)" in temp_options: default_nf_selection.append("(Vazio)")

                    selected_nf_words = st.multiselect(
                        "Palavras-chave selecionadas:",
                        options=temp_options,
                        default=default_nf_selection,
                        key="re_gen_nf_select"
                    )
                    re_gen_ia_params['nome_fantasia'] = selected_nf_words if selected_nf_words else []

                    new_nf_tag = st.text_input("Adicionar nova palavra-chave:", key="re_gen_new_nf_tag_input")
                    if new_nf_tag and st.button("Adicionar Tag (Nome Fantasia)", key="re_gen_add_nf_tag_button"):
                        if new_nf_tag.strip() not in st.session_state.re_gen_custom_tags_nf:
                            st.session_state.re_gen_custom_tags_nf.append(new_nf_tag.strip())
                            st.rerun()
                else:
                    re_gen_ia_params['nome_fantasia'] = []

            st.divider()

            # --- UF ---
            col1_uf, col2_uf, col3_uf = st.columns([1, 1, 2])
            with col1_uf:
                use_uf = st.checkbox("Incluir UF", value=True, key="re_gen_use_uf")
            with col2_uf:
                include_null_uf = st.checkbox("Nulo?", key="re_gen_uf_null") if use_uf else False
                include_empty_uf = st.checkbox("Vazio?", key="re_gen_uf_empty") if use_uf else False
            with col3_uf:
                if use_uf:
                    top_n_uf = st.slider("Top N UFs mais frequentes:", min_value=1, max_value=27, value=5, key="re_gen_top_uf")
                    top_ufs = get_unique_values(df_analysis_source, 'uf', top_n_uf, include_null=include_null_uf, include_empty=include_empty_uf)
                    
                    all_uf_options = list(set(top_ufs + st.session_state.re_gen_custom_tags_uf))
                    temp_options = [opt for opt in all_uf_options if opt not in ["(Nulo)", "(Vazio)"]]
                    temp_options.sort()
                    if "(Nulo)" in all_uf_options: temp_options.append("(Nulo)")
                    if "(Vazio)" in all_uf_options: temp_options.append("(Vazio)")

                    default_uf_selection = list(set(top_ufs + [t for t in st.session_state.re_gen_custom_tags_uf if t in temp_options]))
                    if include_null_uf and "(Nulo)" in temp_options: default_uf_selection.append("(Nulo)")
                    if include_empty_uf and "(Vazio)" in temp_options: default_uf_selection.append("(Vazio)")

                    selected_ufs = st.multiselect(
                        "UFs selecionadas:",
                        options=temp_options,
                        default=default_uf_selection,
                        key="re_gen_uf_select"
                    )
                    re_gen_ia_params['uf'] = selected_ufs if selected_ufs else []

                    new_uf_tag = st.text_input("Adicionar nova UF:", key="re_gen_new_uf_tag_input")
                    if new_uf_tag and st.button("Adicionar Tag (UF)", key="re_gen_add_uf_tag_button"):
                        if new_uf_tag.strip().upper() not in st.session_state.re_gen_custom_tags_uf:
                            st.session_state.re_gen_custom_tags_uf.append(new_uf_tag.strip().upper())
                            st.rerun()
                else:
                    re_gen_ia_params['uf'] = []

            st.divider()
                
            # --- Município ---
            col1_mun, col2_mun, col3_mun = st.columns([1, 1, 2])
            with col1_mun:
                use_municipio = st.checkbox("Incluir Município", value=True, key="re_gen_use_municipio")
            with col2_mun:
                include_null_municipio = st.checkbox("Nulo?", key="re_gen_municipio_null") if use_municipio else False
                include_empty_municipio = st.checkbox("Vazio?", key="re_gen_municipio_empty") if use_municipio else False
            with col3_mun:
                if use_municipio:
                    top_n_municipio = st.slider("Top N Municípios mais frequentes:", min_value=1, max_value=50, value=10, key="re_gen_top_municipio")
                    top_municipios = get_unique_values(df_analysis_source, 'municipio', top_n_municipio, include_null=include_null_municipio, include_empty=include_empty_municipio)

                    all_municipio_options = list(set(top_municipios + st.session_state.re_gen_custom_tags_municipio))
                    temp_options = [opt for opt in all_municipio_options if opt not in ["(Nulo)", "(Vazio)"]]
                    temp_options.sort()
                    if "(Nulo)" in all_municipio_options: temp_options.append("(Nulo)")
                    if "(Vazio)" in all_municipio_options: temp_options.append("(Vazio)")

                    default_municipio_selection = list(set(top_municipios + [t for t in st.session_state.re_gen_custom_tags_municipio if t in temp_options]))
                    if include_null_municipio and "(Nulo)" in temp_options: default_municipio_selection.append("(Nulo)")
                    if include_empty_municipio and "(Vazio)" in temp_options: default_municipio_selection.append("(Vazio)")

                    selected_municipios = st.multiselect(
                        "Municípios selecionados:",
                        options=temp_options,
                        default=default_municipio_selection,
                        key="re_gen_municipio_select"
                    )
                    re_gen_ia_params['municipio'] = selected_municipios if selected_municipios else []

                    new_municipio_tag = st.text_input("Adicionar novo Município:", key="re_gen_new_municipio_tag_input")
                    if new_municipio_tag and st.button("Adicionar Tag (Município)", key="re_gen_add_municipio_tag_button"):
                        if new_municipio_tag.strip() not in st.session_state.re_gen_custom_tags_municipio:
                            st.session_state.re_gen_custom_tags_municipio.append(new_municipio_tag.strip())
                            st.rerun()
                else:
                    re_gen_ia_params['municipio'] = []

            st.divider()

            # --- Bairro ---
            col1_bairro, col2_bairro, col3_bairro = st.columns([1, 1, 2])
            with col1_bairro:
                use_bairro = st.checkbox("Incluir Bairro", value=False, key="re_gen_use_bairro")
            with col2_bairro:
                include_null_bairro = st.checkbox("Nulo?", key="re_gen_bairro_null") if use_bairro else False
                include_empty_bairro = st.checkbox("Vazio?", key="re_gen_bairro_empty") if use_bairro else False
            with col3_bairro:
                if use_bairro:
                    top_n_bairro = st.slider("Top N Bairros mais frequentes:", min_value=1, max_value=50, value=10, key="re_gen_top_bairro")
                    df_temp_bairro = df_analysis_source.copy()
                    def normalizar_bairro_ia(bairro):
                        if pd.isna(bairro): return None
                        return unidecode(str(bairro).upper().split('/')[0].strip())
                    df_temp_bairro['bairro_normalizado_ia'] = df_temp_bairro['bairro'].apply(normalizar_bairro_ia)
                    top_bairros = get_unique_values(df_temp_bairro, 'bairro_normalizado_ia', top_n_bairro, include_null=include_null_bairro, include_empty=include_empty_bairro)
                    
                    all_bairro_options = list(set(top_bairros + st.session_state.re_gen_custom_tags_bairro))
                    temp_options = [opt for opt in all_bairro_options if opt not in ["(Nulo)", "(Vazio)"]]
                    temp_options.sort()
                    if "(Nulo)" in all_bairro_options: temp_options.append("(Nulo)")
                    if "(Vazio)" in all_bairro_options: temp_options.append("(Vazio)")

                    default_bairro_selection = list(set(top_bairros + [t for t in st.session_state.re_gen_custom_tags_bairro if t in temp_options]))
                    if include_null_bairro and "(Nulo)" in temp_options: default_bairro_selection.append("(Nulo)")
                    if include_empty_bairro and "(Vazio)" in temp_options: default_bairro_selection.append("(Vazio)")

                    selected_bairros = st.multiselect(
                        "Bairros selecionados:",
                        options=temp_options,
                        default=default_bairro_selection,
                        key="re_gen_bairro_select"
                    )
                    re_gen_ia_params['bairro'] = selected_bairros if selected_bairros else []

                    new_bairro_tag = st.text_input("Adicionar novo Bairro:", key="re_gen_new_bairro_tag_input")
                    if new_bairro_tag and st.button("Adicionar Tag (Bairro)", key="re_gen_add_bairro_tag_button"):
                        if new_bairro_tag.strip() not in st.session_state.re_gen_custom_tags_bairro:
                            st.session_state.re_gen_custom_tags_bairro.append(new_bairro_tag.strip())
                            st.rerun()
                else:
                    re_gen_ia_params['bairro'] = []

            st.divider()

            # --- CNAE Principal ---
            col1_cnae_p, col2_cnae_p, col3_cnae_p = st.columns([1, 1, 2])
            with col1_cnae_p:
                use_cnae_principal = st.checkbox("Incluir CNAE Principal", value=True, key="re_gen_use_cnae_principal")
            with col2_cnae_p:
                include_null_cnae_p = st.checkbox("Nulo?", key="re_gen_cnae_p_null") if use_cnae_principal else False
                include_empty_cnae_p = st.checkbox("Vazio?", key="re_gen_cnae_p_empty") if use_cnae_principal else False
            with col3_cnae_p:
                if use_cnae_principal:
                    top_n_cnae_principal = st.slider("Top N CNAEs Principais mais frequentes:", min_value=1, max_value=50, value=10, key="re_gen_top_cnae_principal")
                    top_cnaes_principal_pairs = get_top_n_cnaes(df_analysis_source, 'principal', top_n_cnae_principal, include_null=include_null_cnae_p, include_empty=include_empty_cnae_p)
                    
                    all_cnae_p_options_tuple = list(set(top_cnaes_principal_pairs + st.session_state.re_gen_custom_tags_cnae_principal))
                    all_cnae_p_options_display = []
                    default_cnae_p_selection_display = []

                    special_tags_p = []
                    for code, desc in all_cnae_p_options_tuple:
                        if code in ["(Nulo)", "(Vazio)"]:
                            special_tags_p.append((code, desc))
                        else:
                            all_cnae_p_options_display.append(f"{code} - {desc}")
                    
                    all_cnae_p_options_display.sort()
                    
                    for code, desc in special_tags_p:
                        all_cnae_p_options_display.append(f"{code} - {desc}")

                    for code, desc in top_cnaes_principal_pairs:
                        if f"{code} - {desc}" in all_cnae_p_options_display:
                            default_cnae_p_selection_display.append(f"{code} - {desc}")
                    for code, desc in st.session_state.re_gen_custom_tags_cnae_principal:
                        if f"{code} - {desc}" in all_cnae_p_options_display:
                            default_cnae_p_selection_display.append(f"{code} - {desc}")
                    
                    if include_null_cnae_p and "(Nulo) - (Nulo)" in all_cnae_p_options_display:
                        default_cnae_p_selection_display.append("(Nulo) - (Nulo)")
                    if include_empty_cnae_p and "(Vazio) - (Vazio)" in all_cnae_p_options_display:
                        default_cnae_p_selection_display.append("(Vazio) - (Vazio)")

                    selected_options_cnae_p = st.multiselect(
                        "CNAEs Principais selecionados:",
                        options=all_cnae_p_options_display,
                        default=list(set(default_cnae_p_selection_display)),
                        key="re_gen_cnae_principal_select"
                    )

                    re_gen_ia_params['cod_cnae_principal'] = []
                    for opt in selected_options_cnae_p:
                        if opt == "(Nulo) - (Nulo)":
                            re_gen_ia_params['cod_cnae_principal'].append(("(Nulo)", "(Nulo)"))
                        elif opt == "(Vazio) - (Vazio)":
                            re_gen_ia_params['cod_cnae_principal'].append(("(Vazio)", "(Vazio)"))
                        else:
                            code_desc_pair = opt.split(' - ', 1)
                            if len(code_desc_pair) == 2:
                                re_gen_ia_params['cod_cnae_principal'].append((code_desc_pair[0], code_desc_pair[1]))
                            else:
                                re_gen_ia_params['cod_cnae_principal'].append((opt, opt))
                    
                    new_cnae_p_input = st.text_input("Adicionar novo CNAE Principal (código ou descrição):", key="re_gen_new_cnae_p_input")
                    if new_cnae_p_input and st.button("Adicionar Tag (CNAE Principal)", key="re_gen_add_cnae_p_tag_button"):
                        new_cnae_p_code = new_cnae_p_input.strip()
                        new_cnae_p_desc = new_cnae_p_input.strip()
                        if re.match(r'^\d{4}-\d{1}$', new_cnae_p_code) or re.match(r'^\d{4}-\d{2}$', new_cnae_p_code):
                            pass
                        elif re.match(r'^\d{4}\d{2}$', new_cnae_p_code):
                            new_cnae_p_code = f"{new_cnae_p_code[:4]}-{new_cnae_p_code[4:]}"
                        
                        new_cnae_pair = (new_cnae_p_code, new_cnae_p_desc)
                        if new_cnae_pair not in st.session_state.re_gen_custom_tags_cnae_principal:
                            st.session_state.re_gen_custom_tags_cnae_principal.append(new_cnae_pair)
                            st.rerun()
                else:
                    re_gen_ia_params['cod_cnae_principal'] = []

            st.divider()

            # --- CNAE Secundário ---
            col1_cnae_s, col2_cnae_s, col3_cnae_s = st.columns([1, 1, 2])
            with col1_cnae_s:
                use_cnae_secundario = st.checkbox("Incluir CNAE Secundário", value=False, key="re_gen_use_cnae_secundario")
            with col2_cnae_s:
                include_null_cnae_s = st.checkbox("Nulo?", key="re_gen_cnae_s_null") if use_cnae_secundario else False
                include_empty_cnae_s = st.checkbox("Vazio?", key="re_gen_cnae_s_empty") if use_cnae_secundario else False
            with col3_cnae_s:
                if use_cnae_secundario:
                    top_n_cnae_secundario = st.slider("Top N CNAEs Secundários mais frequentes:", min_value=1, max_value=50, value=10, key="re_gen_top_cnae_secundario")
                    top_cnaes_secundario_pairs = get_top_n_cnaes(df_analysis_source, 'secundario', top_n_cnae_secundario, include_null=include_null_cnae_s, include_empty=include_empty_cnae_s)
                    
                    all_cnae_s_options_tuple = list(set(top_cnaes_secundario_pairs + st.session_state.re_gen_custom_tags_cnae_secundario))
                    all_cnae_s_options_display = []
                    default_cnae_s_selection_display = []

                    special_tags_s = []
                    for code, desc in all_cnae_s_options_tuple:
                        if code in ["(Nulo)", "(Vazio)"]:
                            special_tags_s.append((code, desc))
                        else:
                            all_cnae_s_options_display.append(f"{code} - {desc}")
                    
                    all_cnae_s_options_display.sort()
                    
                    for code, desc in special_tags_s:
                        all_cnae_s_options_display.append(f"{code} - {desc}")

                    for code, desc in top_cnaes_secundario_pairs:
                        if f"{code} - {desc}" in all_cnae_s_options_display:
                            default_cnae_s_selection_display.append(f"{code} - {desc}")
                    for code, desc in st.session_state.re_gen_custom_tags_cnae_secundario:
                        if f"{code} - {desc}" in all_cnae_s_options_display:
                            default_cnae_s_selection_display.append(f"{code} - {desc}")

                    selected_options_cnae_s = st.multiselect(
                        "CNAEs Secundários selecionados:",
                        options=all_cnae_s_options_display,
                        default=list(set(default_cnae_s_selection_display)),
                        key="re_gen_cnae_secundario_select"
                    )

                    re_gen_ia_params['cod_cnae_secundario'] = []
                    for opt in selected_options_cnae_s:
                        if opt == "(Nulo) - (Nulo)":
                            re_gen_ia_params['cod_cnae_secundario'].append(("(Nulo)", "(Nulo)"))
                        elif opt == "(Vazio) - (Vazio)":
                            re_gen_ia_params['cod_cnae_secundario'].append(("(Vazio)", "(Vazio)"))
                        else:
                            code_desc_pair = opt.split(' - ', 1)
                            if len(code_desc_pair) == 2:
                                re_gen_ia_params['cod_cnae_secundario'].append((code_desc_pair[0], code_desc_pair[1]))
                            else:
                                re_gen_ia_params['cod_cnae_secundario'].append((opt, opt))
                    
                    new_cnae_s_input = st.text_input("Adicionar novo CNAE Secundário (código ou descrição):", key="re_gen_new_cnae_s_input")
                    if new_cnae_s_input and st.button("Adicionar Tag (CNAE Secundário)", key="re_gen_add_cnae_s_tag_button"):
                        new_cnae_s_code = new_cnae_s_input.strip()
                        new_cnae_s_desc = new_cnae_s_input.strip()
                        if re.match(r'^\d{4}-\d{1}$', new_cnae_s_code) or re.match(r'^\d{4}-\d{2}$', new_cnae_s_code):
                            pass
                        elif re.match(r'^\d{4}\d{2}$', new_cnae_s_code):
                            new_cnae_s_code = f"{new_cnae_s_code[:4]}-{new_cnae_s_code[4:]}"
                        
                        new_cnae_pair = (new_cnae_s_code, new_cnae_s_desc)
                        if new_cnae_pair not in st.session_state.re_gen_custom_tags_cnae_secundario:
                            st.session_state.re_gen_custom_tags_cnae_secundario.append(new_cnae_pair)
                            st.rerun()
                else:
                    re_gen_ia_params['cod_cnae_secundario'] = []

            st.divider()

            # --- Data de Início de Atividade ---
            col1_data, col2_data, col3_data = st.columns([1, 1, 2])
            with col1_data:
                use_data_inicio = st.checkbox("Incluir Período de Início de Atividade", value=True, key="re_gen_use_data_inicio")
            with col2_data:
                st.write("")
            with col3_data:
                if use_data_inicio:
                    min_date_client = df_analysis_source['data_inicio_atividade'].min() if 'data_inicio_atividade' in df_analysis_source.columns and not df_analysis_source['data_inicio_atividade'].empty else datetime(1900, 1, 1).date()
                    max_date_client = df_analysis_source['data_inicio_atividade'].max() if 'data_inicio_atividade' in df_analysis_source.columns and not df_analysis_source['data_inicio_atividade'].empty else datetime.now().date()

                    if isinstance(min_date_client, pd.Timestamp):
                        min_date_client = min_date_client.date()
                    if isinstance(max_date_client, pd.Timestamp):
                        max_date_client = max_date_client.date()
                    
                    min_calendar_date = datetime(1900, 1, 1).date()
                    max_calendar_date = datetime.now().date()

                    start_date = st.date_input(
                        "Data de Início (De):",
                        value=min_date_client,
                        min_value=min_calendar_date,
                        max_value=max_calendar_date,
                        key="re_gen_start_date_input"
                    )
                    end_date = st.date_input(
                        "Data de Início (Até):",
                        value=max_calendar_date,
                        min_value=min_calendar_date,
                        max_value=max_calendar_date,
                        key="re_gen_end_date_input"
                    )

                    if start_date > end_date:
                        st.error("A 'Data de Início (De)' não pode ser posterior à 'Data de Início (Até)'. Por favor, ajuste o período.")
                        re_gen_ia_params['data_inicio_atividade'] = None
                    else:
                        re_gen_ia_params['data_inicio_atividade'] = (start_date, end_date)
                else:
                    re_gen_ia_params['data_inicio_atividade'] = None

            st.divider()

            # --- Capital Social ---
            col1_capital, col2_capital, col3_capital = st.columns([1, 1, 2])
            with col1_capital:
                use_capital_social = st.checkbox("Incluir Faixa de Capital Social", value=True, key="re_gen_use_capital_social")
            with col2_capital:
                st.write("")
            with col3_capital:
                if use_capital_social:
                    min_capital_client = df_analysis_source['capital_social'].min() if 'capital_social' in df_analysis_source.columns and not df_analysis_source['capital_social'].empty else 0.0
                    max_capital_client = df_analysis_source['capital_social'].max() if 'capital_social' in df_analysis_source.columns and not df_analysis_source['capital_social'].empty else 10000000.0
                    
                    if min_capital_client == max_capital_client and min_capital_client > 0:
                        min_capital_client = max(0.0, min_capital_client * 0.9)
                        max_capital_client = max_capital_client * 1.1

                    min_val = st.number_input(
                        "Capital Social (Mínimo):",
                        min_value=0.0,
                        value=float(min_capital_client),
                        step=1000.0,
                        format="%.2f",
                        key="re_gen_min_capital_input"
                    )
                    max_val = st.number_input(
                        "Capital Social (Máximo):",
                        min_value=0.0,
                        value=float(max_capital_client),
                        step=1000.0,
                        format="%.2f",
                        key="re_gen_max_capital_input"
                    )

                    if min_val > max_val:
                        st.error("O Capital Social Mínimo não pode ser maior que o Capital Social Máximo.")
                        re_gen_ia_params['capital_social'] = None
                    else:
                        re_gen_ia_params['capital_social'] = (min_val, max_val)
                    st.info(f"Faixa Selecionada: R$ {min_val:,.2f} a R$ {max_val:,.2f}")
                else:
                    re_gen_ia_params['capital_social'] = None

            st.divider()

            # --- Porte da Empresa ---
            col1_porte, col2_porte, col3_porte = st.columns([1, 1, 2])
            with col1_porte:
                use_porte_empresa = st.checkbox("Incluir Porte da Empresa", value=True, key="re_gen_use_porte_empresa")
            with col2_porte:
                include_null_porte = st.checkbox("Nulo?", key="re_gen_porte_null") if use_porte_empresa else False
                include_empty_porte = st.checkbox("Vazio?", key="re_gen_porte_empty") if use_porte_empresa else False
            with col3_porte:
                if use_porte_empresa:
                    base_options_porte = ["MICRO EMPRESA", "EMPRESA DE PEQUENO PORTE", "DEMAIS"]
                    unique_portes_from_df = df_analysis_source['porte_empresa'].dropna().unique().tolist()
                    
                    all_porte_options = list(set(base_options_porte + unique_portes_from_df + st.session_state.re_gen_custom_tags_porte_empresa))
                    temp_options = [opt for opt in all_porte_options if opt not in ["(Nulo)", "(Vazio)"]]
                    temp_options.sort()
                    if "(Nulo)" in all_porte_options: temp_options.append("(Nulo)")
                    if "(Vazio)" in all_porte_options: temp_options.append("(Vazio)")

                    default_selected_portes = list(set([p for p in base_options_porte if p in temp_options] + unique_portes_from_df + [t for t in st.session_state.re_gen_custom_tags_porte_empresa if t in temp_options]))
                    if include_null_porte and "(Nulo)" in temp_options: default_selected_portes.append("(Nulo)")
                    if include_empty_porte and "(Vazio)" in temp_options: default_selected_portes.append("(Vazio)")
                    
                    selected_portes = st.multiselect(
                        "Selecione o(s) Porte(s) da Empresa:",
                        options=temp_options,
                        default=list(set(default_selected_portes)),
                        key="re_gen_porte_empresa_select"
                    )
                    re_gen_ia_params['porte_empresa'] = selected_portes if selected_portes else []

                    new_porte_tag = st.text_input("Adicionar novo Porte da Empresa:", key="re_gen_new_porte_tag_input")
                    if new_porte_tag and st.button("Adicionar Tag (Porte)", key="re_gen_add_porte_tag_button"):
                        if new_porte_tag.strip() not in st.session_state.re_gen_custom_tags_porte_empresa:
                            st.session_state.re_gen_custom_tags_porte_empresa.append(new_porte_tag.strip())
                            st.rerun()
                else:
                    re_gen_ia_params['porte_empresa'] = []

            st.divider()

            # --- Natureza Jurídica ---
            col1_nj, col2_nj, col3_nj = st.columns([1, 1, 2])
            with col1_nj:
                use_natureza_juridica = st.checkbox("Incluir Natureza Jurídica", value=False, key="re_gen_use_natureza_juridica")
            with col2_nj:
                include_null_nj = st.checkbox("Nulo?", key="re_gen_nj_null") if use_natureza_juridica else False
                include_empty_nj = st.checkbox("Vazio?", key="re_gen_nj_empty") if use_natureza_juridica else False
            with col3_nj:
                if use_natureza_juridica:
                    top_n_nj = st.slider("Top N Naturezas Jurídicas mais frequentes:", min_value=1, max_value=50, value=10, key="re_gen_top_nj")
                    top_njs = get_unique_values(df_analysis_source, 'natureza_juridica', top_n_nj, include_null=include_null_nj, include_empty=include_empty_nj)
                    
                    all_nj_options = list(set(top_njs + st.session_state.re_gen_custom_tags_natureza_juridica))
                    temp_options = [opt for opt in all_nj_options if opt not in ["(Nulo)", "(Vazio)"]]
                    temp_options.sort()
                    if "(Nulo)" in all_nj_options: temp_options.append("(Nulo)")
                    if "(Vazio)" in all_nj_options: temp_options.append("(Vazio)")

                    default_nj_selection = list(set(top_njs + [t for t in st.session_state.re_gen_custom_tags_natureza_juridica if t in temp_options]))
                    if include_null_nj and "(Nulo)" in temp_options: default_nj_selection.append("(Nulo)")
                    if include_empty_nj and "(Vazio)" in temp_options: default_nj_selection.append("(Vazio)")

                    selected_njs = st.multiselect(
                        "Naturezas Jurídicas selecionadas:",
                        options=temp_options,
                        default=list(set(default_nj_selection)),
                        key="re_gen_nj_select"
                    )
                    re_gen_ia_params['natureza_juridica'] = selected_njs if selected_njs else []

                    new_nj_tag = st.text_input("Adicionar nova Natureza Jurídica:", key="re_gen_new_nj_tag_input")
                    if new_nj_tag and st.button("Adicionar Tag (Natureza Jurídica)", key="re_gen_add_nj_tag_button"):
                        if new_nj_tag.strip() not in st.session_state.re_gen_custom_tags_natureza_juridica:
                            st.session_state.re_gen_custom_tags_natureza_juridica.append(new_nj_tag.strip())
                            st.rerun()
                else:
                    re_gen_ia_params['natureza_juridica'] = []

            st.divider()

            # --- Opção Simples Nacional ---
            col1_simples, col2_simples, col3_simples = st.columns([1, 1, 2])
            with col1_simples:
                use_opcao_simples = st.checkbox("Incluir Opção Simples Nacional", value=False, key="re_gen_use_opcao_simples")
            with col2_simples:
                include_null_simples = st.checkbox("Nulo?", key="re_gen_simples_null") if use_opcao_simples else False
                include_empty_simples = st.checkbox("Vazio?", key="re_gen_simples_empty") if use_opcao_simples else False
            with col3_simples:
                if use_opcao_simples:
                    base_simples_options = ['S', 'N']
                    unique_simples_from_df = df_analysis_source['opcao_simples'].dropna().unique().tolist()

                    all_simples_options = list(set(base_simples_options + unique_simples_from_df + st.session_state.re_gen_custom_tags_opcao_simples))
                    temp_options = [opt for opt in all_simples_options if opt not in ["(Nulo)", "(Vazio)"]]
                    temp_options.sort()
                    if "(Nulo)" in all_simples_options: temp_options.append("(Nulo)")
                    if "(Vazio)" in all_simples_options: temp_options.append("(Vazio)")

                    default_selected_simples = list(set([s for s in base_simples_options if s in temp_options] + unique_simples_from_df + [t for t in st.session_state.re_gen_custom_tags_opcao_simples if t in temp_options]))
                    if include_null_simples and "(Nulo)" in temp_options: default_selected_simples.append("(Nulo)")
                    if include_empty_simples and "(Vazio)" in temp_options: default_selected_simples.append("(Vazio)")

                    selected_opcao_simples = st.multiselect(
                        "Optante pelo Simples Nacional?",
                        options=temp_options,
                        default=list(set(default_selected_simples)),
                        key="re_gen_simples_select"
                    )
                    re_gen_ia_params['opcao_simples'] = selected_opcao_simples if selected_opcao_simples else []

                    new_simples_tag = st.text_input("Adicionar nova Opção Simples Nacional:", key="re_gen_new_simples_tag_input")
                    if new_simples_tag and st.button("Adicionar Tag (Simples)", key="re_gen_add_simples_tag_button"):
                        if new_simples_tag.strip().upper() not in st.session_state.re_gen_custom_tags_opcao_simples:
                            st.session_state.re_gen_custom_tags_opcao_simples.append(new_simples_tag.strip().upper())
                            st.rerun()
                else:
                    re_gen_ia_params['opcao_simples'] = []

            st.divider()

            # --- Opção MEI ---
            col1_mei, col2_mei, col3_mei = st.columns([1, 1, 2])
            with col1_mei:
                use_opcao_mei = st.checkbox("Incluir Opção MEI", value=False, key="re_gen_use_opcao_mei")
            with col2_mei:
                include_null_mei = st.checkbox("Nulo?", key="re_gen_mei_null") if use_opcao_mei else False
                include_empty_mei = st.checkbox("Vazio?", key="re_gen_mei_empty") if use_opcao_mei else False
            with col3_mei:
                if use_opcao_mei:
                    base_mei_options = ['S', 'N']
                    unique_mei_from_df = df_analysis_source['opcao_mei'].dropna().unique().tolist()

                    all_mei_options = list(set(base_mei_options + unique_mei_from_df + st.session_state.re_gen_custom_tags_opcao_mei))
                    temp_options = [opt for opt in all_mei_options if opt not in ["(Nulo)", "(Vazio)"]]
                    temp_options.sort()
                    if "(Nulo)" in all_mei_options: temp_options.append("(Nulo)")
                    if "(Vazio)" in all_mei_options: temp_options.append("(Vazio)")

                    default_selected_mei = list(set([m for m in base_mei_options if m in temp_options] + unique_mei_from_df + [t for t in st.session_state.re_gen_custom_tags_opcao_mei if t in temp_options]))
                    if include_null_mei and "(Nulo)" in temp_options: default_selected_mei.append("(Nulo)")
                    if include_empty_mei and "(Vazio)" in temp_options: default_selected_mei.append("(Vazio)")

                    selected_opcao_mei = st.multiselect(
                        "Optante pelo MEI?",
                        options=temp_options,
                        default=list(set(default_selected_mei)),
                        key="re_gen_mei_select"
                    )
                    re_gen_ia_params['opcao_mei'] = selected_opcao_mei if selected_opcao_mei else []

                    new_mei_tag = st.text_input("Adicionar nova Opção MEI:", key="re_gen_new_mei_tag_input")
                    if new_mei_tag and st.button("Adicionar Tag (MEI)", key="re_gen_add_mei_tag_button"):
                        if new_mei_tag.strip().upper() not in st.session_state.re_gen_custom_tags_opcao_mei:
                            st.session_state.re_gen_custom_tags_opcao_mei.append(new_mei_tag.strip().upper())
                            st.rerun()
                else:
                    re_gen_ia_params['opcao_mei'] = []

            st.divider()

            # --- Critérios de Contato e Sócios ---
            st.subheader("Critérios de Contato e Sócios")

            # --- DDD de Contato ---
            col1_ddd, col2_ddd, col3_ddd = st.columns([1, 1, 2])
            with col1_ddd:
                use_ddd1 = st.checkbox("Incluir DDD de Contato", value=False, key="re_gen_use_ddd1")
            with col2_ddd:
                include_null_ddd1 = st.checkbox("Nulo?", key="re_gen_ddd1_null") if use_ddd1 else False
                include_empty_ddd1 = st.checkbox("Vazio?", key="re_gen_ddd1_empty") if use_ddd1 else False
            with col3_ddd:
                if use_ddd1:
                    top_n_ddd = st.slider("Top N DDDs mais frequentes:", min_value=1, max_value=50, value=10, key="re_gen_top_ddd")
                    unique_ddds = get_unique_values(df_analysis_source, 'ddd1', top_n_ddd, include_null=include_null_ddd1, include_empty=include_empty_ddd1)
                    
                    all_ddd_options = list(set(unique_ddds + st.session_state.re_gen_custom_tags_ddd1))
                    temp_options = [opt for opt in all_ddd_options if opt not in ["(Nulo)", "(Vazio)"]]
                    temp_options.sort()
                    if "(Nulo)" in all_ddd_options: temp_options.append("(Nulo)")
                    if "(Vazio)" in all_ddd_options: temp_options.append("(Vazio)")

                    default_selected_ddds = list(set(unique_ddds + [t for t in st.session_state.re_gen_custom_tags_ddd1 if t in temp_options]))
                    if include_null_ddd1 and "(Nulo)" in temp_options: default_selected_ddds.append("(Nulo)")
                    if include_empty_ddd1 and "(Vazio)" in temp_options: default_selected_ddds.append("(Vazio)")

                    selected_ddds = st.multiselect(
                        "DDDs de interesse:",
                        options=temp_options,
                        default=list(set(default_selected_ddds)),
                        key="re_gen_ddd1_select"
                    )
                    re_gen_ia_params['ddd1'] = selected_ddds if selected_ddds else []

                    new_ddd_tag = st.text_input("Adicionar novo DDD:", key="re_gen_new_ddd_tag_input")
                    if new_ddd_tag and st.button("Adicionar Tag (DDD)", key="re_gen_add_ddd_tag_button"):
                        if new_ddd_tag.strip() not in st.session_state.re_gen_custom_tags_ddd1:
                            st.session_state.re_gen_custom_tags_ddd1.append(new_ddd_tag.strip())
                            st.rerun()
                else:
                    re_gen_ia_params['ddd1'] = []

            st.divider()

            # --- Nome Sócio / Razão Social ---
            col1_socio_nome, col2_socio_nome, col3_socio_nome = st.columns([1, 1, 2])
            with col1_socio_nome:
                use_nomes_socios = st.checkbox("Incluir Palavras-Chave (Nome Sócio/Razão Social)", value=False, key="re_gen_use_nomes_socios")
            with col2_socio_nome:
                include_null_socio_nome = st.checkbox("Nulo?", key="re_gen_socio_nome_null") if use_nomes_socios else False
                include_empty_socio_nome = st.checkbox("Vazio?", key="re_gen_socio_nome_empty") if use_nomes_socios else False
            with col3_socio_nome:
                if use_nomes_socios:
                    top_n_socio_nome = st.slider("Top N palavras mais frequentes (Sócio/Razão Social):", min_value=1, max_value=50, value=10, key="re_gen_top_socio_nome")
                    # Use a broader stop word list or refine if needed
                    socio_stop_words = set(unidecode(word.lower()) for word in [
                        "e", "de", "do", "da", "dos", "das", "o", "a", "os", "as", "um", "uma", "uns", "umas",
                        "para", "com", "sem", "em", "no", "na", "nos", "nas", "ao", "aos", "à", "às",
                        "por", "pelo", "pela", "pelos", "pelas", "ou", "nem", "mas", "mais", "menos",
                        "desde", "até", "após", "entre", "sob", "sobre", "ante", "após", "contra",
                        "desde", "durante", "entre", "mediante", "perante", "salvo", "sem", "sob", "sobre", "trás",
                        "s.a", "sa", "ltda", "me", "eireli", "epp", "s.a.", "ltda.", "me.", "eireli.", "epp.",
                        "sa.", "ltda.", "me.", "eireli.", "epp.", "comercio", "servicos", "serviços", "brasil", "brasileira",
                        "administracao", "gestao", "participacoes", "holding", "investimentos", "empreendimentos"
                    ])
                    top_socio_words = get_top_n_words(df_analysis_source, 'nomes_socios', top_n_socio_nome, socio_stop_words, include_null=include_null_socio_nome, include_empty=include_empty_socio_nome)
                    
                    all_socio_options = list(set(top_socio_words + st.session_state.re_gen_custom_tags_nomes_socios))
                    temp_options = [opt for opt in all_socio_options if opt not in ["(Nulo)", "(Vazio)"]]
                    temp_options.sort()
                    if "(Nulo)" in all_socio_options: temp_options.append("(Nulo)")
                    if "(Vazio)" in all_socio_options: temp_options.append("(Vazio)")

                    default_socio_selection = list(set(top_socio_words + [t for t in st.session_state.re_gen_custom_tags_nomes_socios if t in temp_options]))
                    if include_null_socio_nome and "(Nulo)" in temp_options: default_socio_selection.append("(Nulo)")
                    if include_empty_socio_nome and "(Vazio)" in temp_options: default_socio_selection.append("(Vazio)")

                    selected_socio_words = st.multiselect(
                        "Palavras-chave selecionadas (Sócio/Razão Social):",
                        options=temp_options,
                        default=default_socio_selection,
                        key="re_gen_nomes_socios_select"
                    )
                    re_gen_ia_params['nomes_socios'] = selected_socio_words if selected_socio_words else []

                    new_socio_tag = st.text_input("Adicionar nova palavra-chave (Sócio/Razão Social):", key="re_gen_new_socio_tag_input")
                    if new_socio_tag and st.button("Adicionar Tag (Sócio/Razão Social)", key="re_gen_add_socio_tag_button"):
                        if new_socio_tag.strip() not in st.session_state.re_gen_custom_tags_nomes_socios:
                            st.session_state.re_gen_custom_tags_nomes_socios.append(new_socio_tag.strip())
                            st.rerun()
                else:
                    re_gen_ia_params['nomes_socios'] = []

            st.divider()

            # --- Qualificação do Sócio ---
            col1_qs, col2_qs, col3_qs = st.columns([1, 1, 2])
            with col1_qs:
                use_qualificacoes = st.checkbox("Incluir Qualificação do Sócio", value=False, key="re_gen_use_qualificacoes")
            with col2_qs:
                include_null_qs = st.checkbox("Nulo?", key="re_gen_qs_null") if use_qualificacoes else False
                include_empty_qs = st.checkbox("Vazio?", key="re_gen_qs_empty") if use_qualificacoes else False
            with col3_qs:
                if use_qualificacoes:
                    top_n_qs = st.slider("Top N Qualificações de Sócios mais frequentes:", min_value=1, max_value=50, value=10, key="re_gen_top_qs")
                    top_qss = get_unique_values(df_analysis_source, 'qualificacoes', top_n_qs, include_null=include_null_qs, include_empty=include_empty_qs)
                    
                    all_qs_options = list(set(top_qss + st.session_state.re_gen_custom_tags_qualificacoes))
                    temp_options = [opt for opt in all_qs_options if opt not in ["(Nulo)", "(Vazio)"]]
                    temp_options.sort()
                    if "(Nulo)" in all_qs_options: temp_options.append("(Nulo)")
                    if "(Vazio)" in all_qs_options: temp_options.append("(Vazio)")

                    default_qs_selection = list(set(top_qss + [t for t in st.session_state.re_gen_custom_tags_qualificacoes if t in temp_options]))
                    if include_null_qs and "(Nulo)" in temp_options: default_qs_selection.append("(Nulo)")
                    if include_empty_qs and "(Vazio)" in temp_options: default_qs_selection.append("(Vazio)")

                    selected_qss = st.multiselect(
                        "Qualificações de Sócios selecionadas:",
                        options=temp_options,
                        default=list(set(default_qs_selection)),
                        key="re_gen_qualificacoes_select"
                    )
                    re_gen_ia_params['qualificacoes'] = selected_qss if selected_qss else []

                    new_qs_tag = st.text_input("Adicionar nova Qualificação do Sócio:", key="re_gen_new_qs_tag_input")
                    if new_qs_tag and st.button("Adicionar Tag (Qualificação Sócio)", key="re_gen_add_qs_tag_button"):
                        if new_qs_tag.strip() not in st.session_state.re_gen_custom_tags_qualificacoes:
                            st.session_state.re_gen_custom_tags_qualificacoes.append(new_qs_tag.strip())
                            st.rerun()
                else:
                    re_gen_ia_params['qualificacoes'] = []

            st.divider()

            # --- Faixa Etária do Sócio ---
            col1_fe, col2_fe, col3_fe = st.columns([1, 1, 2])
            with col1_fe:
                use_faixas_etarias = st.checkbox("Incluir Faixa Etária do Sócio", value=False, key="re_gen_use_faixas_etarias")
            with col2_fe:
                include_null_fe = st.checkbox("Nulo?", key="re_gen_fe_null") if use_faixas_etarias else False
                include_empty_fe = st.checkbox("Vazio?", key="re_gen_fe_empty") if use_faixas_etarias else False
            with col3_fe:
                if use_faixas_etarias:
                    # Define common age ranges if not enough data or to suggest options
                    base_age_ranges = ["0 a 10 anos", "11 a 20 anos", "21 a 30 anos", "31 a 40 anos", "41 a 50 anos", 
                                    "51 a 60 anos", "61 a 70 anos", "71 a 80 anos", "81 a 90 anos", "91 a 100 anos",
                                    "Acima de 100 anos"]
                    unique_fe_from_df = df_analysis_source['faixas_etarias'].dropna().unique().tolist()

                    all_fe_options = list(set(base_age_ranges + unique_fe_from_df + st.session_state.re_gen_custom_tags_faixas_etarias))
                    temp_options = [opt for opt in all_fe_options if opt not in ["(Nulo)", "(Vazio)"]]
                    temp_options.sort() # Sort alphabetically for consistent display
                    # Add special tags back at the end
                    if "(Nulo)" in all_fe_options: temp_options.append("(Nulo)")
                    if "(Vazio)" in all_fe_options: temp_options.append("(Vazio)")

                    default_selected_fe = list(set([fe for fe in base_age_ranges if fe in temp_options] + unique_fe_from_df + [t for t in st.session_state.re_gen_custom_tags_faixas_etarias if t in temp_options]))
                    if include_null_fe and "(Nulo)" in temp_options: default_selected_fe.append("(Nulo)")
                    if include_empty_fe and "(Vazio)" in temp_options: default_selected_fe.append("(Vazio)")
                    
                    selected_fes = st.multiselect(
                        "Faixas Etárias de Sócios selecionadas:",
                        options=temp_options,
                        default=list(set(default_selected_fe)),
                        key="re_gen_faixas_etarias_select"
                    )
                    re_gen_ia_params['faixas_etarias'] = selected_fes if selected_fes else []

                    new_fe_tag = st.text_input("Adicionar nova Faixa Etária do Sócio:", key="re_gen_new_fe_tag_input")
                    if new_fe_tag and st.button("Adicionar Tag (Faixa Etária Sócio)", key="re_gen_add_fe_tag_button"):
                        if new_fe_tag.strip() not in st.session_state.re_gen_custom_tags_faixas_etarias:
                            st.session_state.re_gen_custom_tags_faixas_etarias.append(new_fe_tag.strip())
                            st.rerun()
                else:
                    re_gen_ia_params['faixas_etarias'] = []

            st.divider()

            current_score = calculate_score(re_gen_ia_params)
            st.session_state.re_gen_current_score = current_score
            st.info(f"Pontuação atual da pesquisa: {current_score} (baseada nos parâmetros ativos).")


            st.markdown("---")
            st.markdown("## 3️⃣ Gerar Novos Leads")

            max_leads_limit = st.slider(
                "🔢 Limite máximo de leads a retornar",
                min_value=100,
                max_value=100000,
                step=100,
                value=1000,
                help="Ajuste o número máximo de leads que deseja retornar da busca SQL."
            )

            if st.button("🚀 Gerar Novos Leads Baseado nos Critérios"):
                if not selected_client_refs:
                    st.error("Por favor, selecione pelo menos um 'Cliente de Referência' na Etapa 1.")
                else:
                    # Collect all CNPJs that exist for the selected client references to exclude them from the new search
                    cnpjs_to_exclude_from_new_search = set(df_analysis_source['cnpj'].dropna().astype(str).tolist())

                    with st.spinner("Gerando consulta SQL e buscando novos leads..."):
                        try:
                            # Generate SQL query excluding existing CNPJs for the selected client references
                            sql_query_obj, query_params = generate_sql_query(
                                re_gen_ia_params, 
                                excluded_cnpjs_set=cnpjs_to_exclude_from_new_search,
                                limit=max_leads_limit
                            )
                            
                            
                            st.session_state.re_gen_current_sql_query = str(sql_query_obj)
                            
                            
                            engine = create_engine(DATABASE_URL)
                            with engine.connect() as conn:
                                df_new_leads = pd.read_sql(sql_query_obj, conn, params=query_params)
                            
                            st.session_state.re_gen_df_new_leads_found = df_new_leads

                            if not df_new_leads.empty:
                                st.success(f"Encontrados {len(df_new_leads)} novos leads! 🎉")
                                st.dataframe(df_new_leads)

                                # Adicionar opções de exportação/salvamento
                                st.markdown("### Exportar/Salvar Novos Leads")

                                col_excel, col_csv, col_db = st.columns(3)
                                with col_excel:
                                    st.download_button(
                                        label="Exportar para Excel",
                                        data=to_excel(df_new_leads),
                                        file_name=f"novos_leads_{'_'.join(selected_client_refs)}.xlsx",
                                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                        key="re_gen_download_excel"
                                    )
                                with col_csv:
                                    st.download_button(
                                        label="Exportar para CSV",
                                        data=df_new_leads.to_csv(index=False).encode('utf-8'),
                                        file_name=f"novos_leads_{'_'.join(selected_client_refs)}.csv",
                                        mime="text/csv",
                                        key="re_gen_download_csv"
                                    )
                                with col_db:
                                    if st.button("Salvar no Banco de Dados (tb_leads_gerados)", key="re_gen_save_db"):
                                        if len(selected_client_refs) > 1:
                                            st.error("Atenção: Para salvar no banco de dados, você deve selecionar apenas UM 'Cliente de Referência'.")
                                        elif not selected_client_refs:
                                            st.error("Por favor, selecione um 'Cliente de Referência' para salvar.")
                                        else:
                                            cliente_ref_to_save = selected_client_refs[0]
                                            
                                            with st.spinner(f"Salvando leads para o cliente '{cliente_ref_to_save}' no banco de dados..."):
                                                try:
                                                    engine = create_engine(DATABASE_URL)
                                                    ensure_leads_table_exists(df_new_leads, engine=engine) # Ensure table exists and has PK

                                                    # Fetch existing CNPJs for the specific client_referencia before saving
                                                    existing_cnpjs_for_this_client_query = text(
                                                        f"SELECT cnpj FROM tb_leads_gerados WHERE cliente_referencia = :cliente_ref_param"
                                                    )
                                                    existing_cnpjs_df = pd.read_sql(existing_cnpjs_for_this_client_query, engine, 
                                                                                    params={'cliente_ref_param': cliente_ref_to_save})
                                                    existing_cnpjs_set_for_saving = set(existing_cnpjs_df['cnpj'].dropna().astype(str).tolist())
                                                    
                                                    # Filter out CNPJs that already exist for this client_referencia
                                                    df_leads_to_save = df_new_leads[
                                                        ~df_new_leads['cnpj'].isin(existing_cnpjs_set_for_saving)
                                                    ].copy()

                                                    if not df_leads_to_save.empty:
                                                        df_leads_to_save['pontuacao'] = current_score
                                                        df_leads_to_save['data_geracao'] = datetime.now()
                                                        df_leads_to_save['cliente_referencia'] = cliente_ref_to_save

                                                        # Ensure string columns are treated as objects and fillna for DB insertion
                                                        for col in df_leads_to_save.columns:
                                                            if df_leads_to_save[col].dtype == 'object':
                                                                df_leads_to_save[col] = df_leads_to_save[col].fillna('').replace('None', '')
                                                            elif pd.api.types.is_datetime64_any_dtype(df_leads_to_save[col]):
                                                                df_leads_to_save[col] = df_leads_to_save[col].dt.date # Convert to date object for DB
                                                                
                                                        df_leads_to_save.to_sql(
                                                            'tb_leads_gerados',
                                                            con=engine,
                                                            if_exists='append',
                                                            index=False
                                                        )
                                                        st.success(f"{len(df_leads_to_save)} novos leads salvos com sucesso na tabela 'tb_leads_gerados' para o cliente '{cliente_ref_to_save}'.")
                                                        if len(df_new_leads) > len(df_leads_to_save):
                                                            st.info(f"{len(df_new_leads) - len(df_leads_to_save)} leads já existiam para o cliente '{cliente_ref_to_save}' e foram ignorados.")
                                                    else:
                                                        st.info("Nenhum novo lead para salvar (todos já existem para este cliente de referência).")

                                                except Exception as e:
                                                    st.error(f"Erro ao salvar leads no banco de dados: {e}")
                                                    st.exception(e)
                            else:
                                st.info("Nenhum lead encontrado com os critérios selecionados. Tente ajustar os filtros.")

                        except Exception as e:
                            st.error(f"Erro ao gerar ou executar a consulta SQL: {e}")
                            st.exception(e)
    else:
        st.info("Por favor, selecione um 'Cliente de Referência' para começar a análise.")

st.markdown("---")
if st.button("⬅️ Voltar para IA de Geração Original"):
    st.switch_page("3_IA_Generator.py") # Assuming the original page is named 3_IA_Generator.py