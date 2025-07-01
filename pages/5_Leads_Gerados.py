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
        # Check against the original column names for null/empty checks
        if include_null and df['cod_cnae_principal'].isna().any():
            if ("(Nulo)", "(Nulo)") not in common_cnaes:
                common_cnaes.append(("(Nulo)", "(Nulo)"))
        if include_empty and (df['cod_cnae_principal'].astype(str).str.strip() == "").any():
            if ("(Vazio)", "(Vazio)") not in common_cnaes:
                common_cnaes.append(("(Vazio)", "(Vazio)"))
    if cnae_type == 'secundario' or cnae_type == 'ambos':
        # Check against the original column names for null/empty checks
        if include_null and df['cod_cnae_secundario'].isna().any():
            if ("(Nulo)", "(Nulo)") not in common_cnaes:
                common_cnaes.append(("(Nulo)", "(Nulo)"))
        if include_empty and (df['cod_cnae_secundario'].astype(str).str.strip() == "").any():
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
                    param_counter += 1
                    param_conditions.append(f"({' OR '.join(placeholders_cond)})")
                
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
            
            # Type conversion for specific columns
            if 'data_inicio_atividade' in df_temp.columns:
                df_temp['data_inicio_atividade'] = pd.to_datetime(df_temp['data_inicio_atividade'], errors='coerce').dt.date
            if 'capital_social' in df_temp.columns:
                df_temp['capital_social'] = pd.to_numeric(df_temp['capital_social'], errors='coerce')

            # Use the correct column names for table creation based on how they should be in DB
            # The actual DataFrame passed to to_sql will need to be renamed to match these.
            df_temp_for_db_schema = df_temp.rename(columns={
                'cod_cnae_principal': 'cnae_principal_cod',
                'cod_cnae_secundario': 'cnae_secundario_cod'
            })


            df_temp_for_db_schema.head(0).to_sql(table_name, con=engine, if_exists='append', index=False)
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
                    top_bairros = get_unique_values(df_analysis_source, 'bairro', top_n_bairro, include_null=include_null_bairro, include_empty=include_empty_bairro)
                    
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
                include_null_cnae_p = st.checkbox("Nulo?", key="re_gen_cnae_principal_null") if use_cnae_principal else False
                include_empty_cnae_p = st.checkbox("Vazio?", key="re_gen_cnae_principal_empty") if use_cnae_principal else False
            with col3_cnae_p:
                if use_cnae_principal:
                    top_n_cnae_p = st.slider("Top N CNAEs Principais mais frequentes:", min_value=1, max_value=50, value=10, key="re_gen_top_cnae_principal")
                    top_cnaes_p = get_top_n_cnaes(df_analysis_source, 'principal', top_n_cnae_p, include_null=include_null_cnae_p, include_empty=include_empty_cnae_p)
                    
                    all_cnae_p_options = list(set(top_cnaes_p + [tuple(t) for t in st.session_state.re_gen_custom_tags_cnae_principal])) # Convert list of lists to list of tuples
                    temp_options = [opt for opt in all_cnae_p_options if opt not in [("(Nulo)", "(Nulo)"), ("(Vazio)", "(Vazio)")]]
                    temp_options.sort(key=lambda x: x[0])
                    if ("(Nulo)", "(Nulo)") in all_cnae_p_options: temp_options.append(("(Nulo)", "(Nulo)"))
                    if ("(Vazio)", "(Vazio)") in all_cnae_p_options: temp_options.append(("(Vazio)", "(Vazio)"))
                    
                    default_cnae_p_selection = list(set(top_cnaes_p + [t for t in [tuple(x) for x in st.session_state.re_gen_custom_tags_cnae_principal] if t in temp_options]))
                    if include_null_cnae_p and ("(Nulo)", "(Nulo)") in temp_options: default_cnae_p_selection.append(("(Nulo)", "(Nulo)"))
                    if include_empty_cnae_p and ("(Vazio)", "(Vazio)") in temp_options: default_cnae_p_selection.append(("(Vazio)", "(Vazio)"))
                    
                    selected_cnaes_p = st.multiselect(
                        "CNAEs Principais selecionados (código - descrição):",
                        options=[f"{code} - {desc}" for code, desc in temp_options],
                        format_func=lambda x: x,
                        default=[f"{code} - {desc}" for code, desc in default_cnae_p_selection],
                        key="re_gen_cnae_principal_select"
                    )
                    
                    re_gen_ia_params['cod_cnae_principal'] = [tuple(item.split(' - ', 1)) for item in selected_cnaes_p] if selected_cnaes_p else []

                    new_cnae_p_code = st.text_input("Adicionar novo CNAE Principal (código):", key="re_gen_new_cnae_p_code_input")
                    new_cnae_p_desc = st.text_input("Adicionar nova CNAE Principal (descrição):", key="re_gen_new_cnae_p_desc_input")
                    if new_cnae_p_code and new_cnae_p_desc and st.button("Adicionar Tag (CNAE Principal)", key="re_gen_add_cnae_p_tag_button"):
                        new_tag = (new_cnae_p_code.strip(), new_cnae_p_desc.strip())
                        if list(new_tag) not in st.session_state.re_gen_custom_tags_cnae_principal: # Check as list because it's stored as list of lists
                            st.session_state.re_gen_custom_tags_cnae_principal.append(list(new_tag)) # Store as list
                        st.rerun()
                else:
                    re_gen_ia_params['cod_cnae_principal'] = []
            st.divider()

            # --- CNAE Secundário ---
            col1_cnae_s, col2_cnae_s, col3_cnae_s = st.columns([1, 1, 2])
            with col1_cnae_s:
                use_cnae_secundario = st.checkbox("Incluir CNAE Secundário", value=False, key="re_gen_use_cnae_secundario")
            with col2_cnae_s:
                include_null_cnae_s = st.checkbox("Nulo?", key="re_gen_cnae_secundario_null") if use_cnae_secundario else False
                include_empty_cnae_s = st.checkbox("Vazio?", key="re_gen_cnae_secundario_empty") if use_cnae_secundario else False
            with col3_cnae_s:
                if use_cnae_secundario:
                    top_n_cnae_s = st.slider("Top N CNAEs Secundários mais frequentes:", min_value=1, max_value=50, value=10, key="re_gen_top_cnae_secundario")
                    top_cnaes_s = get_top_n_cnaes(df_analysis_source, 'secundario', top_n_cnae_s, include_null=include_null_cnae_s, include_empty=include_empty_cnae_s)

                    all_cnae_s_options = list(set(top_cnaes_s + [tuple(t) for t in st.session_state.re_gen_custom_tags_cnae_secundario]))
                    temp_options = [opt for opt in all_cnae_s_options if opt not in [("(Nulo)", "(Nulo)"), ("(Vazio)", "(Vazio)")]]
                    temp_options.sort(key=lambda x: x[0])
                    if ("(Nulo)", "(Nulo)") in all_cnae_s_options: temp_options.append(("(Nulo)", "(Nulo)"))
                    if ("(Vazio)", "(Vazio)") in all_cnae_s_options: temp_options.append(("(Vazio)", "(Vazio)"))

                    default_cnae_s_selection = list(set(top_cnaes_s + [t for t in [tuple(x) for x in st.session_state.re_gen_custom_tags_cnae_secundario] if t in temp_options]))
                    if include_null_cnae_s and ("(Nulo)", "(Nulo)") in temp_options: default_cnae_s_selection.append(("(Nulo)", "(Nulo)"))
                    if include_empty_cnae_s and ("(Vazio)", "(Vazio)") in temp_options: default_cnae_s_selection.append(("(Vazio)", "(Vazio)"))

                    selected_cnaes_s = st.multiselect(
                        "CNAEs Secundários selecionados (código - descrição):",
                        options=[f"{code} - {desc}" for code, desc in temp_options],
                        format_func=lambda x: x,
                        default=[f"{code} - {desc}" for code, desc in default_cnae_s_selection],
                        key="re_gen_cnae_secundario_select"
                    )
                    re_gen_ia_params['cod_cnae_secundario'] = [tuple(item.split(' - ', 1)) for item in selected_cnaes_s] if selected_cnaes_s else []

                    new_cnae_s_code = st.text_input("Adicionar novo CNAE Secundário (código):", key="re_gen_new_cnae_s_code_input")
                    new_cnae_s_desc = st.text_input("Adicionar nova CNAE Secundário (descrição):", key="re_gen_new_cnae_s_desc_input")
                    if new_cnae_s_code and new_cnae_s_desc and st.button("Adicionar Tag (CNAE Secundário)", key="re_gen_add_cnae_s_tag_button"):
                        new_tag = (new_cnae_s_code.strip(), new_cnae_s_desc.strip())
                        if list(new_tag) not in st.session_state.re_gen_custom_tags_cnae_secundario:
                            st.session_state.re_gen_custom_tags_cnae_secundario.append(list(new_tag))
                        st.rerun()
                else:
                    re_gen_ia_params['cod_cnae_secundario'] = []
            st.divider()

            # --- Data de Início de Atividade ---
            col1_dt_ini, col2_dt_ini = st.columns([1, 3])
            with col1_dt_ini:
                use_data_inicio_atividade = st.checkbox("Incluir Data de Início de Atividade", value=False, key="re_gen_use_data_inicio_atividade")
            with col2_dt_ini:
                if use_data_inicio_atividade:
                    min_date_val = df_analysis_source['data_inicio_atividade'].min() if not df_analysis_source['data_inicio_atividade'].empty else datetime(1900, 1, 1).date()
                    max_date_val = df_analysis_source['data_inicio_atividade'].max() if not df_analysis_source['data_inicio_atividade'].empty else datetime.now().date()
                    
                    if pd.isna(min_date_val): min_date_val = datetime(1900, 1, 1).date()
                    if pd.isna(max_date_val): max_date_val = datetime.now().date()

                    min_dt_filter, max_dt_filter = st.slider(
                        "Faixa de Data de Início de Atividade:",
                        min_value=min_date_val,
                        max_value=max_date_val,
                        value=(min_date_val, max_date_val),
                        format="DD/MM/YYYY",
                        key="re_gen_data_inicio_atividade_range"
                    )
                    re_gen_ia_params['data_inicio_atividade'] = (min_dt_filter, max_dt_filter)
                else:
                    re_gen_ia_params['data_inicio_atividade'] = None
            st.divider()

            # --- Capital Social ---
            col1_cs, col2_cs = st.columns([1, 3])
            with col1_cs:
                use_capital_social = st.checkbox("Incluir Capital Social", value=False, key="re_gen_use_capital_social")
            with col2_cs:
                if use_capital_social:
                    min_cs_val = df_analysis_source['capital_social'].min() if not df_analysis_source['capital_social'].empty else 0.0
                    max_cs_val = df_analysis_source['capital_social'].max() if not df_analysis_source['capital_social'].empty else 10000000.0

                    # Handle NaN values for min/max and ensure they are numbers
                    if pd.isna(min_cs_val): min_cs_val = 0.0
                    if pd.isna(max_cs_val): max_cs_val = 10000000.0 # A reasonable default max

                    # Adjust for cases where min_cs_val might be greater than max_cs_val after NaNs
                    if min_cs_val > max_cs_val:
                        min_cs_val = max_cs_val * 0.9 if max_cs_val > 0 else 0.0

                    min_cs_filter, max_cs_filter = st.slider(
                        "Faixa de Capital Social:",
                        min_value=float(min_cs_val),
                        max_value=float(max_cs_val),
                        value=(float(min_cs_val), float(max_cs_val)),
                        key="re_gen_capital_social_range"
                    )
                    re_gen_ia_params['capital_social'] = (min_cs_filter, max_cs_filter)
                else:
                    re_gen_ia_params['capital_social'] = None
            st.divider()

            # --- Porte da Empresa ---
            col1_porte, col2_porte, col3_porte = st.columns([1, 1, 2])
            with col1_porte:
                use_porte_empresa = st.checkbox("Incluir Porte da Empresa", value=True, key="re_gen_use_porte_empresa")
            with col2_porte:
                include_null_porte = st.checkbox("Nulo?", key="re_gen_porte_empresa_null") if use_porte_empresa else False
                include_empty_porte = st.checkbox("Vazio?", key="re_gen_porte_empresa_empty") if use_porte_empresa else False
            with col3_porte:
                if use_porte_empresa:
                    top_n_porte = st.slider("Top N Portes de Empresa mais frequentes:", min_value=1, max_value=10, value=3, key="re_gen_top_porte_empresa")
                    top_portes = get_unique_values(df_analysis_source, 'porte_empresa', top_n_porte, include_null=include_null_porte, include_empty=include_empty_porte)
                    
                    all_porte_options = list(set(top_portes + st.session_state.re_gen_custom_tags_porte_empresa))
                    temp_options = [opt for opt in all_porte_options if opt not in ["(Nulo)", "(Vazio)"]]
                    temp_options.sort()
                    if "(Nulo)" in all_porte_options: temp_options.append("(Nulo)")
                    if "(Vazio)" in all_porte_options: temp_options.append("(Vazio)")

                    default_porte_selection = list(set(top_portes + [t for t in st.session_state.re_gen_custom_tags_porte_empresa if t in temp_options]))
                    if include_null_porte and "(Nulo)" in temp_options: default_porte_selection.append("(Nulo)")
                    if include_empty_porte and "(Vazio)" in temp_options: default_porte_selection.append("(Vazio)")
                    selected_portes = st.multiselect(
                        "Portes de Empresa selecionados:",
                        options=temp_options,
                        default=default_porte_selection,
                        key="re_gen_porte_empresa_select"
                    )
                    re_gen_ia_params['porte_empresa'] = selected_portes if selected_portes else []

                    new_porte_tag = st.text_input("Adicionar novo Porte da Empresa:", key="re_gen_new_porte_empresa_tag_input")
                    if new_porte_tag and st.button("Adicionar Tag (Porte)", key="re_gen_add_porte_empresa_tag_button"):
                        if new_porte_tag.strip() not in st.session_state.re_gen_custom_tags_porte_empresa:
                            st.session_state.re_gen_custom_tags_porte_empresa.append(new_porte_tag.strip())
                        st.rerun()
                else:
                    re_gen_ia_params['porte_empresa'] = []
            st.divider()

            # --- Natureza Jurídica ---
            col1_nat_jur, col2_nat_jur, col3_nat_jur = st.columns([1, 1, 2])
            with col1_nat_jur:
                use_natureza_juridica = st.checkbox("Incluir Natureza Jurídica", value=False, key="re_gen_use_natureza_juridica")
            with col2_nat_jur:
                include_null_nat_jur = st.checkbox("Nulo?", key="re_gen_natureza_juridica_null") if use_natureza_juridica else False
                include_empty_nat_jur = st.checkbox("Vazio?", key="re_gen_natureza_juridica_empty") if use_natureza_juridica else False
            with col3_nat_jur:
                if use_natureza_juridica:
                    top_n_nat_jur = st.slider("Top N Naturezas Jurídicas mais frequentes:", min_value=1, max_value=50, value=10, key="re_gen_top_natureza_juridica")
                    top_nat_jurs = get_unique_values(df_analysis_source, 'natureza_juridica', top_n_nat_jur, include_null=include_null_nat_jur, include_empty=include_empty_nat_jur)

                    all_nat_jur_options = list(set(top_nat_jurs + st.session_state.re_gen_custom_tags_natureza_juridica))
                    temp_options = [opt for opt in all_nat_jur_options if opt not in ["(Nulo)", "(Vazio)"]]
                    temp_options.sort()
                    if "(Nulo)" in all_nat_jur_options: temp_options.append("(Nulo)")
                    if "(Vazio)" in all_nat_jur_options: temp_options.append("(Vazio)")
                    
                    default_nat_jur_selection = list(set(top_nat_jurs + [t for t in st.session_state.re_gen_custom_tags_natureza_juridica if t in temp_options]))
                    if include_null_nat_jur and "(Nulo)" in temp_options: default_nat_jur_selection.append("(Nulo)")
                    if include_empty_nat_jur and "(Vazio)" in temp_options: default_nat_jur_selection.append("(Vazio)")
                    selected_nat_jurs = st.multiselect(
                        "Naturezas Jurídicas selecionadas:",
                        options=temp_options,
                        default=default_nat_jur_selection,
                        key="re_gen_natureza_juridica_select"
                    )
                    re_gen_ia_params['natureza_juridica'] = selected_nat_jurs if selected_nat_jurs else []

                    new_nat_jur_tag = st.text_input("Adicionar nova Natureza Jurídica:", key="re_gen_new_natureza_juridica_tag_input")
                    if new_nat_jur_tag and st.button("Adicionar Tag (Natureza Jurídica)", key="re_gen_add_natureza_juridica_tag_button"):
                        if new_nat_jur_tag.strip() not in st.session_state.re_gen_custom_tags_natureza_juridica:
                            st.session_state.re_gen_custom_tags_natureza_juridica.append(new_nat_jur_tag.strip())
                        st.rerun()
                else:
                    re_gen_ia_params['natureza_juridica'] = []
            st.divider()

            # --- Opção pelo Simples ---
            col1_simples, col2_simples = st.columns([1, 3])
            with col1_simples:
                use_opcao_simples = st.checkbox("Incluir Opção pelo Simples", value=False, key="re_gen_use_opcao_simples")
            with col2_simples:
                if use_opcao_simples:
                    opcao_simples_options = get_unique_values(df_analysis_source, 'opcao_simples', include_null=True, include_empty=True)
                    selected_opcao_simples = st.multiselect(
                        "Opção pelo Simples Nacional:",
                        options=[opt for opt in opcao_simples_options if opt is not None and opt != ''],
                        default=[opt for opt in opcao_simples_options if opt is not None and opt != ''],
                        key="re_gen_opcao_simples_select"
                    )
                    re_gen_ia_params['opcao_simples'] = selected_opcao_simples if selected_opcao_simples else []
                else:
                    re_gen_ia_params['opcao_simples'] = []
            st.divider()

            # --- Opção pelo MEI ---
            col1_mei, col2_mei = st.columns([1, 3])
            with col1_mei:
                use_opcao_mei = st.checkbox("Incluir Opção pelo MEI", value=False, key="re_gen_use_opcao_mei")
            with col2_mei:
                if use_opcao_mei:
                    opcao_mei_options = get_unique_values(df_analysis_source, 'opcao_mei', include_null=True, include_empty=True)
                    selected_opcao_mei = st.multiselect(
                        "Opção pelo MEI:",
                        options=[opt for opt in opcao_mei_options if opt is not None and opt != ''],
                        default=[opt for opt in opcao_mei_options if opt is not None and opt != ''],
                        key="re_gen_opcao_mei_select"
                    )
                    re_gen_ia_params['opcao_mei'] = selected_opcao_mei if selected_opcao_mei else []
                else:
                    re_gen_ia_params['opcao_mei'] = []
            st.divider()
            
            # --- DDD1 ---
            col1_ddd, col2_ddd, col3_ddd = st.columns([1, 1, 2])
            with col1_ddd:
                use_ddd1 = st.checkbox("Incluir DDD", value=False, key="re_gen_use_ddd1")
            with col2_ddd:
                include_null_ddd1 = st.checkbox("Nulo?", key="re_gen_ddd1_null") if use_ddd1 else False
                include_empty_ddd1 = st.checkbox("Vazio?", key="re_gen_ddd1_empty") if use_ddd1 else False
            with col3_ddd:
                if use_ddd1:
                    top_n_ddd1 = st.slider("Top N DDDs mais frequentes:", min_value=1, max_value=50, value=10, key="re_gen_top_ddd1")
                    top_ddd1s = get_unique_values(df_analysis_source, 'ddd1', top_n_ddd1, include_null=include_null_ddd1, include_empty=include_empty_ddd1)
                    
                    all_ddd1_options = list(set(top_ddd1s + st.session_state.re_gen_custom_tags_ddd1))
                    temp_options = [opt for opt in all_ddd1_options if opt not in ["(Nulo)", "(Vazio)"]]
                    temp_options.sort()
                    if "(Nulo)" in all_ddd1_options: temp_options.append("(Nulo)")
                    if "(Vazio)" in all_ddd1_options: temp_options.append("(Vazio)")

                    default_ddd1_selection = list(set(top_ddd1s + [t for t in st.session_state.re_gen_custom_tags_ddd1 if t in temp_options]))
                    if include_null_ddd1 and "(Nulo)" in temp_options: default_ddd1_selection.append("(Nulo)")
                    if include_empty_ddd1 and "(Vazio)" in temp_options: default_ddd1_selection.append("(Vazio)")
                    selected_ddd1s = st.multiselect(
                        "DDDs selecionados:",
                        options=temp_options,
                        default=default_ddd1_selection,
                        key="re_gen_ddd1_select"
                    )
                    re_gen_ia_params['ddd1'] = selected_ddd1s if selected_ddd1s else []

                    new_ddd1_tag = st.text_input("Adicionar novo DDD:", key="re_gen_new_ddd1_tag_input")
                    if new_ddd1_tag and st.button("Adicionar Tag (DDD)", key="re_gen_add_ddd1_tag_button"):
                        if new_ddd1_tag.strip() not in st.session_state.re_gen_custom_tags_ddd1:
                            st.session_state.re_gen_custom_tags_ddd1.append(new_ddd1_tag.strip())
                        st.rerun()
                else:
                    re_gen_ia_params['ddd1'] = []
            st.divider()

            # --- Nomes dos Sócios ---
            col1_socios, col2_socios, col3_socios = st.columns([1, 1, 2])
            with col1_socios:
                use_nomes_socios = st.checkbox("Incluir Nomes de Sócios", value=False, key="re_gen_use_nomes_socios")
            with col2_socios:
                include_null_socios = st.checkbox("Nulo?", key="re_gen_nomes_socios_null") if use_nomes_socios else False
                include_empty_socios = st.checkbox("Vazio?", key="re_gen_nomes_socios_empty") if use_nomes_socios else False
            with col3_socios:
                if use_nomes_socios:
                    top_n_socios = st.slider("Top N Partes de Nomes de Sócios mais frequentes:", min_value=1, max_value=50, value=10, key="re_gen_top_nomes_socios")
                    stop_words_socios = set(unidecode(word.lower()) for word in ["de", "da", "do", "dos", "das", "e", "com", "sem"])
                    top_socios_words = get_top_n_words(df_analysis_source, 'nomes_socios', top_n_socios, stop_words_socios, include_null=include_null_socios, include_empty=include_empty_socios)
                    
                    all_socios_options = list(set(top_socios_words + st.session_state.re_gen_custom_tags_nomes_socios))
                    temp_options = [opt for opt in all_socios_options if opt not in ["(Nulo)", "(Vazio)"]]
                    temp_options.sort()
                    if "(Nulo)" in all_socios_options: temp_options.append("(Nulo)")
                    if "(Vazio)" in all_socios_options: temp_options.append("(Vazio)")
                    
                    default_socios_selection = list(set(top_socios_words + [t for t in st.session_state.re_gen_custom_tags_nomes_socios if t in temp_options]))
                    if include_null_socios and "(Nulo)" in temp_options: default_socios_selection.append("(Nulo)")
                    if include_empty_socios and "(Vazio)" in temp_options: default_socios_selection.append("(Vazio)")
                    selected_socios_words = st.multiselect(
                        "Partes de Nomes de Sócios selecionadas:",
                        options=temp_options,
                        default=default_socios_selection,
                        key="re_gen_nomes_socios_select"
                    )
                    re_gen_ia_params['nomes_socios'] = selected_socios_words if selected_socios_words else []

                    new_socios_tag = st.text_input("Adicionar nova Parte de Nome de Sócio:", key="re_gen_new_nomes_socios_tag_input")
                    if new_socios_tag and st.button("Adicionar Tag (Sócio)", key="re_gen_add_nomes_socios_tag_button"):
                        if new_socios_tag.strip() not in st.session_state.re_gen_custom_tags_nomes_socios:
                            st.session_state.re_gen_custom_tags_nomes_socios.append(new_socios_tag.strip())
                        st.rerun()
                else:
                    re_gen_ia_params['nomes_socios'] = []
            st.divider()

            # --- Qualificações ---
            col1_qual, col2_qual, col3_qual = st.columns([1, 1, 2])
            with col1_qual:
                use_qualificacoes = st.checkbox("Incluir Qualificações de Sócios", value=False, key="re_gen_use_qualificacoes")
            with col2_qual:
                include_null_qual = st.checkbox("Nulo?", key="re_gen_qualificacoes_null") if use_qualificacoes else False
                include_empty_qual = st.checkbox("Vazio?", key="re_gen_qualificacoes_empty") if use_qualificacoes else False
            with col3_qual:
                if use_qualificacoes:
                    top_n_qual = st.slider("Top N Qualificações mais frequentes:", min_value=1, max_value=50, value=10, key="re_gen_top_qualificacoes")
                    top_qualificacoes = get_unique_values(df_analysis_source, 'qualificacoes', top_n_qual, include_null=include_null_qual, include_empty=include_empty_qual)
                    
                    all_qual_options = list(set(top_qualificacoes + st.session_state.re_gen_custom_tags_qualificacoes))
                    temp_options = [opt for opt in all_qual_options if opt not in ["(Nulo)", "(Vazio)"]]
                    temp_options.sort()
                    if "(Nulo)" in all_qual_options: temp_options.append("(Nulo)")
                    if "(Vazio)" in all_qual_options: temp_options.append("(Vazio)")

                    default_qual_selection = list(set(top_qualificacoes + [t for t in st.session_state.re_gen_custom_tags_qualificacoes if t in temp_options]))
                    if include_null_qual and "(Nulo)" in temp_options: default_qual_selection.append("(Nulo)")
                    if include_empty_qual and "(Vazio)" in temp_options: default_qual_selection.append("(Vazio)")
                    selected_qualificacoes = st.multiselect(
                        "Qualificações selecionadas:",
                        options=temp_options,
                        default=default_qual_selection,
                        key="re_gen_qualificacoes_select"
                    )
                    re_gen_ia_params['qualificacoes'] = selected_qualificacoes if selected_qualificacoes else []

                    new_qual_tag = st.text_input("Adicionar nova Qualificação:", key="re_gen_new_qualificacoes_tag_input")
                    if new_qual_tag and st.button("Adicionar Tag (Qualificação)", key="re_gen_add_qualificacoes_tag_button"):
                        if new_qual_tag.strip() not in st.session_state.re_gen_custom_tags_qualificacoes:
                            st.session_state.re_gen_custom_tags_qualificacoes.append(new_qual_tag.strip())
                        st.rerun()
                else:
                    re_gen_ia_params['qualificacoes'] = []
            st.divider()

            # --- Faixas Etárias ---
            col1_faixa_etaria, col2_faixa_etaria, col3_faixa_etaria = st.columns([1, 1, 2])
            with col1_faixa_etaria:
                use_faixas_etarias = st.checkbox("Incluir Faixas Etárias de Sócios", value=False, key="re_gen_use_faixas_etarias")
            with col2_faixa_etaria:
                include_null_faixa_etaria = st.checkbox("Nulo?", key="re_gen_faixas_etarias_null") if use_faixas_etarias else False
                include_empty_faixa_etaria = st.checkbox("Vazio?", key="re_gen_faixas_etarias_empty") if use_faixas_etarias else False
            with col3_faixa_etaria:
                if use_faixas_etarias:
                    top_n_faixa_etaria = st.slider("Top N Faixas Etárias mais frequentes:", min_value=1, max_value=50, value=10, key="re_gen_top_faixas_etarias")
                    top_faixas_etarias = get_unique_values(df_analysis_source, 'faixas_etarias', top_n_faixa_etaria, include_null=include_null_faixa_etaria, include_empty=include_empty_faixa_etaria)
                    
                    all_faixa_etaria_options = list(set(top_faixas_etarias + st.session_state.re_gen_custom_tags_faixas_etarias))
                    temp_options = [opt for opt in all_faixa_etaria_options if opt not in ["(Nulo)", "(Vazio)"]]
                    temp_options.sort()
                    if "(Nulo)" in all_faixa_etaria_options: temp_options.append("(Nulo)")
                    if "(Vazio)" in all_faixa_etaria_options: temp_options.append("(Vazio)")

                    default_faixa_etaria_selection = list(set(top_faixas_etarias + [t for t in st.session_state.re_gen_custom_tags_faixas_etarias if t in temp_options]))
                    if include_null_faixa_etaria and "(Nulo)" in temp_options: default_faixa_etaria_selection.append("(Nulo)")
                    if include_empty_faixa_etaria and "(Vazio)" in temp_options: default_faixa_etaria_selection.append("(Vazio)")
                    selected_faixas_etarias = st.multiselect(
                        "Faixas Etárias selecionadas:",
                        options=temp_options,
                        default=default_faixa_etaria_selection,
                        key="re_gen_faixas_etarias_select"
                    )
                    re_gen_ia_params['faixas_etarias'] = selected_faixas_etarias if selected_faixas_etarias else []

                    new_faixa_etaria_tag = st.text_input("Adicionar nova Faixa Etária:", key="re_gen_new_faixas_etarias_tag_input")
                    if new_faixa_etaria_tag and st.button("Adicionar Tag (Faixa Etária)", key="re_gen_add_faixas_etarias_tag_button"):
                        if new_faixa_etaria_tag.strip() not in st.session_state.re_gen_custom_tags_faixas_etarias:
                            st.session_state.re_gen_custom_tags_faixas_etarias.append(new_faixa_etaria_tag.strip())
                        st.rerun()
                else:
                    re_gen_ia_params['faixas_etarias'] = []
            st.divider()

            # --- Limite de leads a gerar ---
            st.subheader("Configuração de Geração")
            lead_limit = st.slider("Número máximo de leads a gerar:", min_value=100, max_value=5000, value=1000, step=100, key="re_gen_lead_limit")
            
            st.markdown("---")
            st.markdown("## 3️⃣ Geração e Análise de Novos Leads")

            if st.button("🚀 Gerar Novos Leads", key="re_gen_generate_leads_button", type="primary"):
                if not re_gen_ia_params:
                    st.warning("Selecione pelo menos um critério para gerar novos leads.")
                else:
                    st.session_state.re_gen_current_score = calculate_score(re_gen_ia_params)
                    st.info(f"Pontuação de similaridade do perfil de busca: {st.session_state.re_gen_current_score} pontos.")

                    try:
                        sql_query, query_params = generate_sql_query(re_gen_ia_params, excluded_cnpjs_set=st.session_state.re_gen_existing_cnpjs, limit=lead_limit)
                        st.session_state.re_gen_current_sql_query = str(sql_query) # Store for display, CORREÇÃO AQUI
                        
                        engine = create_engine(DATABASE_URL)
                        with engine.connect() as conn:
                            st.write("Executando consulta SQL...")
                            st.code(str(sql_query), language="sql") # Display the generated SQL, CORREÇÃO AQUI
                            
                            df_new_leads = pd.read_sql(sql_query, conn, params=query_params)
                            st.session_state.re_gen_df_new_leads_found = df_new_leads

                            if not df_new_leads.empty:
                                st.success(f"Encontrados {len(df_new_leads)} novos leads.")
                                st.dataframe(df_new_leads, use_container_width=True)

                                # Adicionar botão de download
                                st.download_button(
                                    label="📥 Baixar Novos Leads (Excel)",
                                    data=to_excel(df_new_leads),
                                    file_name=f"novos_leads_gerados_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key="re_gen_download_button"
                                )

                            else:
                                st.warning("Nenhum novo lead encontrado com os critérios especificados.")

                    except Exception as e:
                        st.error(f"Erro ao gerar ou executar a consulta SQL: {e}")
                        st.write("Detalhes do erro:")
                        st.exception(e)
    else:
        st.info("Por favor, selecione um 'Cliente de Referência' para começar a análise.")

st.markdown("---")
if st.button("⬅️ Voltar para IA de Geração Original"):
    st.switch_page("3_IA_Generator.py") # Assuming the original page is named 3_IA_Generator.py

# --- Bloco de Salvamento de Leads ---
st.markdown("## 💾 Salvar Novos Leads")

save_client_ref = st.text_input("Nome do Cliente de Referência para salvar estes novos leads:", key="re_gen_save_client_ref")

if st.button("💾 Salvar Novos Leads no Banco de Dados"):
    try:
        df_leads_to_save = st.session_state.get("re_gen_df_new_leads_found", pd.DataFrame()).copy()
        if not save_client_ref:
            st.error("⚠️ Por favor, insira um nome válido para o cliente de referência.")
        elif df_leads_to_save.empty:
            st.warning("⚠️ Não há leads novos para salvar.")
        else:
            df_leads_to_save['cliente_referencia'] = save_client_ref
            df_leads_to_save['data_geracao'] = datetime.now().date()
            df_leads_to_save['pontuacao'] = st.session_state.get("re_gen_current_score", 0)

            engine = create_engine(DATABASE_URL)
            ensure_leads_table_exists(df_leads_to_save, engine=engine)

            with engine.begin() as conn:
                existing_query = text("SELECT cnpj FROM tb_leads_gerados WHERE cliente_referencia = :ref")
                existing_cnpjs = pd.read_sql(existing_query, conn, params={"ref": save_client_ref})
                existing_cnpjs_set = set(existing_cnpjs['cnpj'].astype(str))
                df_to_insert = df_leads_to_save[~df_leads_to_save['cnpj'].astype(str).isin(existing_cnpjs_set)]

                if df_to_insert.empty:
                    st.info("Todos os leads já estavam cadastrados para este cliente.")
                else:
                    colunas_existentes = [
                        'cnpj', 'razao_social', 'nome_fantasia', 'cod_cnae_principal', 'cnae_principal',
                        'cod_cnae_secundario', 'cnae_secundario', 'logradouro', 'numero', 'complemento',
                        'bairro', 'municipio', 'uf', 'cep', 'ddd1', 'telefone1', 'email',
                        'data_inicio_atividade', 'capital_social', 'porte_empresa', 'natureza_juridica',
                        'opcao_simples', 'opcao_mei', 'situacao_cadastral', 'nomes_socios',
                        'qualificacoes', 'faixas_etarias', 'cliente_referencia', 'data_geracao', 'pontuacao'
                    ]
                    df_to_insert = df_to_insert[[col for col in df_to_insert.columns if col in colunas_existentes]]

                    df_to_insert.to_sql('tb_leads_gerados', con=conn, if_exists='append', index=False)
                    st.success(f"{len(df_to_insert)} novos leads salvos com sucesso para '{save_client_ref}'.")
    except Exception as e:
        st.error(f"Erro ao salvar leads: {e}")
