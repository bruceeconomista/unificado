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

# --- Funções de Caching ---
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
        add_cnaes_from_columns('cnae_principal_cod', 'cnae_principal')

    if cnae_type == 'secundario' or cnae_type == 'ambos':
        add_cnaes_from_columns('cnae_secundario_cod', 'cnae_secundario')
    
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

# --- FUNÇÃO PRINCIPAL DE GERAÇÃO DA QUERY SQL ---
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

    if ('cod_cnae_principal' in params and params['cod_cnae_principal']) or \
       ('cod_cnae_secundario' in params and params['cod_cnae_secundario']):
        if "LEFT JOIN tb_cnae tc_principal ON unaccent(upper(vec.cnae_principal)) = unaccent(upper(tc_principal.descricao))" not in joins:
            base_query += ", tc_principal.cod_cnae AS cod_cnae_principal_found"
            joins.append("LEFT JOIN tb_cnae tc_principal ON unaccent(upper(vec.cnae_principal)) = unaccent(upper(tc_principal.descricao))")
        if "LEFT JOIN tb_cnae tc_secundaria ON unaccent(upper(vec.cnae_secundario)) = unaccent(upper(tc_secundaria.descricao))" not in joins:
            base_query += ", tc_secundaria.cod_cnae AS cod_cnae_secundario_found"
            joins.append("LEFT JOIN tb_cnae tc_secundaria ON unaccent(upper(vec.cnae_secundario)) = unaccent(upper(tc_secundaria.descricao))")


    for param, value in params.items():
        if value is None or (isinstance(value, list) and not value) or (isinstance(value, tuple) and not all(value)):
            continue
            
        if param != 'situacao_cadastral': 
            col_name = col_map.get(param)
            param_conditions = []
            
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
                    main_cnae_conditions.append(f"vec.cnae_principal IS NULL") 
                if include_empty_cnae_p:
                    main_cnae_conditions.append(f"vec.cnae_principal = ''") 

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
                    secondary_cnae_like_conditions.append(f"vec.cnae_secundario IS NULL") 
                if include_empty_cnae_s:
                    secondary_cnae_like_conditions.append(f"vec.cnae_secundario = ''") 

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

    if excluded_cnpjs_set:
        param_name_excluded_cnpjs = f"excluded_cnpjs_{param_counter}"
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

df_clientes = st.session_state.df_cnpjs
cnpjs_para_excluir = set(df_clientes['cnpj'].dropna().astype(str).tolist()) if 'cnpj' in df_clientes.columns else set()

# Initialize custom_tags in session_state if not already present
if 'custom_tags_nf' not in st.session_state:
    st.session_state.custom_tags_nf = []
if 'custom_tags_uf' not in st.session_state:
    st.session_state.custom_tags_uf = []
# ... adicione para outros campos que precisarão de custom tags

st.markdown("## ⚙️ Configuração dos Parâmetros de Busca")

st.subheader("Informações do Cliente")
cliente_referencia = st.text_input("Nome ou ID do Cliente para esta Geração de Leads:", key="cliente_referencia_input")
if not cliente_referencia:
    st.warning("Por favor, insira um nome ou ID para o cliente antes de gerar leads.")

ia_params = {}
current_score = 0

# --- Critérios de Identificação de Perfil ---
st.subheader("Critérios de Identificação de Perfil")

# --- Nome Fantasia ---
col1_nf, col2_nf, col3_nf = st.columns([1, 1, 2])
with col1_nf:
    use_nome_fantasia = st.checkbox("Incluir Palavras-Chave (Nome Fantasia)", value=True)
with col2_nf:
    include_null_nf = st.checkbox("Nulo?", key="ia_nf_null") if use_nome_fantasia else False
    include_empty_nf = st.checkbox("Vazio?", key="ia_nf_empty") if use_nome_fantasia else False
with col3_nf:
    if use_nome_fantasia:
        top_n_nf = st.slider("Top N palavras mais frequentes:", min_value=1, max_value=50, value=10, key="ia_top_nf")
        stop_words = set(unidecode(word.lower()) for word in [
            "e", "de", "do", "da", "dos", "das", "o", "a", "os", "as", "um", "uma", "uns", "umas",
            "para", "com", "sem", "em", "no", "na", "nos", "nas", "ao", "aos", "à", "às",
            "por", "pelo", "pela", "pelos", "pelas", "ou", "nem", "mas", "mais", "menos",
            "desde", "até", "após", "entre", "sob", "sobre", "ante", "após", "contra",
            "desde", "durante", "entre", "mediante", "perante", "salvo", "sem", "sob", "sobre", "trás",
            "s.a", "sa", "ltda", "me", "eireli", "epp", "s.a.", "ltda.", "me.", "eireli.", "epp.",
            "sa.", "ltda.", "me.", "eireli.", "epp.", "comercio", "servicos", "serviços", "brasil", "brasileira"              
        ])
        top_nf_words = get_top_n_words(df_clientes, 'nome_fantasia', top_n_nf, stop_words, include_null=include_null_nf, include_empty=include_empty_nf)
        
        # Combinar palavras top N com tags customizadas
        all_nf_options = list(set(top_nf_words + st.session_state.custom_tags_nf))
        # Remover (Nulo) e (Vazio) da lista de opções antes do sort, para que fiquem no final se selecionados
        temp_options = [opt for opt in all_nf_options if opt not in ["(Nulo)", "(Vazio)"]]
        temp_options.sort()
        if "(Nulo)" in all_nf_options: temp_options.append("(Nulo)")
        if "(Vazio)" in all_nf_options: temp_options.append("(Vazio)")

        default_nf_selection = list(set(top_nf_words + [t for t in st.session_state.custom_tags_nf if t in temp_options]))
        if include_null_nf and "(Nulo)" in temp_options: default_nf_selection.append("(Nulo)")
        if include_empty_nf and "(Vazio)" in temp_options: default_nf_selection.append("(Vazio)")

        selected_nf_words = st.multiselect(
            "Palavras-chave selecionadas:",
            options=temp_options,
            default=default_nf_selection,
            key="ia_nf_select"
        )
        ia_params['nome_fantasia'] = selected_nf_words if selected_nf_words else []

        new_nf_tag = st.text_input("Adicionar nova palavra-chave:", key="new_nf_tag_input")
        if new_nf_tag and st.button("Adicionar Tag (Nome Fantasia)", key="add_nf_tag_button"):
            # Adicionar a nova tag se não for vazia e não estiver duplicada
            if new_nf_tag.strip() not in st.session_state.custom_tags_nf:
                st.session_state.custom_tags_nf.append(new_nf_tag.strip())
                st.rerun() # Reruns to update the multiselect options
    else:
        ia_params['nome_fantasia'] = []

st.divider() # ou st.markdown("---")

# --- UF ---
col1_uf, col2_uf, col3_uf = st.columns([1, 1, 2])
with col1_uf:
    use_uf = st.checkbox("Incluir UF", value=True)
with col2_uf:
    include_null_uf = st.checkbox("Nulo?", key="ia_uf_null") if use_uf else False
    include_empty_uf = st.checkbox("Vazio?", key="ia_uf_empty") if use_uf else False
with col3_uf:
    if use_uf:
        top_n_uf = st.slider("Top N UFs mais frequentes:", min_value=1, max_value=27, value=5, key="ia_top_uf")
        top_ufs = get_unique_values(df_clientes, 'uf', top_n_uf, include_null=include_null_uf, include_empty=include_empty_uf)
        
        all_uf_options = list(set(top_ufs + st.session_state.custom_tags_uf))
        temp_options = [opt for opt in all_uf_options if opt not in ["(Nulo)", "(Vazio)"]]
        temp_options.sort()
        if "(Nulo)" in all_uf_options: temp_options.append("(Nulo)")
        if "(Vazio)" in all_uf_options: temp_options.append("(Vazio)")

        default_uf_selection = list(set(top_ufs + [t for t in st.session_state.custom_tags_uf if t in temp_options]))
        if include_null_uf and "(Nulo)" in temp_options: default_uf_selection.append("(Nulo)")
        if include_empty_uf and "(Vazio)" in temp_options: default_uf_selection.append("(Vazio)")

        selected_ufs = st.multiselect(
            "UFs selecionadas:",
            options=temp_options,
            default=default_uf_selection,
            key="ia_uf_select"
        )
        ia_params['uf'] = selected_ufs if selected_ufs else []

        new_uf_tag = st.text_input("Adicionar nova UF:", key="new_uf_tag_input")
        if new_uf_tag and st.button("Adicionar Tag (UF)", key="add_uf_tag_button"):
            if new_uf_tag.strip().upper() not in st.session_state.custom_tags_uf:
                st.session_state.custom_tags_uf.append(new_uf_tag.strip().upper())
                st.rerun()
    else:
        ia_params['uf'] = []

st.divider() # ou st.markdown("---")
    
# --- Município ---
# Initialize custom_tags_municipio
if 'custom_tags_municipio' not in st.session_state:
    st.session_state.custom_tags_municipio = []

col1_mun, col2_mun, col3_mun = st.columns([1, 1, 2])
with col1_mun:
    use_municipio = st.checkbox("Incluir Município", value=True)
with col2_mun:
    include_null_municipio = st.checkbox("Nulo?", key="ia_municipio_null") if use_municipio else False
    include_empty_municipio = st.checkbox("Vazio?", key="ia_municipio_empty") if use_municipio else False
with col3_mun:
    if use_municipio:
        top_n_municipio = st.slider("Top N Municípios mais frequentes:", min_value=1, max_value=50, value=10, key="ia_top_municipio")
        top_municipios = get_unique_values(df_clientes, 'municipio', top_n_municipio, include_null=include_null_municipio, include_empty=include_empty_municipio)

        all_municipio_options = list(set(top_municipios + st.session_state.custom_tags_municipio))
        temp_options = [opt for opt in all_municipio_options if opt not in ["(Nulo)", "(Vazio)"]]
        temp_options.sort()
        if "(Nulo)" in all_municipio_options: temp_options.append("(Nulo)")
        if "(Vazio)" in all_municipio_options: temp_options.append("(Vazio)")

        default_municipio_selection = list(set(top_municipios + [t for t in st.session_state.custom_tags_municipio if t in temp_options]))
        if include_null_municipio and "(Nulo)" in temp_options: default_municipio_selection.append("(Nulo)")
        if include_empty_municipio and "(Vazio)" in temp_options: default_municipio_selection.append("(Vazio)")

        selected_municipios = st.multiselect(
            "Municípios selecionados:",
            options=temp_options,
            default=default_municipio_selection,
            key="ia_municipio_select"
        )
        ia_params['municipio'] = selected_municipios if selected_municipios else []

        new_municipio_tag = st.text_input("Adicionar novo Município:", key="new_municipio_tag_input")
        if new_municipio_tag and st.button("Adicionar Tag (Município)", key="add_municipio_tag_button"):
            if new_municipio_tag.strip() not in st.session_state.custom_tags_municipio:
                st.session_state.custom_tags_municipio.append(new_municipio_tag.strip())
                st.rerun()
    else:
        ia_params['municipio'] = []

st.divider() # ou st.markdown("---")

# --- Bairro ---
# Initialize custom_tags_bairro
if 'custom_tags_bairro' not in st.session_state:
    st.session_state.custom_tags_bairro = []

col1_bairro, col2_bairro, col3_bairro = st.columns([1, 1, 2])
with col1_bairro:
    use_bairro = st.checkbox("Incluir Bairro", value=False)
with col2_bairro:
    include_null_bairro = st.checkbox("Nulo?", key="ia_bairro_null") if use_bairro else False
    include_empty_bairro = st.checkbox("Vazio?", key="ia_bairro_empty") if use_bairro else False
with col3_bairro:
    if use_bairro:
        top_n_bairro = st.slider("Top N Bairros mais frequentes:", min_value=1, max_value=50, value=10, key="ia_top_bairro")
        df_temp_bairro = df_clientes.copy()
        def normalizar_bairro_ia(bairro):
            if pd.isna(bairro): return None
            return unidecode(str(bairro).upper().split('/')[0].strip())
        df_temp_bairro['bairro_normalizado_ia'] = df_temp_bairro['bairro'].apply(normalizar_bairro_ia)
        top_bairros = get_unique_values(df_temp_bairro, 'bairro_normalizado_ia', top_n_bairro, include_null=include_null_bairro, include_empty=include_empty_bairro)
        
        all_bairro_options = list(set(top_bairros + st.session_state.custom_tags_bairro))
        temp_options = [opt for opt in all_bairro_options if opt not in ["(Nulo)", "(Vazio)"]]
        temp_options.sort()
        if "(Nulo)" in all_bairro_options: temp_options.append("(Nulo)")
        if "(Vazio)" in all_bairro_options: temp_options.append("(Vazio)")

        default_bairro_selection = list(set(top_bairros + [t for t in st.session_state.custom_tags_bairro if t in temp_options]))
        if include_null_bairro and "(Nulo)" in temp_options: default_bairro_selection.append("(Nulo)")
        if include_empty_bairro and "(Vazio)" in temp_options: default_bairro_selection.append("(Vazio)")

        selected_bairros = st.multiselect(
            "Bairros selecionados:",
            options=temp_options,
            default=default_bairro_selection,
            key="ia_bairro_select"
        )
        ia_params['bairro'] = selected_bairros if selected_bairros else []

        new_bairro_tag = st.text_input("Adicionar novo Bairro:", key="new_bairro_tag_input")
        if new_bairro_tag and st.button("Adicionar Tag (Bairro)", key="add_bairro_tag_button"):
            if new_bairro_tag.strip() not in st.session_state.custom_tags_bairro:
                st.session_state.custom_tags_bairro.append(new_bairro_tag.strip())
                st.rerun()
    else:
        ia_params['bairro'] = []

st.divider() # ou st.markdown("---")

# --- CNAE Principal ---
# Initialize custom_tags_cnae_principal
if 'custom_tags_cnae_principal' not in st.session_state:
    st.session_state.custom_tags_cnae_principal = [] # Stores tuples (code, description)

col1_cnae_p, col2_cnae_p, col3_cnae_p = st.columns([1, 1, 2])
with col1_cnae_p:
    use_cnae_principal = st.checkbox("Incluir CNAE Principal", value=True)
with col2_cnae_p:
    include_null_cnae_p = st.checkbox("Nulo?", key="ia_cnae_p_null") if use_cnae_principal else False
    include_empty_cnae_p = st.checkbox("Vazio?", key="ia_cnae_p_empty") if use_cnae_principal else False
with col3_cnae_p:
    if use_cnae_principal:
        top_n_cnae_principal = st.slider("Top N CNAEs Principais mais frequentes:", min_value=1, max_value=50, value=10, key="ia_top_cnae_principal")
        top_cnaes_principal_pairs = get_top_n_cnaes(df_clientes, 'principal', top_n_cnae_principal, include_null=include_null_cnae_p, include_empty=include_empty_cnae_p)
        
        all_cnae_p_options_tuple = list(set(top_cnaes_principal_pairs + st.session_state.custom_tags_cnae_principal))
        all_cnae_p_options_display = []
        default_cnae_p_selection_display = []

        # Separate special tags for sorting
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
        for code, desc in st.session_state.custom_tags_cnae_principal:
            if f"{code} - {desc}" in all_cnae_p_options_display:
                default_cnae_p_selection_display.append(f"{code} - {desc}")
        
        if include_null_cnae_p and "(Nulo) - (Nulo)" in all_cnae_p_options_display:
            default_cnae_p_selection_display.append("(Nulo) - (Nulo)")
        if include_empty_cnae_p and "(Vazio) - (Vazio)" in all_cnae_p_options_display:
            default_cnae_p_selection_display.append("(Vazio) - (Vazio)")

        selected_options_cnae_p = st.multiselect(
            "CNAEs Principais selecionados:",
            options=all_cnae_p_options_display,
            default=list(set(default_cnae_p_selection_display)), # Use set to remove duplicates
            key="ia_cnae_principal_select"
        )

        ia_params['cod_cnae_principal'] = []
        for opt in selected_options_cnae_p:
            if opt == "(Nulo) - (Nulo)":
                ia_params['cod_cnae_principal'].append(("(Nulo)", "(Nulo)"))
            elif opt == "(Vazio) - (Vazio)":
                ia_params['cod_cnae_principal'].append(("(Vazio)", "(Vazio)"))
            else:
                code_desc_pair = opt.split(' - ', 1)
                if len(code_desc_pair) == 2:
                    ia_params['cod_cnae_principal'].append((code_desc_pair[0], code_desc_pair[1]))
                else:
                    ia_params['cod_cnae_principal'].append((opt, opt)) # Fallback if format is unexpected
        
        new_cnae_p_input = st.text_input("Adicionar novo CNAE Principal (código ou descrição):", key="new_cnae_p_input")
        if new_cnae_p_input and st.button("Adicionar Tag (CNAE Principal)", key="add_cnae_p_tag_button"):
            # Basic attempt to parse. For full lookup, might need DB query
            new_cnae_p_code = new_cnae_p_input.strip()
            new_cnae_p_desc = new_cnae_p_input.strip() # Assuming description if not code
            
            # Simple check if it looks like a code (e.g., 4 digits, dot, 2 digits)
            if re.match(r'^\d{4}-\d{1}$', new_cnae_p_code) or re.match(r'^\d{4}-\d{2}$', new_cnae_p_code):
                pass # Already looks like a code
            elif re.match(r'^\d{4}\d{2}$', new_cnae_p_code): # If 6 digits, format it
                 new_cnae_p_code = f"{new_cnae_p_code[:4]}-{new_cnae_p_code[4:]}"
            
            new_cnae_pair = (new_cnae_p_code, new_cnae_p_desc)
            if new_cnae_pair not in st.session_state.custom_tags_cnae_principal:
                st.session_state.custom_tags_cnae_principal.append(new_cnae_pair)
                st.rerun()
    else:
        ia_params['cod_cnae_principal'] = []

st.divider() # ou st.markdown("---")

# --- CNAE Secundário ---
# Initialize custom_tags_cnae_secundario
if 'custom_tags_cnae_secundario' not in st.session_state:
    st.session_state.custom_tags_cnae_secundario = [] # Stores tuples (code, description)

col1_cnae_s, col2_cnae_s, col3_cnae_s = st.columns([1, 1, 2])
with col1_cnae_s:
    use_cnae_secundario = st.checkbox("Incluir CNAE Secundário", value=False)
with col2_cnae_s:
    include_null_cnae_s = st.checkbox("Nulo?", key="ia_cnae_s_null") if use_cnae_secundario else False
    include_empty_cnae_s = st.checkbox("Vazio?", key="ia_cnae_s_empty") if use_cnae_secundario else False
with col3_cnae_s:
    if use_cnae_secundario:
        top_n_cnae_secundario = st.slider("Top N CNAEs Secundários mais frequentes:", min_value=1, max_value=50, value=10, key="ia_top_cnae_secundario")
        top_cnaes_secundario_pairs = get_top_n_cnaes(df_clientes, 'secundario', top_n_cnae_secundario, include_null=include_null_cnae_s, include_empty=include_empty_cnae_s)
        
        all_cnae_s_options_tuple = list(set(top_cnaes_secundario_pairs + st.session_state.custom_tags_cnae_secundario))
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
        for code, desc in st.session_state.custom_tags_cnae_secundario:
            if f"{code} - {desc}" in all_cnae_s_options_display:
                default_cnae_s_selection_display.append(f"{code} - {desc}")

        selected_options_cnae_s = st.multiselect(
            "CNAEs Secundários selecionados:",
            options=all_cnae_s_options_display,
            default=list(set(default_cnae_s_selection_display)),
            key="ia_cnae_secundario_select"
        )

        ia_params['cod_cnae_secundario'] = []
        for opt in selected_options_cnae_s:
            if opt == "(Nulo) - (Nulo)":
                ia_params['cod_cnae_secundario'].append(("(Nulo)", "(Nulo)"))
            elif opt == "(Vazio) - (Vazio)":
                ia_params['cod_cnae_secundario'].append(("(Vazio)", "(Vazio)"))
            else:
                code_desc_pair = opt.split(' - ', 1)
                if len(code_desc_pair) == 2:
                    ia_params['cod_cnae_secundario'].append((code_desc_pair[0], code_desc_pair[1]))
                else:
                    ia_params['cod_cnae_secundario'].append((opt, opt))
        
        new_cnae_s_input = st.text_input("Adicionar novo CNAE Secundário (código ou descrição):", key="new_cnae_s_input")
        if new_cnae_s_input and st.button("Adicionar Tag (CNAE Secundário)", key="add_cnae_s_tag_button"):
            new_cnae_s_code = new_cnae_s_input.strip()
            new_cnae_s_desc = new_cnae_s_input.strip()
            if re.match(r'^\d{4}-\d{1}$', new_cnae_s_code) or re.match(r'^\d{4}-\d{2}$', new_cnae_s_code):
                pass
            elif re.match(r'^\d{4}\d{2}$', new_cnae_s_code):
                 new_cnae_s_code = f"{new_cnae_s_code[:4]}-{new_cnae_s_code[4:]}"
            
            new_cnae_pair = (new_cnae_s_code, new_cnae_s_desc)
            if new_cnae_pair not in st.session_state.custom_tags_cnae_secundario:
                st.session_state.custom_tags_cnae_secundario.append(new_cnae_pair)
                st.rerun()
    else:
        ia_params['cod_cnae_secundario'] = []

st.divider() # ou st.markdown("---")

# --- Data de Início de Atividade ---
col1_data, col2_data, col3_data = st.columns([1, 1, 2])
with col1_data:
    use_data_inicio = st.checkbox("Incluir Período de Início de Atividade", value=True)
with col2_data:
    # No "data de início", não há opção de Nulo/Vazio para checkbox separado, 
    # pois o range já naturalmente trata isso se não houver dados.
    # Se precisar de um "Incluir Nulo/Vazio" específico para a data, precisaria de uma lógica de query separada.
    st.write("") # Placeholder for alignment
with col3_data:
    if use_data_inicio:
        min_date_client = df_clientes['data_inicio_atividade'].min() if 'data_inicio_atividade' in df_clientes.columns and not df_clientes['data_inicio_atividade'].empty else datetime(1900, 1, 1).date()
        max_date_client = df_clientes['data_inicio_atividade'].max() if 'data_inicio_atividade' in df_clientes.columns and not df_clientes['data_inicio_atividade'].empty else datetime.now().date()

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
            key="ia_start_date_input"
        )
        end_date = st.date_input(
            "Data de Início (Até):",
            value=max_calendar_date,
            min_value=min_calendar_date,
            max_value=max_calendar_date,
            key="ia_end_date_input"
        )

        if start_date > end_date:
            st.error("A 'Data de Início (De)' não pode ser posterior à 'Data de Início (Até)'. Por favor, ajuste o período.")
            ia_params['data_inicio_atividade'] = None
        else:
            ia_params['data_inicio_atividade'] = (start_date, end_date)
    else:
        ia_params['data_inicio_atividade'] = None

st.divider() # ou st.markdown("---")

# --- Capital Social ---
col1_capital, col2_capital, col3_capital = st.columns([1, 1, 2])
with col1_capital:
    use_capital_social = st.checkbox("Incluir Faixa de Capital Social", value=True)
with col2_capital:
    st.write("") # Placeholder for alignment
with col3_capital:
    if use_capital_social:
        min_capital_client = df_clientes['capital_social'].min() if 'capital_social' in df_clientes.columns and not df_clientes['capital_social'].empty else 0.0
        max_capital_client = df_clientes['capital_social'].max() if 'capital_social' in df_clientes.columns and not df_clientes['capital_social'].empty else 10000000.0
        
        if min_capital_client == max_capital_client and min_capital_client > 0:
            min_capital_client = max(0.0, min_capital_client * 0.9)
            max_capital_client = max_capital_client * 1.1

        min_val = st.number_input(
            "Capital Social (Mínimo):",
            min_value=0.0,
            value=float(min_capital_client),
            step=1000.0,
            format="%.2f",
            key="ia_min_capital_input"
        )
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
            ia_params['capital_social'] = None
        else:
            ia_params['capital_social'] = (min_val, max_val)
        st.info(f"Faixa Selecionada: R$ {min_val:,.2f} a R$ {max_val:,.2f}")
    else:
        ia_params['capital_social'] = None

st.divider() # ou st.markdown("---")

# --- Porte da Empresa ---
# Initialize custom_tags_porte_empresa
if 'custom_tags_porte_empresa' not in st.session_state:
    st.session_state.custom_tags_porte_empresa = []

col1_porte, col2_porte, col3_porte = st.columns([1, 1, 2])
with col1_porte:
    use_porte_empresa = st.checkbox("Incluir Porte da Empresa", value=True)
with col2_porte:
    include_null_porte = st.checkbox("Nulo?", key="ia_porte_null") if use_porte_empresa else False
    include_empty_porte = st.checkbox("Vazio?", key="ia_porte_empty") if use_porte_empresa else False
with col3_porte:
    if use_porte_empresa:
        # Original options
        base_options_porte = ["MICRO EMPRESA", "EMPRESA DE PEQUENO PORTE", "DEMAIS"]
        # Filter options based on existing client data to use as default, or add if not present
        unique_portes_from_df = df_clientes['porte_empresa'].dropna().unique().tolist()
        
        all_porte_options = list(set(base_options_porte + unique_portes_from_df + st.session_state.custom_tags_porte_empresa))
        temp_options = [opt for opt in all_porte_options if opt not in ["(Nulo)", "(Vazio)"]]
        temp_options.sort()
        if "(Nulo)" in all_porte_options: temp_options.append("(Nulo)")
        if "(Vazio)" in all_porte_options: temp_options.append("(Vazio)")

        default_selected_portes = list(set([p for p in base_options_porte if p in temp_options] + unique_portes_from_df + [t for t in st.session_state.custom_tags_porte_empresa if t in temp_options]))
        if include_null_porte and "(Nulo)" in temp_options: default_selected_portes.append("(Nulo)")
        if include_empty_porte and "(Vazio)" in temp_options: default_selected_portes.append("(Vazio)")
        
        selected_portes = st.multiselect(
            "Selecione o(s) Porte(s) da Empresa:",
            options=temp_options,
            default=list(set(default_selected_portes)),
            key="ia_porte_empresa_select"
        )
        ia_params['porte_empresa'] = selected_portes if selected_portes else []

        new_porte_tag = st.text_input("Adicionar novo Porte da Empresa:", key="new_porte_tag_input")
        if new_porte_tag and st.button("Adicionar Tag (Porte)", key="add_porte_tag_button"):
            if new_porte_tag.strip() not in st.session_state.custom_tags_porte_empresa:
                st.session_state.custom_tags_porte_empresa.append(new_porte_tag.strip())
                st.rerun()
    else:
        ia_params['porte_empresa'] = []

st.divider() # ou st.markdown("---")

# --- Natureza Jurídica ---
# Initialize custom_tags_natureza_juridica
if 'custom_tags_natureza_juridica' not in st.session_state:
    st.session_state.custom_tags_natureza_juridica = []

col1_nj, col2_nj, col3_nj = st.columns([1, 1, 2])
with col1_nj:
    use_natureza_juridica = st.checkbox("Incluir Natureza Jurídica", value=False)
with col2_nj:
    include_null_nj = st.checkbox("Nulo?", key="ia_nj_null") if use_natureza_juridica else False
    include_empty_nj = st.checkbox("Vazio?", key="ia_nj_empty") if use_natureza_juridica else False
with col3_nj:
    if use_natureza_juridica:
        top_n_nj = st.slider("Top N Naturezas Jurídicas mais frequentes:", min_value=1, max_value=50, value=10, key="ia_top_nj")
        top_njs = get_unique_values(df_clientes, 'natureza_juridica', top_n_nj, include_null=include_null_nj, include_empty=include_empty_nj)
        
        all_nj_options = list(set(top_njs + st.session_state.custom_tags_natureza_juridica))
        temp_options = [opt for opt in all_nj_options if opt not in ["(Nulo)", "(Vazio)"]]
        temp_options.sort()
        if "(Nulo)" in all_nj_options: temp_options.append("(Nulo)")
        if "(Vazio)" in all_nj_options: temp_options.append("(Vazio)")

        default_nj_selection = list(set(top_njs + [t for t in st.session_state.custom_tags_natureza_juridica if t in temp_options]))
        if include_null_nj and "(Nulo)" in temp_options: default_nj_selection.append("(Nulo)")
        if include_empty_nj and "(Vazio)" in temp_options: default_nj_selection.append("(Vazio)")

        selected_njs = st.multiselect(
            "Naturezas Jurídicas selecionadas:",
            options=temp_options,
            default=list(set(default_nj_selection)),
            key="ia_nj_select"
        )
        ia_params['natureza_juridica'] = selected_njs if selected_njs else []

        new_nj_tag = st.text_input("Adicionar nova Natureza Jurídica:", key="new_nj_tag_input")
        if new_nj_tag and st.button("Adicionar Tag (Natureza Jurídica)", key="add_nj_tag_button"):
            if new_nj_tag.strip() not in st.session_state.custom_tags_natureza_juridica:
                st.session_state.custom_tags_natureza_juridica.append(new_nj_tag.strip())
                st.rerun()
    else:
        ia_params['natureza_juridica'] = []

st.divider() # ou st.markdown("---")

# --- Opção Simples Nacional ---
# Initialize custom_tags_opcao_simples
if 'custom_tags_opcao_simples' not in st.session_state:
    st.session_state.custom_tags_opcao_simples = []

col1_simples, col2_simples, col3_simples = st.columns([1, 1, 2])
with col1_simples:
    use_opcao_simples = st.checkbox("Incluir Opção Simples Nacional", value=False)
with col2_simples:
    include_null_simples = st.checkbox("Nulo?", key="ia_simples_null") if use_opcao_simples else False
    include_empty_simples = st.checkbox("Vazio?", key="ia_simples_empty") if use_opcao_simples else False
with col3_simples:
    if use_opcao_simples:
        base_simples_options = ['S', 'N']
        unique_simples_from_df = df_clientes['opcao_simples'].dropna().unique().tolist()

        all_simples_options = list(set(base_simples_options + unique_simples_from_df + st.session_state.custom_tags_opcao_simples))
        temp_options = [opt for opt in all_simples_options if opt not in ["(Nulo)", "(Vazio)"]]
        temp_options.sort()
        if "(Nulo)" in all_simples_options: temp_options.append("(Nulo)")
        if "(Vazio)" in all_simples_options: temp_options.append("(Vazio)")

        default_selected_simples = list(set([s for s in base_simples_options if s in temp_options] + unique_simples_from_df + [t for t in st.session_state.custom_tags_opcao_simples if t in temp_options]))
        if include_null_simples and "(Nulo)" in temp_options: default_selected_simples.append("(Nulo)")
        if include_empty_simples and "(Vazio)" in temp_options: default_selected_simples.append("(Vazio)")

        selected_opcao_simples = st.multiselect(
            "Optante pelo Simples Nacional?",
            options=temp_options,
            default=list(set(default_selected_simples)),
            key="ia_simples_select"
        )
        ia_params['opcao_simples'] = selected_opcao_simples if selected_opcao_simples else []

        new_simples_tag = st.text_input("Adicionar nova Opção Simples Nacional:", key="new_simples_tag_input")
        if new_simples_tag and st.button("Adicionar Tag (Simples)", key="add_simples_tag_button"):
            if new_simples_tag.strip().upper() not in st.session_state.custom_tags_opcao_simples:
                st.session_state.custom_tags_opcao_simples.append(new_simples_tag.strip().upper())
                st.rerun()
    else:
        ia_params['opcao_simples'] = []

st.divider() # ou st.markdown("---")

# --- Opção MEI ---
# Initialize custom_tags_opcao_mei
if 'custom_tags_opcao_mei' not in st.session_state:
    st.session_state.custom_tags_opcao_mei = []

col1_mei, col2_mei, col3_mei = st.columns([1, 1, 2])
with col1_mei:
    use_opcao_mei = st.checkbox("Incluir Opção MEI", value=False)
with col2_mei:
    include_null_mei = st.checkbox("Nulo?", key="ia_mei_null") if use_opcao_mei else False
    include_empty_mei = st.checkbox("Vazio?", key="ia_mei_empty") if use_opcao_mei else False
with col3_mei:
    if use_opcao_mei:
        base_mei_options = ['S', 'N']
        unique_mei_from_df = df_clientes['opcao_mei'].dropna().unique().tolist()

        all_mei_options = list(set(base_mei_options + unique_mei_from_df + st.session_state.custom_tags_opcao_mei))
        temp_options = [opt for opt in all_mei_options if opt not in ["(Nulo)", "(Vazio)"]]
        temp_options.sort()
        if "(Nulo)" in all_mei_options: temp_options.append("(Nulo)")
        if "(Vazio)" in all_mei_options: temp_options.append("(Vazio)")

        default_selected_mei = list(set([m for m in base_mei_options if m in temp_options] + unique_mei_from_df + [t for t in st.session_state.custom_tags_opcao_mei if t in temp_options]))
        if include_null_mei and "(Nulo)" in temp_options: default_selected_mei.append("(Nulo)")
        if include_empty_mei and "(Vazio)" in temp_options: default_selected_mei.append("(Vazio)")

        selected_opcao_mei = st.multiselect(
            "Optante pelo MEI?",
            options=temp_options,
            default=list(set(default_selected_mei)),
            key="ia_mei_select"
        )
        ia_params['opcao_mei'] = selected_opcao_mei if selected_opcao_mei else []

        new_mei_tag = st.text_input("Adicionar nova Opção MEI:", key="new_mei_tag_input")
        if new_mei_tag and st.button("Adicionar Tag (MEI)", key="add_mei_tag_button"):
            if new_mei_tag.strip().upper() not in st.session_state.custom_tags_opcao_mei:
                st.session_state.custom_tags_opcao_mei.append(new_mei_tag.strip().upper())
                st.rerun()
    else:
        ia_params['opcao_mei'] = []

st.divider() # ou st.markdown("---")

# --- Critérios de Contato e Sócios ---
st.subheader("Critérios de Contato e Sócios")

# --- DDD de Contato ---
# Initialize custom_tags_ddd1
if 'custom_tags_ddd1' not in st.session_state:
    st.session_state.custom_tags_ddd1 = []

col1_ddd, col2_ddd, col3_ddd = st.columns([1, 1, 2])
with col1_ddd:
    use_ddd1 = st.checkbox("Incluir DDD de Contato", value=False)
with col2_ddd:
    include_null_ddd1 = st.checkbox("Nulo?", key="ia_ddd1_null") if use_ddd1 else False
    include_empty_ddd1 = st.checkbox("Vazio?", key="ia_ddd1_empty") if use_ddd1 else False
with col3_ddd:
    if use_ddd1:
        top_n_ddd = st.slider("Top N DDDs mais frequentes:", min_value=1, max_value=50, value=10, key="ia_top_ddd")
        unique_ddds = get_unique_values(df_clientes, 'ddd1', top_n_ddd, include_null=include_null_ddd1, include_empty=include_empty_ddd1)
        
        all_ddd_options = list(set(unique_ddds + st.session_state.custom_tags_ddd1))
        temp_options = [opt for opt in all_ddd_options if opt not in ["(Nulo)", "(Vazio)"]]
        temp_options.sort()
        if "(Nulo)" in all_ddd_options: temp_options.append("(Nulo)")
        if "(Vazio)" in all_ddd_options: temp_options.append("(Vazio)")

        default_selected_ddds = list(set(unique_ddds + [t for t in st.session_state.custom_tags_ddd1 if t in temp_options]))
        if include_null_ddd1 and "(Nulo)" in temp_options: default_selected_ddds.append("(Nulo)")
        if include_empty_ddd1 and "(Vazio)" in temp_options: default_selected_ddds.append("(Vazio)")

        selected_ddds = st.multiselect(
            "DDDs de interesse:",
            options=temp_options,
            default=list(set(default_selected_ddds)),
            key="ia_ddd1_select"
        )
        ia_params['ddd1'] = selected_ddds if selected_ddds else []

        new_ddd_tag = st.text_input("Adicionar novo DDD:", key="new_ddd_tag_input")
        if new_ddd_tag and st.button("Adicionar Tag (DDD)", key="add_ddd_tag_button"):
            if new_ddd_tag.strip() not in st.session_state.custom_tags_ddd1:
                st.session_state.custom_tags_ddd1.append(new_ddd_tag.strip())
                st.rerun()
    else:
        ia_params['ddd1'] = []

st.divider() # ou st.markdown("---")

# --- Nome Sócio / Razão Social ---
# Initialize custom_tags_nome_socio
if 'custom_tags_nome_socio' not in st.session_state:
    st.session_state.custom_tags_nome_socio = []

col1_socio, col2_socio, col3_socio = st.columns([1, 1, 2])
with col1_socio:
    use_nome_socio_razao = st.checkbox("Incluir Nome Sócio / Razão Social (similaridade)", value=False)
with col2_socio:
    include_null_socio = st.checkbox("Nulo?", key="ia_socio_null") if use_nome_socio_razao else False
    include_empty_socio = st.checkbox("Vazio?", key="ia_socio_empty") if use_nome_socio_razao else False
with col3_socio:
    if use_nome_socio_razao:
        unique_socios = df_clientes['nome_socio'].dropna().astype(str).unique().tolist() if 'nome_socio' in df_clientes.columns else []
        # Combine unique socios with custom ones
        all_socio_options = list(set(unique_socios + st.session_state.custom_tags_nome_socio))
        temp_options = [opt for opt in all_socio_options if opt not in ["(Nulo)", "(Vazio)"]]
        temp_options.sort()
        if "(Nulo)" in all_socio_options: temp_options.append("(Nulo)")
        if "(Vazio)" in all_socio_options: temp_options.append("(Vazio)")

        default_socio_selection = list(set(unique_socios + [t for t in st.session_state.custom_tags_nome_socio if t in temp_options]))
        if include_null_socio and "(Nulo)" in temp_options: default_socio_selection.append("(Nulo)")
        if include_empty_socio and "(Vazio)" in temp_options: default_socio_selection.append("(Vazio)")

        selected_socio_names = st.multiselect(
            "Nomes/Partes de nomes selecionados:",
            options=temp_options,
            default=list(set(default_socio_selection)),
            key="ia_nome_socio_select"
        )
        ia_params['nome_socio_razao_social'] = selected_socio_names if selected_socio_names else []

        new_socio_tag = st.text_input("Adicionar novo Nome Sócio / Razão Social:", key="new_socio_tag_input")
        if new_socio_tag and st.button("Adicionar Tag (Sócio/Razão Social)", key="add_socio_tag_button"):
            if new_socio_tag.strip() not in st.session_state.custom_tags_nome_socio:
                st.session_state.custom_tags_nome_socio.append(new_socio_tag.strip())
                st.rerun()
    else:
        ia_params['nome_socio_razao_social'] = []

st.divider() # ou st.markdown("---")

# --- Qualificação do Sócio ---
# Initialize custom_tags_qualificacao_socio
if 'custom_tags_qualificacao_socio' not in st.session_state:
    st.session_state.custom_tags_qualificacao_socio = []

col1_qs, col2_qs, col3_qs = st.columns([1, 1, 2])
with col1_qs:
    use_qualificacao_socio = st.checkbox("Incluir Qualificação do Sócio", value=False)
with col2_qs:
    include_null_qs = st.checkbox("Nulo?", key="ia_qs_null") if use_qualificacao_socio else False
    include_empty_qs = st.checkbox("Vazio?", key="ia_qs_empty") if use_qualificacao_socio else False
with col3_qs:
    if use_qualificacao_socio:
        top_n_qs = st.slider("Top N Qualificações de Sócio mais frequentes:", min_value=1, max_value=50, value=10, key="ia_top_qs")
        top_qss = get_unique_values(df_clientes, 'qualificacao_socio', top_n_qs, include_null=include_null_qs, include_empty=include_empty_qs)
        
        all_qs_options = list(set(top_qss + st.session_state.custom_tags_qualificacao_socio))
        temp_options = [opt for opt in all_qs_options if opt not in ["(Nulo)", "(Vazio)"]]
        temp_options.sort()
        if "(Nulo)" in all_qs_options: temp_options.append("(Nulo)")
        if "(Vazio)" in all_qs_options: temp_options.append("(Vazio)")

        default_qs_selection = list(set(top_qss + [t for t in st.session_state.custom_tags_qualificacao_socio if t in temp_options]))
        if include_null_qs and "(Nulo)" in temp_options: default_qs_selection.append("(Nulo)")
        if include_empty_qs and "(Vazio)" in temp_options: default_qs_selection.append("(Vazio)")

        selected_qss = st.multiselect(
            "Qualificações de sócio selecionadas:",
            options=temp_options,
            default=list(set(default_qs_selection)),
            key="ia_qs_select"
        )
        ia_params['qualificacao_socio'] = selected_qss if selected_qss else []

        new_qs_tag = st.text_input("Adicionar nova Qualificação do Sócio:", key="new_qs_tag_input")
        if new_qs_tag and st.button("Adicionar Tag (Qualificação Sócio)", key="add_qs_tag_button"):
            if new_qs_tag.strip() not in st.session_state.custom_tags_qualificacao_socio:
                st.session_state.custom_tags_qualificacao_socio.append(new_qs_tag.strip())
                st.rerun()
    else:
        ia_params['qualificacao_socio'] = []

st.divider() # ou st.markdown("---")

# --- Faixa Etária do Sócio ---
# Initialize custom_tags_faixa_etaria_socio
if 'custom_tags_faixa_etaria_socio' not in st.session_state:
    st.session_state.custom_tags_faixa_etaria_socio = []

col1_fes, col2_fes, col3_fes = st.columns([1, 1, 2])
with col1_fes:
    use_faixa_etaria_socio = st.checkbox("Incluir Faixa Etária do Sócio", value=False)
with col2_fes:
    include_null_fes = st.checkbox("Nulo?", key="ia_fes_null") if use_faixa_etaria_socio else False
    include_empty_fes = st.checkbox("Vazio?", key="ia_fes_empty") if use_faixa_etaria_socio else False
with col3_fes:
    if use_faixa_etaria_socio:
        top_n_fes = st.slider("Top N Faixas Etárias de Sócio mais frequentes:", min_value=1, max_value=10, value=5, key="ia_top_fes")
        top_fess = get_unique_values(df_clientes, 'faixa_etaria_socio', top_n_fes, include_null=include_null_fes, include_empty=include_empty_fes)
        
        all_fes_options = list(set(top_fess + st.session_state.custom_tags_faixa_etaria_socio))
        temp_options = [opt for opt in all_fes_options if opt not in ["(Nulo)", "(Vazio)"]]
        temp_options.sort()
        if "(Nulo)" in all_fes_options: temp_options.append("(Nulo)")
        if "(Vazio)" in all_fes_options: temp_options.append("(Vazio)")

        default_fes_selection = list(set(top_fess + [t for t in st.session_state.custom_tags_faixa_etaria_socio if t in temp_options]))
        if include_null_fes and "(Nulo)" in temp_options: default_fes_selection.append("(Nulo)")
        if include_empty_fes and "(Vazio)" in temp_options: default_fes_selection.append("(Vazio)")

        selected_fess = st.multiselect(
            "Faixas etárias de sócio selecionadas:",
            options=temp_options,
            default=list(set(default_fes_selection)),
            key="ia_fes_select"
        )
        ia_params['faixa_etaria_socio'] = selected_fess if selected_fess else []

        new_fes_tag = st.text_input("Adicionar nova Faixa Etária do Sócio:", key="new_fes_tag_input")
        if new_fes_tag and st.button("Adicionar Tag (Faixa Etária Sócio)", key="add_fes_tag_button"):
            if new_fes_tag.strip() not in st.session_state.custom_tags_faixa_etaria_socio:
                st.session_state.custom_tags_faixa_etaria_socio.append(new_fes_tag.strip())
                st.rerun()
    else:
        ia_params['faixa_etaria_socio'] = []

ia_params['situacao_cadastral'] = 'ATIVA'

st.markdown("---")
if st.button("🚀 Gerar Leads com IA"):
    if not cliente_referencia:
        st.error("Por favor, preencha o campo 'Nome ou ID do Cliente' antes de gerar leads.")
        st.stop()
    
    if not cnpjs_para_excluir:
        st.warning("Não há CNPJs carregados na base do cliente para exclusão. A busca pode incluir clientes existentes.")

    with st.spinner("Gerando leads..."):
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
                if 'cod_cnae_principal_found' in df_leads_gerados.columns:
                    df_leads_gerados = df_leads_gerados.drop(columns=['cod_cnae_principal_found'])
                if 'cod_cnae_secundario_found' in df_leads_gerados.columns:
                    df_leads_gerados = df_leads_gerados.drop(columns=['cod_cnae_secundario_found'])
            
            df_leads_gerados['pontuacao'] = score
            df_leads_gerados['data_geracao'] = datetime.now()
            df_leads_gerados['cliente_referencia'] = cliente_referencia

            for col in df_leads_gerados.columns:
                if df_leads_gerados[col].dtype == 'object':
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
    used_criteria = {k: v for k, v in ia_params.items() if v is not None and not (isinstance(v, list) and not v) and not (isinstance(v, tuple) and not all(v))}
    
    if used_criteria:
        for param, value in used_criteria.items():
            if param == 'data_inicio_atividade':
                st.write(f"- **Data Início Atividade:** De {value[0].strftime('%d/%m/%Y')} a {value[1].strftime('%d/%m/%Y')}")
            elif param == 'capital_social':
                st.write(f"- **Capital Social:** Entre R$ {value[0]:,.2f} e R$ {value[1]:,.2f}")
            elif param in ['nome_fantasia', 'uf', 'municipio', 'bairro', 'natureza_juridica',
                            'qualificacao_socio', 'faixa_etaria_socio', 'ddd1',
                            'porte_empresa', 'opcao_simples', 'opcao_mei', 'nome_socio_razao_social']:
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
                                     df_leads_gerados_filtered[col] = df_leads_gerados_filtered[col].fillna('').replace('None', '')
                                    
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
