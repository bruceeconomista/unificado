import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from io import BytesIO
import re
from collections import Counter
from unidecode import unidecode
from datetime import datetime, timedelta

# Configuração da página Streamlit
st.set_page_config(layout="wide", page_title="IA de Geração de Leads")
st.title("🤖 IA Generator: Encontre Novos Leads")

def normalize_text(value):
    if isinstance(value, str):
        return unidecode(value.strip().upper())
    return value

# Conexão com o banco de dados
DATABASE_URL = 
engine = create_engine(DATABASE_URL)

# --- Estados da Sessão ---
if 'current_score' not in st.session_state:
    st.session_state.current_score = 0
if 'current_sql_query' not in st.session_state:
    st.session_state.current_sql_query = None
if 'origem_dados' not in st.session_state:
    st.session_state.origem_dados = "upload"  # Default para upload
    st.session_state.df_cnpjs = None
if 'df_cnpjs' not in st.session_state:
    st.session_state.df_cnpjs = None
if 'df_leads_gerados' not in st.session_state:
    st.session_state.df_leads_gerados = pd.DataFrame()

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
        'qualificacao_socio': 5,
    'faixa_etaria_socio': 5
}

# --- Expected columns conforme as MVs ---
# (Atualize essas listas conforme os arquivos CSV enviados)
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

    temp_series = df[column][df[column].notna() & (df[column].astype(str).str.strip() != "")]
    all_words = temp_series.apply(clean_and_tokenize).explode().dropna().tolist()

    word_counts = Counter(all_words)
    common_words = [word for word, count in word_counts.most_common(top_n)]

    if include_null and df[column].isna().any() and "(Nulo)" not in common_words:
        common_words.append("(Nulo)")
    if include_empty and (df[column].astype(str).str.strip() == "").any() and "(Vazio)" not in common_words:
        common_words.append("(Vazio)")
    return common_words

@st.cache_data
def get_top_n_cnaes(df, cnae_type, top_n, include_null=False, include_empty=False):
    all_cnaes_info = []
    
    def add_cnaes_from_columns(codes_col, descriptions_col):
        if codes_col in df.columns and descriptions_col in df.columns:
            valid_rows = df[df[codes_col].notna() & (df[codes_col].astype(str).str.strip() != "")]
            for _, row in valid_rows.iterrows():
                codes_str = str(row[codes_col]).strip()
                descriptions_str = str(row[descriptions_col]).strip()
                codes = [c.strip() for c in codes_str.split('; ') if c.strip()]
                descriptions = [d.strip() for d in descriptions_str.split('; ') if d.strip()]
                for i in range(min(len(codes), len(descriptions))):
                    code = codes[i]
                    description = descriptions[i]
                    if code and description:
                        all_cnaes_info.append((code, description))
    
    if cnae_type in ['principal', 'ambos']:
        add_cnaes_from_columns('cod_cnae_principal', 'cnae_principal')
    if cnae_type in ['secundario', 'ambos']:
        add_cnaes_from_columns('cod_cnae_secundario', 'cnae_secundario')
    
    if not all_cnaes_info:
        common_cnaes = []
    else:
        cnae_pair_counts = Counter(all_cnaes_info)
        common_cnaes = [(code, desc) for (code, desc), freq in cnae_pair_counts.most_common(top_n)]
    
    # Acrescenta condições especiais se necessário
    return common_cnaes

# --- Geração da query SQL sem JOIN com tb_cnae ---

def generate_sql_query(params, base_view, excluded_cnpjs_set=None):
    from sqlalchemy import text

    col_map = {
        'nome_fantasia': 'nome_fantasia_normalizado',
        'uf': 'uf_normalizado',
        'municipio': 'municipio_normalizado',
        'bairro': 'bairro_normalizado',
        'cod_cnae_principal': 'cod_cnae_principal',
        'cod_cnae_secundario': 'cod_cnae_secundario',
        'data_inicio_atividade': 'data_inicio_atividade',
        'capital_social': 'capital_social',
        'porte_empresa': 'porte_empresa',
        'natureza_juridica': 'natureza_juridica',
        'opcao_simples': 'opcao_simples',
        'opcao_mei': 'opcao_mei',
        'qualificacao_socio': 'qualificacoes',
        'faixa_etaria_socio': 'faixas_etarias',
        'ddd1': 'ddd1'
    }

    base_query = f"SELECT *"
    conditions = []
    query_params = {}
    param_counter = 0

    # 1. situacao_cadastral
    if 'situacao_cadastral' in params:
        conditions.append("situacao_cadastral = 'ATIVA'")

    # 2. UF
    if 'uf' in params and params['uf']:
        col_uf = col_map['uf']
        uf_values = [normalize_text(v) for v in params['uf'] if v not in ("(Nulo)", "(Vazio)")]
        uf_conditions = []
        for i, val in enumerate(uf_values):
            key = f"uf_{i}"
            uf_conditions.append(f"{col_uf} = :{key}")
            query_params[key] = val
        if uf_conditions:
            conditions.append("(" + " OR ".join(uf_conditions) + ")")

    # 3. Município
    if 'municipio' in params and params['municipio']:
        col_mun = col_map['municipio']
        mun_values = [normalize_text(v) for v in params['municipio'] if v not in ("(Nulo)", "(Vazio)")]
        mun_conditions = []
        for i, val in enumerate(mun_values):
            key = f"municipio_{i}"
            mun_conditions.append(f"{col_mun} = :{key}")
            query_params[key] = val
        if mun_conditions:
            conditions.append("(" + " OR ".join(mun_conditions) + ")")

    # 4. Outros filtros
    for param, value in params.items():
        if param in ['situacao_cadastral', 'uf', 'municipio']:
            continue
        if value is None or (isinstance(value, list) and not value):
            continue
        col_name = col_map.get(param)
        if not col_name:
            continue

        if param in ['bairro', 'porte_empresa', 'natureza_juridica', 'opcao_simples',
                     'opcao_mei', 'qualificacao_socio', 'faixa_etaria_socio', 'ddd1']:
            actual_values = [normalize_text(v) for v in value if v not in ("(Nulo)", "(Vazio)")]
            include_null = "(Nulo)" in value
            include_empty = "(Vazio)" in value
            param_conditions = []
            for i, val in enumerate(actual_values):
                key = f"{param}_{param_counter}_{i}"
                param_conditions.append(f"{col_name} = :{key}")
                query_params[key] = val
            if include_null:
                param_conditions.append(f"{col_name} IS NULL")
            if include_empty:
                param_conditions.append(f"{col_name} = ''")
            if param_conditions:
                conditions.append("(" + " OR ".join(param_conditions) + ")")
            param_counter += 1

        elif param in ['nome_fantasia', 'nome_socio_razao_social']:
            word_conditions = []
            include_null = "(Nulo)" in value
            include_empty = "(Vazio)" in value
            for word in value:
                if word in ("(Nulo)", "(Vazio)"):
                    continue
                key = f"{param}_{param_counter}"
                query_params[key] = f"%{normalize_text(word)}%"
                word_conditions.append(f"{col_name} ILIKE :{key}")
                param_counter += 1
            if include_null:
                word_conditions.append(f"{col_name} IS NULL")
            if include_empty:
                word_conditions.append(f"{col_name} = ''")
            if word_conditions:
                conditions.append("(" + " OR ".join(word_conditions) + ")")

        elif param == 'capital_social':
            min_val, max_val = value
            key_min = f"cap_min_{param_counter}"
            key_max = f"cap_max_{param_counter}"
            conditions.append(f"{col_name} BETWEEN :{key_min} AND :{key_max}")
            query_params[key_min] = min_val
            query_params[key_max] = max_val
            param_counter += 1

        elif param in ['data_inicio_atividade', 'data_entrada_sociedade']:
            start_date, end_date = value
            key_start = f"data_start_{param_counter}"
            key_end = f"data_end_{param_counter}"
            conditions.append(f"{col_name} BETWEEN :{key_start} AND :{key_end}")
            query_params[key_start] = start_date
            query_params[key_end] = end_date
            param_counter += 1

        elif param in ['cod_cnae_principal', 'cod_cnae_secundario']:
            cnae_conditions = []
            for code, _ in value:
                key = f"{param}_{param_counter}"
                cnae_conditions.append(f"{col_name} = :{key}")
                query_params[key] = code
                param_counter += 1
            if cnae_conditions:
                conditions.append("(" + " OR ".join(cnae_conditions) + ")")

    # 5. Exclusão por CNPJ
    if excluded_cnpjs_set:
        placeholders = []
        for i, cnpj in enumerate(list(excluded_cnpjs_set)):
            key = f"excluded_{i}"
            placeholders.append(f":{key}")
            query_params[key] = cnpj
        conditions.append("cnpj NOT IN (" + ", ".join(placeholders) + ")")

    sql = f"{base_query} FROM {base_view}"
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    return text(sql), query_params

def calculate_score(params):
    score = 0
    for param, value in params.items():
        if value:
            score += PONTUACAO_PARAMETROS.get(param, 0)
    return score

# --- Função para garantir a existência da tabela de leads (cria a tabela conforme a view utilizada) ---
def ensure_leads_table_exists(df_to_save, table_name, expected_cols, engine):
    inspector = inspect(engine)
    
    if not inspector.has_table(table_name):
        st.info(f"Tabela '{table_name}' não encontrada. Criando a tabela...")
        try:
            df_temp = df_to_save.copy()
            df_temp['pontuacao'] = 0
            df_temp['data_geracao'] = datetime.now()
            df_temp['cliente_referencia'] = 'dummy_client'
            
            # Garantir que todas as colunas esperadas existam
            for col in expected_cols:
                if col not in df_temp.columns:
                    # Tipos padrão para criação da coluna – ajuste conforme necessidade
                    if 'capital_social' in col:
                        df_temp[col] = pd.Series(dtype=float)
                    elif 'data_inicio_atividade' in col:
                        df_temp[col] = pd.Series(dtype='datetime64[ns]')
                    else:
                        df_temp[col] = pd.Series(dtype=str)
            
            for col in expected_cols + ['cliente_referencia']:
                if col in df_temp.columns:
                    df_temp[col] = df_temp[col].astype(str).replace({pd.NA: None, 'nan': None, '': None})
            if 'data_inicio_atividade' in df_temp.columns:
                df_temp['data_inicio_atividade'] = pd.to_datetime(df_temp['data_inicio_atividade'], errors='coerce').dt.date
            if 'capital_social' in df_temp.columns:
                df_temp['capital_social'] = pd.to_numeric(df_temp['capital_social'], errors='coerce')
            
            df_temp.head(0).to_sql(table_name, con=engine, if_exists='append', index=False)
            st.success(f"Tabela '{table_name}' criada com colunas iniciais.")
        except Exception as e:
            st.error(f"Erro ao criar a tabela '{table_name}': {e}")
            raise

    # Configuração da chave primária (coluna 'id')
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            columns = inspector.get_columns(table_name)
            id_exists = any(col['name'] == 'id' for col in columns)
            if not id_exists:
                st.info(f"Coluna 'id' não encontrada na tabela '{table_name}'. Adicionando SERIAL PRIMARY KEY...")
                conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN id SERIAL PRIMARY KEY;"))
                st.success("Coluna 'id' adicionada.")
            trans.commit()
        except Exception as e:
            trans.rollback()
            st.error(f"Erro na configuração da chave primária da tabela {table_name}: {e}")
            raise

# Compatibilidade com chave antiga, se necessário
if "df_cnpjs" in st.session_state and "dados_cliente" not in st.session_state: # trecho adicionado para reforçar
    st.session_state.dados_cliente = st.session_state.df_cnpjs # trecho adicionado para reforçar

# --- Início da UI Streamlit ---
df_clientes = st.session_state.get("dados_cliente")

if df_clientes is None or not isinstance(df_clientes, pd.DataFrame) or df_clientes.empty:
    st.warning("Nenhum dado válido carregado. Por favor, volte para a Etapa 1 e carregue os CNPJs.")
    st.stop()

# Só chega aqui se o df_clientes for um DataFrame válido
cnpjs_para_excluir = set(df_clientes['cnpj'].dropna().astype(str).tolist()) if 'cnpj' in df_clientes.columns else set()

# Inicializar tags customizadas para os diversos filtros (exemplo para UF, Município, etc.)
if 'custom_tags_nf' not in st.session_state:
    st.session_state.custom_tags_nf = []
if 'custom_tags_uf' not in st.session_state:
    st.session_state.custom_tags_uf = []
if 'custom_tags_municipio' not in st.session_state:
    st.session_state.custom_tags_municipio = []
if 'custom_tags_bairro' not in st.session_state:
    st.session_state.custom_tags_bairro = []
if 'custom_tags_cnae_principal' not in st.session_state:
    st.session_state.custom_tags_cnae_principal = []
if 'custom_tags_cnae_secundario' not in st.session_state:
    st.session_state.custom_tags_cnae_secundario = []
if 'custom_tags_porte_empresa' not in st.session_state:
    st.session_state.custom_tags_porte_empresa = []
if 'custom_tags_natureza_juridica' not in st.session_state:
    st.session_state.custom_tags_natureza_juridica = []
if 'custom_tags_opcao_simples' not in st.session_state:
    st.session_state.custom_tags_opcao_simples = []
if 'custom_tags_opcao_mei' not in st.session_state:
    st.session_state.custom_tags_opcao_mei = []
if 'custom_tags_ddd1' not in st.session_state:
    st.session_state.custom_tags_ddd1 = []
if 'custom_tags_nome_socio' not in st.session_state:
    st.session_state.custom_tags_nome_socio = []
if 'custom_tags_qualificacao_socio' not in st.session_state:
    st.session_state.custom_tags_qualificacao_socio = []
if 'custom_tags_faixa_etaria_socio' not in st.session_state:
    st.session_state.custom_tags_faixa_etaria_socio = []

st.markdown("## ⚙️ Configuração dos Parâmetros de Busca")
st.subheader("Informações do Cliente")
cliente_referencia = st.text_input("Nome ou ID do Cliente para esta Geração de Leads:", key="cliente_referencia_input")
if not cliente_referencia.strip():
    st.warning("⚠️ Você deve preencher o nome do cliente referência para continuar.")
    st.stop()

ia_params = {}  # Parâmetros para a query
# ---------------------- Seção de filtros (exemplos para Nome Fantasia, UF, Município, Bairro, etc.) ----------------------
# (Abaixo segue a lógica de criação de filtros; esta parte permanece muito similar à versão anterior)
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
            "desde", "até", "após", "entre", "sob", "sobre", "ante", "contra"
        ])
        top_nf_words = get_top_n_words(df_clientes, 'nome_fantasia', top_n_nf, stop_words, include_null=include_null_nf, include_empty=include_empty_nf)
        all_nf_options = list(set(top_nf_words + st.session_state.custom_tags_nf))
        temp_options = [opt for opt in all_nf_options if opt not in ("(Nulo)", "(Vazio)")]
        temp_options.sort()
        if "(Nulo)" in all_nf_options: temp_options.append("(Nulo)")
        if "(Vazio)" in all_nf_options: temp_options.append("(Vazio)")
        default_nf_selection = list(set(top_nf_words + st.session_state.custom_tags_nf))
        if include_null_nf and "(Nulo)" in temp_options: default_nf_selection.append("(Nulo)")
        if include_empty_nf and "(Vazio)" in temp_options: default_nf_selection.append("(Vazio)")
        selected_nf_words = st.multiselect("Palavras-chave selecionadas:", options=temp_options, default=default_nf_selection, key="ia_nf_select")
        ia_params['nome_fantasia'] = selected_nf_words if selected_nf_words else []
        new_nf_tag = st.text_input("Adicionar nova palavra-chave:", key="new_nf_tag_input")
        if new_nf_tag and st.button("Adicionar Tag (Nome Fantasia)", key="add_nf_tag_button"):
            if new_nf_tag.strip() not in st.session_state.custom_tags_nf:
                st.session_state.custom_tags_nf.append(new_nf_tag.strip())
                st.rerun()
    else:
        ia_params['nome_fantasia'] = []

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
        temp_options = [opt for opt in all_uf_options if opt not in ("(Nulo)", "(Vazio)")]
        temp_options.sort()
        if "(Nulo)" in all_uf_options: temp_options.append("(Nulo)")
        if "(Vazio)" in all_uf_options: temp_options.append("(Vazio)")
        default_uf_selection = list(set(top_ufs + st.session_state.custom_tags_uf))
        if include_null_uf and "(Nulo)" in temp_options: default_uf_selection.append("(Nulo)")
        if include_empty_uf and "(Vazio)" in temp_options: default_uf_selection.append("(Vazio)")
        selected_ufs = st.multiselect("UFs selecionadas:", options=temp_options, default=default_uf_selection, key="ia_uf_select")
        ia_params['uf'] = selected_ufs if selected_ufs else []
        new_uf_tag = st.text_input("Adicionar nova UF:", key="new_uf_tag_input")
        if new_uf_tag and st.button("Adicionar Tag (UF)", key="add_uf_tag_button"):
            if new_uf_tag.strip().upper() not in st.session_state.custom_tags_uf:
                st.session_state.custom_tags_uf.append(new_uf_tag.strip().upper())
                st.rerun()
    else:
        ia_params['uf'] = []

# --- Município ---
if 'custom_tags_municipio' not in st.session_state:
    st.session_state.custom_tags_municipio = []
col1_mun, col2_mun, col3_mun = st.columns([1, 1, 2])
with col1_mun:
    use_municipio = st.checkbox("Incluir Município", value=True)
with col2_mun:
    include_null_mun = st.checkbox("Nulo?", key="ia_municipio_null") if use_municipio else False
    include_empty_mun = st.checkbox("Vazio?", key="ia_municipio_empty") if use_municipio else False
with col3_mun:
    if use_municipio:
        top_n_mun = st.slider("Top N Municípios mais frequentes:", min_value=1, max_value=50, value=10, key="ia_top_municipio")
        top_mun = get_unique_values(df_clientes, 'municipio', top_n_mun, include_null=include_null_mun, include_empty=include_empty_mun)
        all_mun_options = list(set(top_mun + st.session_state.custom_tags_municipio))
        temp_options = [opt for opt in all_mun_options if opt not in ("(Nulo)", "(Vazio)")]
        temp_options.sort()
        if "(Nulo)" in all_mun_options: temp_options.append("(Nulo)")
        if "(Vazio)" in all_mun_options: temp_options.append("(Vazio)")
        default_mun = list(set(top_mun + st.session_state.custom_tags_municipio))
        if include_null_mun and "(Nulo)" in temp_options: default_mun.append("(Nulo)")
        if include_empty_mun and "(Vazio)" in temp_options: default_mun.append("(Vazio)")
        selected_mun = st.multiselect("Municípios selecionados:", options=temp_options, default=default_mun, key="ia_municipio_select")
        ia_params['municipio'] = selected_mun if selected_mun else []
        new_mun_tag = st.text_input("Adicionar novo Município:", key="new_mun_tag_input")
        if new_mun_tag and st.button("Adicionar Tag (Município)", key="add_mun_tag_button"):
            if new_mun_tag.strip() not in st.session_state.custom_tags_municipio:
                st.session_state.custom_tags_municipio.append(new_mun_tag.strip())
                st.rerun()
    else:
        ia_params['municipio'] = []

# --- Bairro ---
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
        # Normalização básica
        df_temp_bairro = df_clientes.copy()
        df_temp_bairro['bairro_normalizado'] = df_temp_bairro['bairro'].apply(lambda b: unidecode(str(b).upper().split('/')[0].strip()) if pd.notna(b) else None)
        top_bairro = get_unique_values(df_temp_bairro, 'bairro_normalizado', top_n_bairro, include_null=include_null_bairro, include_empty=include_empty_bairro)
        all_bairro_options = list(set(top_bairro + st.session_state.custom_tags_bairro))
        temp_options = [opt for opt in all_bairro_options if opt not in ("(Nulo)", "(Vazio)")]
        temp_options.sort()
        if "(Nulo)" in all_bairro_options: temp_options.append("(Nulo)")
        if "(Vazio)" in all_bairro_options: temp_options.append("(Vazio)")
        default_bairro = list(set(top_bairro + st.session_state.custom_tags_bairro))
        if include_null_bairro and "(Nulo)" in temp_options: default_bairro.append("(Nulo)")
        if include_empty_bairro and "(Vazio)" in temp_options: default_bairro.append("(Vazio)")
        selected_bairro = st.multiselect("Bairros selecionados:", options=temp_options, default=default_bairro, key="ia_bairro_select")
        ia_params['bairro'] = selected_bairro if selected_bairro else []
        new_bairro_tag = st.text_input("Adicionar novo Bairro:", key="new_bairro_tag_input")
        if new_bairro_tag and st.button("Adicionar Tag (Bairro)", key="add_bairro_tag_button"):
            if new_bairro_tag.strip() not in st.session_state.custom_tags_bairro:
                st.session_state.custom_tags_bairro.append(new_bairro_tag.strip())
                st.rerun()
    else:
        ia_params['bairro'] = []

# --- CNAE Principal ---
if 'custom_tags_cnae_principal' not in st.session_state:
    st.session_state.custom_tags_cnae_principal = []
col1_cnae_p, col2_cnae_p, col3_cnae_p = st.columns([1, 1, 2])
with col1_cnae_p:
    use_cnae_principal = st.checkbox("Incluir CNAE Principal", value=True)
with col2_cnae_p:
    include_null_cnae_p = st.checkbox("Nulo?", key="ia_cnae_p_null") if use_cnae_principal else False
    include_empty_cnae_p = st.checkbox("Vazio?", key="ia_cnae_p_empty") if use_cnae_principal else False
with col3_cnae_p:
    if use_cnae_principal:
        top_n_cnae_p = st.slider("Top N CNAEs Principais mais frequentes:", min_value=1, max_value=50, value=10, key="ia_top_cnae_principal")
        top_cnaes_principal = get_top_n_cnaes(df_clientes, 'principal', top_n_cnae_p, include_null=include_null_cnae_p, include_empty=include_empty_cnae_p)
        options_cnae_p = []
        default_cnae_p = []
        for code, desc in top_cnaes_principal:
            display_str = f"{code} - {desc}"
            options_cnae_p.append(display_str)
            default_cnae_p.append(display_str)
        selected_cnae_p = st.multiselect("CNAEs Principais selecionados:", options=sorted(options_cnae_p), default=default_cnae_p, key="ia_cnae_principal_select")
        # Reconstruir o valor como tuplas
        ia_params['cod_cnae_principal'] = []
        for item in selected_cnae_p:
            parts = item.split(" - ", 1)
            if len(parts) == 2:
                ia_params['cod_cnae_principal'].append((parts[0], parts[1]))
            else:
                ia_params['cod_cnae_principal'].append((item, item))
        new_cnae_p = st.text_input("Adicionar novo CNAE Principal (código ou descrição):", key="new_cnae_p_input")
        if new_cnae_p and st.button("Adicionar Tag (CNAE Principal)", key="add_cnae_p_tag_button"):
            new_tag = (new_cnae_p.strip(), new_cnae_p.strip())
            if new_tag not in st.session_state.custom_tags_cnae_principal:
                st.session_state.custom_tags_cnae_principal.append(new_tag)
                st.rerun()
    else:
        ia_params['cod_cnae_principal'] = []

# --- CNAE Secundário ---
if 'custom_tags_cnae_secundario' not in st.session_state:
    st.session_state.custom_tags_cnae_secundario = []
col1_cnae_s, col2_cnae_s, col3_cnae_s = st.columns([1, 1, 2])
with col1_cnae_s:
    use_cnae_secundario = st.checkbox("Incluir CNAE Secundário", value=False)
with col2_cnae_s:
    include_null_cnae_s = st.checkbox("Nulo?", key="ia_cnae_s_null") if use_cnae_secundario else False
    include_empty_cnae_s = st.checkbox("Vazio?", key="ia_cnae_s_empty") if use_cnae_secundario else False
with col3_cnae_s:
    if use_cnae_secundario:
        top_n_cnae_s = st.slider("Top N CNAEs Secundários mais frequentes:", min_value=1, max_value=50, value=10, key="ia_top_cnae_secundario")
        top_cnaes_secundario = get_top_n_cnaes(df_clientes, 'secundario', top_n_cnae_s, include_null=include_null_cnae_s, include_empty=include_empty_cnae_s)
        options_cnae_s = []
        default_cnae_s = []
        for code, desc in top_cnaes_secundario:
            display_str = f"{code} - {desc}"
            options_cnae_s.append(display_str)
            default_cnae_s.append(display_str)
        selected_cnae_s = st.multiselect("CNAEs Secundários selecionados:", options=sorted(options_cnae_s), default=default_cnae_s, key="ia_cnae_secundario_select")
        ia_params['cod_cnae_secundario'] = []
        for item in selected_cnae_s:
            parts = item.split(" - ", 1)
            if len(parts) == 2:
                ia_params['cod_cnae_secundario'].append((parts[0], parts[1]))
            else:
                ia_params['cod_cnae_secundario'].append((item, item))
        new_cnae_s = st.text_input("Adicionar novo CNAE Secundário (código ou descrição):", key="new_cnae_s_input")
        if new_cnae_s and st.button("Adicionar Tag (CNAE Secundário)", key="add_cnae_s_tag_button"):
            new_tag = (new_cnae_s.strip(), new_cnae_s.strip())
            if new_tag not in st.session_state.custom_tags_cnae_secundario:
                st.session_state.custom_tags_cnae_secundario.append(new_tag)
                st.rerun()
    else:
        ia_params['cod_cnae_secundario'] = []

# --- Data de Início de Atividade ---
col1_data, col2_data, col3_data = st.columns([1, 1, 2])
with col1_data:
    use_data = st.checkbox("Incluir Período de Início de Atividade", value=True)
with col3_data:
    if use_data:
        min_date = df_clientes['data_inicio_atividade'].min() if ('data_inicio_atividade' in df_clientes.columns and not df_clientes['data_inicio_atividade'].empty) else datetime(1900, 1, 1).date()
        max_date = df_clientes['data_inicio_atividade'].max() if ('data_inicio_atividade' in df_clientes.columns and not df_clientes['data_inicio_atividade'].empty) else datetime.now().date()
        if isinstance(min_date, pd.Timestamp): min_date = min_date.date()
        if isinstance(max_date, pd.Timestamp): max_date = max_date.date()
        start_date = st.date_input("Data de Início (De):", value=min_date, min_value=datetime(1900, 1, 1).date(), max_value=datetime.now().date(), key="start_date_input")
        end_date = st.date_input("Data de Início (Até):", value=max_date, min_value=datetime(1900, 1, 1).date(), max_value=datetime.now().date(), key="end_date_input")
        if start_date > end_date:
            st.error("A data 'De' não pode ser posterior à 'Até'.")
            ia_params['data_inicio_atividade'] = None
        else:
            ia_params['data_inicio_atividade'] = (start_date, end_date)
    else:
        ia_params['data_inicio_atividade'] = None

# --- Capital Social ---
col1_cap, col2_cap, col3_cap = st.columns([1, 1, 2])
with col1_cap:
    use_capital = st.checkbox("Incluir Faixa de Capital Social", value=True)
with col3_cap:
    if use_capital:
        min_cap = df_clientes['capital_social'].min() if ('capital_social' in df_clientes.columns and not df_clientes['capital_social'].empty) else 0.0
        max_cap = df_clientes['capital_social'].max() if ('capital_social' in df_clientes.columns and not df_clientes['capital_social'].empty) else 10000000.0
        min_val = st.number_input("Capital Social (Mínimo):", min_value=0.0, value=float(min_cap), step=1000.0, format="%.2f", key="min_capital_input")
        max_val = st.number_input("Capital Social (Máximo):", min_value=0.0, value=float(max_cap), step=1000.0, format="%.2f", key="max_capital_input")
        if min_val > max_val:
            st.error("O valor mínimo não pode ser maior que o máximo.")
            ia_params['capital_social'] = None
        else:
            ia_params['capital_social'] = (min_val, max_val)
    else:
        ia_params['capital_social'] = None

# --- Porte da Empresa ---
if 'custom_tags_porte_empresa' not in st.session_state:
    st.session_state.custom_tags_porte_empresa = []
col1_porte, col2_porte, col3_porte = st.columns([1, 1, 2])
with col1_porte:
    use_porte = st.checkbox("Incluir Porte da Empresa", value=True)
with col2_porte:
    include_null_porte = st.checkbox("Nulo?", key="ia_porte_null") if use_porte else False
    include_empty_porte = st.checkbox("Vazio?", key="ia_porte_empty") if use_porte else False
with col3_porte:
    if use_porte:
        base_portes = ["MICRO EMPRESA", "EMPRESA DE PEQUENO PORTE", "DEMAIS"]
        unique_portes = df_clientes['porte_empresa'].dropna().unique().tolist()
        all_portes = list(set(base_portes + unique_portes + st.session_state.custom_tags_porte_empresa))
        temp_portes = [opt for opt in all_portes if opt not in ("(Nulo)", "(Vazio)")]
        temp_portes.sort()
        if "(Nulo)" in all_portes: temp_portes.append("(Nulo)")
        if "(Vazio)" in all_portes: temp_portes.append("(Vazio)")
        default_portes = list(set(base_portes + unique_portes + st.session_state.custom_tags_porte_empresa))
        if include_null_porte and "(Nulo)" in temp_portes: default_portes.append("(Nulo)")
        if include_empty_porte and "(Vazio)" in temp_portes: default_portes.append("(Vazio)")
        selected_portes = st.multiselect("Selecione o(s) Porte(s) da Empresa:", options=temp_portes, default=default_portes, key="ia_porte_select")
        ia_params['porte_empresa'] = selected_portes if selected_portes else []
        new_porte = st.text_input("Adicionar novo Porte da Empresa:", key="new_porte_input")
        if new_porte and st.button("Adicionar Tag (Porte)", key="add_porte_button"):
            if new_porte.strip() not in st.session_state.custom_tags_porte_empresa:
                st.session_state.custom_tags_porte_empresa.append(new_porte.strip())
                st.rerun()
    else:
        ia_params['porte_empresa'] = []

# --- Natureza Jurídica ---
if 'custom_tags_natureza_juridica' not in st.session_state:
    st.session_state.custom_tags_natureza_juridica = []
col1_nj, col2_nj, col3_nj = st.columns([1, 1, 2])
with col1_nj:
    use_nj = st.checkbox("Incluir Natureza Jurídica", value=False)
with col2_nj:
    include_null_nj = st.checkbox("Nulo?", key="ia_nj_null") if use_nj else False
    include_empty_nj = st.checkbox("Vazio?", key="ia_nj_empty") if use_nj else False
with col3_nj:
    if use_nj:
        top_n_nj = st.slider("Top N Naturezas Jurídicas mais frequentes:", min_value=1, max_value=50, value=10, key="ia_top_nj")
        top_njs = get_unique_values(df_clientes, 'natureza_juridica', top_n_nj, include_null=include_null_nj, include_empty=include_empty_nj)
        all_nj = list(set(top_njs + st.session_state.custom_tags_natureza_juridica))
        temp_nj = [opt for opt in all_nj if opt not in ("(Nulo)", "(Vazio)")]
        temp_nj.sort()
        if "(Nulo)" in all_nj: temp_nj.append("(Nulo)")
        if "(Vazio)" in all_nj: temp_nj.append("(Vazio)")
        default_nj = list(set(top_njs + st.session_state.custom_tags_natureza_juridica))
        if include_null_nj and "(Nulo)" in temp_nj: default_nj.append("(Nulo)")
        if include_empty_nj and "(Vazio)" in temp_nj: default_nj.append("(Vazio)")
        selected_nj = st.multiselect("Naturezas Jurídicas selecionadas:", options=temp_nj, default=default_nj, key="ia_nj_select")
        ia_params['natureza_juridica'] = selected_nj if selected_nj else []
        new_nj = st.text_input("Adicionar nova Natureza Jurídica:", key="new_nj_input")
        if new_nj and st.button("Adicionar Tag (Natureza Jurídica)", key="add_nj_button"):
            if new_nj.strip() not in st.session_state.custom_tags_natureza_juridica:
                st.session_state.custom_tags_natureza_juridica.append(new_nj.strip())
                st.rerun()
    else:
        ia_params['natureza_juridica'] = []

# --- Opção Simples Nacional ---
if 'custom_tags_opcao_simples' not in st.session_state:
    st.session_state.custom_tags_opcao_simples = []
col1_simples, col2_simples, col3_simples = st.columns([1, 1, 2])
with col1_simples:
    use_simples = st.checkbox("Incluir Opção Simples Nacional", value=False)
with col2_simples:
    include_null_simples = st.checkbox("Nulo?", key="ia_simples_null") if use_simples else False
    include_empty_simples = st.checkbox("Vazio?", key="ia_simples_empty") if use_simples else False
with col3_simples:
    if use_simples:
        base_simples = ['S', 'N']
        unique_simples = df_clientes['opcao_simples'].dropna().unique().tolist()
        all_simples = list(set(base_simples + unique_simples + st.session_state.custom_tags_opcao_simples))
        temp_simples = [opt for opt in all_simples if opt not in ("(Nulo)", "(Vazio)")]
        temp_simples.sort()
        if "(Nulo)" in all_simples: temp_simples.append("(Nulo)")
        if "(Vazio)" in all_simples: temp_simples.append("(Vazio)")
        default_simples = list(set(base_simples + unique_simples + st.session_state.custom_tags_opcao_simples))
        if include_null_simples and "(Nulo)" in temp_simples: default_simples.append("(Nulo)")
        if include_empty_simples and "(Vazio)" in temp_simples: default_simples.append("(Vazio)")
        selected_simples = st.multiselect("Optante pelo Simples Nacional?", options=temp_simples, default=default_simples, key="ia_simples_select")
        ia_params['opcao_simples'] = selected_simples if selected_simples else []
        new_simples = st.text_input("Adicionar nova Opção Simples Nacional:", key="new_simples_input")
        if new_simples and st.button("Adicionar Tag (Simples)", key="add_simples_button"):
            if new_simples.strip().upper() not in st.session_state.custom_tags_opcao_simples:
                st.session_state.custom_tags_opcao_simples.append(new_simples.strip().upper())
                st.rerun()
    else:
        ia_params['opcao_simples'] = []

# --- Opção MEI ---
if 'custom_tags_opcao_mei' not in st.session_state:
    st.session_state.custom_tags_opcao_mei = []
col1_mei, col2_mei, col3_mei = st.columns([1, 1, 2])
with col1_mei:
    use_mei = st.checkbox("Incluir Opção MEI", value=False)
with col2_mei:
    include_null_mei = st.checkbox("Nulo?", key="ia_mei_null") if use_mei else False
    include_empty_mei = st.checkbox("Vazio?", key="ia_mei_empty") if use_mei else False
with col3_mei:
    if use_mei:
        base_mei = ['S', 'N']
        unique_mei = df_clientes['opcao_mei'].dropna().unique().tolist()
        all_mei = list(set(base_mei + unique_mei + st.session_state.custom_tags_opcao_mei))
        temp_mei = [opt for opt in all_mei if opt not in ("(Nulo)", "(Vazio)")]
        temp_mei.sort()
        if "(Nulo)" in all_mei: temp_mei.append("(Nulo)")
        if "(Vazio)" in all_mei: temp_mei.append("(Vazio)")
        default_mei = list(set(base_mei + unique_mei + st.session_state.custom_tags_opcao_mei))
        if include_null_mei and "(Nulo)" in temp_mei: default_mei.append("(Nulo)")
        if include_empty_mei and "(Vazio)" in temp_mei: default_mei.append("(Vazio)")
        selected_mei = st.multiselect("Optante pelo MEI?", options=temp_mei, default=default_mei, key="ia_mei_select")
        ia_params['opcao_mei'] = selected_mei if selected_mei else []
        new_mei = st.text_input("Adicionar nova Opção MEI:", key="new_mei_input")
        if new_mei and st.button("Adicionar Tag (MEI)", key="add_mei_button"):
            if new_mei.strip().upper() not in st.session_state.custom_tags_opcao_mei:
                st.session_state.custom_tags_opcao_mei.append(new_mei.strip().upper())
                st.rerun()
    else:
        ia_params['opcao_mei'] = []

# --- Critérios de Contato e Sócios ---
st.subheader("Critérios de Contato e Sócios")

# --- DDD de Contato ---
if 'custom_tags_ddd1' not in st.session_state:
    st.session_state.custom_tags_ddd1 = []
col1_ddd, col2_ddd, col3_ddd = st.columns([1, 1, 2])
with col1_ddd:
    use_ddd = st.checkbox("Incluir DDD de Contato", value=False)
with col2_ddd:
    include_null_ddd = st.checkbox("Nulo?", key="ia_ddd1_null") if use_ddd else False
    include_empty_ddd = st.checkbox("Vazio?", key="ia_ddd1_empty") if use_ddd else False
with col3_ddd:
    if use_ddd:
        top_n_ddd = st.slider("Top N DDDs mais frequentes:", min_value=1, max_value=50, value=10, key="ia_top_ddd")
        unique_ddd = get_unique_values(df_clientes, 'ddd1', top_n_ddd, include_null=include_null_ddd, include_empty=include_empty_ddd)
        all_ddd = list(set(unique_ddd + st.session_state.custom_tags_ddd1))
        temp_ddd = [opt for opt in all_ddd if opt not in ("(Nulo)", "(Vazio)")]
        temp_ddd.sort()
        if "(Nulo)" in all_ddd: temp_ddd.append("(Nulo)")
        if "(Vazio)" in all_ddd: temp_ddd.append("(Vazio)")
        default_ddd = list(set(unique_ddd + st.session_state.custom_tags_ddd1))
        if include_null_ddd and "(Nulo)" in temp_ddd: default_ddd.append("(Nulo)")
        if include_empty_ddd and "(Vazio)" in temp_ddd: default_ddd.append("(Vazio)")
        selected_ddd = st.multiselect("DDDs de interesse:", options=temp_ddd, default=default_ddd, key="ia_ddd1_select")
        ia_params['ddd1'] = selected_ddd if selected_ddd else []
        new_ddd = st.text_input("Adicionar novo DDD:", key="new_ddd_input")
        if new_ddd and st.button("Adicionar Tag (DDD)", key="add_ddd_button"):
            if new_ddd.strip() not in st.session_state.custom_tags_ddd1:
                st.session_state.custom_tags_ddd1.append(new_ddd.strip())
                st.rerun()
    else:
        ia_params['ddd1'] = []

# --- Nome Sócio / Razão Social ---
if 'custom_tags_nome_socio' not in st.session_state:
    st.session_state.custom_tags_nome_socio = []
col1_socio, col2_socio, col3_socio = st.columns([1, 1, 2])
with col1_socio:
    use_socio = st.checkbox("Incluir Nome Sócio / Razão Social (similaridade)", value=False)
with col2_socio:
    include_null_socio = st.checkbox("Nulo?", key="ia_socio_null") if use_socio else False
    include_empty_socio = st.checkbox("Vazio?", key="ia_socio_empty") if use_socio else False
with col3_socio:
    if use_socio:
        unique_socio = df_clientes['nome_socio'].dropna().astype(str).unique().tolist() if 'nome_socio' in df_clientes.columns else []
        all_socio = list(set(unique_socio + st.session_state.custom_tags_nome_socio))
        temp_socio = [opt for opt in all_socio if opt not in ("(Nulo)", "(Vazio)")]
        temp_socio.sort()
        if "(Nulo)" in all_socio: temp_socio.append("(Nulo)")
        if "(Vazio)" in all_socio: temp_socio.append("(Vazio)")
        default_socio = list(set(unique_socio + st.session_state.custom_tags_nome_socio))
        if include_null_socio and "(Nulo)" in temp_socio: default_socio.append("(Nulo)")
        if include_empty_socio and "(Vazio)" in temp_socio: default_socio.append("(Vazio)")
        selected_socio = st.multiselect("Nomes/Partes de nomes selecionados:", options=temp_socio, default=default_socio, key="ia_nome_socio_select")
        ia_params['nome_socio_razao_social'] = selected_socio if selected_socio else []
        new_socio = st.text_input("Adicionar novo Nome Sócio / Razão Social:", key="new_socio_input")
        if new_socio and st.button("Adicionar Tag (Sócio/Razão Social)", key="add_socio_button"):
            if new_socio.strip() not in st.session_state.custom_tags_nome_socio:
                st.session_state.custom_tags_nome_socio.append(new_socio.strip())
                st.rerun()
    else:
        ia_params['nome_socio_razao_social'] = []

# --- Qualificação do Sócio ---
if 'custom_tags_qualificacao_socio' not in st.session_state:
    st.session_state.custom_tags_qualificacao_socio = []
col1_qs, col2_qs, col3_qs = st.columns([1, 1, 2])
with col1_qs:
    use_qs = st.checkbox("Incluir Qualificação do Sócio", value=False)
with col2_qs:
    include_null_qs = st.checkbox("Nulo?", key="ia_qs_null") if use_qs else False
    include_empty_qs = st.checkbox("Vazio?", key="ia_qs_empty") if use_qs else False
with col3_qs:
    if use_qs:
        top_n_qs = st.slider("Top N Qualificações de Sócio mais frequentes:", min_value=1, max_value=50, value=10, key="ia_top_qs")
        top_qs = get_unique_values(df_clientes, 'qualificacao_socio', top_n_qs, include_null=include_null_qs, include_empty=include_empty_qs)
        all_qs = list(set(top_qs + st.session_state.custom_tags_qualificacao_socio))
        temp_qs = [opt for opt in all_qs if opt not in ("(Nulo)", "(Vazio)")]
        temp_qs.sort()
        if "(Nulo)" in all_qs: temp_qs.append("(Nulo)")
        if "(Vazio)" in all_qs: temp_qs.append("(Vazio)")
        default_qs = list(set(top_qs + st.session_state.custom_tags_qualificacao_socio))
        if include_null_qs and "(Nulo)" in temp_qs: default_qs.append("(Nulo)")
        if include_empty_qs and "(Vazio)" in temp_qs: default_qs.append("(Vazio)")
        selected_qs = st.multiselect("Qualificações de sócio selecionadas:", options=temp_qs, default=default_qs, key="ia_qs_select")
        ia_params['qualificacao_socio'] = selected_qs if selected_qs else []
        new_qs = st.text_input("Adicionar nova Qualificação do Sócio:", key="new_qs_input")
        if new_qs and st.button("Adicionar Tag (Qualificação Sócio)", key="add_qs_button"):
            if new_qs.strip() not in st.session_state.custom_tags_qualificacao_socio:
                st.session_state.custom_tags_qualificacao_socio.append(new_qs.strip())
                st.rerun()
    else:
        ia_params['qualificacao_socio'] = []

# --- Faixa Etária do Sócio ---
if 'custom_tags_faixa_etaria_socio' not in st.session_state:
    st.session_state.custom_tags_faixa_etaria_socio = []
col1_faixa, col2_faixa, col3_faixa = st.columns([1, 1, 2])
with col1_faixa:
    use_faixa = st.checkbox("Incluir Faixa Etária do Sócio", value=False)
with col2_faixa:
    include_null_faixa = st.checkbox("Nulo?", key="ia_faixa_null") if use_faixa else False
    include_empty_faixa = st.checkbox("Vazio?", key="ia_faixa_empty") if use_faixa else False
with col3_faixa:
    if use_faixa:
        top_n_faixa = st.slider("Top N Faixas Etárias mais frequentes:", min_value=1, max_value=10, value=5, key="ia_top_faixa")
        top_faixa = get_unique_values(df_clientes, 'faixa_etaria_socio', top_n_faixa, include_null=include_null_faixa, include_empty=include_empty_faixa)
        all_faixa = list(set(top_faixa + st.session_state.custom_tags_faixa_etaria_socio))
        temp_faixa = [opt for opt in all_faixa if opt not in ("(Nulo)", "(Vazio)")]
        temp_faixa.sort()
        if "(Nulo)" in all_faixa: temp_faixa.append("(Nulo)")
        if "(Vazio)" in all_faixa: temp_faixa.append("(Vazio)")
        default_faixa = list(set(top_faixa + st.session_state.custom_tags_faixa_etaria_socio))
        if include_null_faixa and "(Nulo)" in temp_faixa: default_faixa.append("(Nulo)")
        if include_empty_faixa and "(Vazio)" in temp_faixa: default_faixa.append("(Vazio)")
        selected_faixa = st.multiselect("Faixas etárias selecionadas:", options=temp_faixa, default=default_faixa, key="ia_faixa_select")
        ia_params['faixa_etaria_socio'] = selected_faixa if selected_faixa else []
        new_faixa = st.text_input("Adicionar nova Faixa Etária do Sócio:", key="new_faixa_input")
        if new_faixa and st.button("Adicionar Tag (Faixa Etária)", key="add_faixa_button"):
            if new_faixa.strip() not in st.session_state.custom_tags_faixa_etaria_socio:
                st.session_state.custom_tags_faixa_etaria_socio.append(new_faixa.strip())
                st.rerun()
    else:
        ia_params['faixa_etaria_socio'] = []

# --- Fim dos filtros de entrada de parâmetros ---

# Calcular pontuação (opcional)
score = calculate_score(ia_params)
st.info(f"Pontuação dos parâmetros: {score}")

# --- Escolha da view com base nos filtros críticos ---
# Se os filtros críticos de sócios estiverem ativos, usa 'visao_empresa_completa'
BASE_VIEW = "visao_empresa_agrupada_base"
LEADS_TABLE = "tb_leads_gerados"
EXPECTED_COLS = [
    'cnpj', 'razao_social', 'nome_fantasia', 'identificador_matriz_filial', 'data_inicio_atividade',
    'capital_social', 'cod_cnae_principal', 'cnae_principal', 'cod_cnae_secundario', 'cnae_secundario',
    'porte_empresa', 'natureza_juridica', 'opcao_simples', 'opcao_mei', 'motivo', 'situacao_cadastral',
    'data_situacao_cadastral', 'uf', 'municipio', 'bairro', 'logradouro', 'numero', 'complemento', 'cep',
    'latitude', 'longitude', 'ddd1', 'telefone1', 'ddd2', 'telefone2', 'email',
    'qtde_socios', 'nomes_socios', 'cpfs_socios', 'datas_entrada', 'qualificacoes', 'faixas_etarias'
]

st.markdown(f"### Consultando a view: **{BASE_VIEW}**")

# Gerar a query SQL
if st.button("🔍 Gerar Leads com os Filtros Selecionados"):
    try:
        with engine.begin() as conn:
            # 1. Criar a tabela temporária
            conn.execute(text("DROP TABLE IF EXISTS temp_cnpjs_excluir;"))
            conn.execute(text("CREATE TEMP TABLE temp_cnpjs_excluir (cnpj TEXT);"))

            # 2. Inserir os CNPJs
            if cnpjs_para_excluir:
                data = [{'cnpj': c} for c in cnpjs_para_excluir]
                conn.execute(text("INSERT INTO temp_cnpjs_excluir (cnpj) VALUES (:cnpj)"), data)

            # 3. Gerar a query (sem passar o parâmetro de exclusão)
            sql_query, query_params = generate_sql_query(ia_params, BASE_VIEW)

            # 4. Adicionar manualmente a cláusula NOT IN via temp table
            sql_text = str(sql_query)
            if "WHERE" in sql_text:
                sql_text += " AND cnpj NOT IN (SELECT cnpj FROM temp_cnpjs_excluir)"
            else:
                sql_text += " WHERE cnpj NOT IN (SELECT cnpj FROM temp_cnpjs_excluir)"
            sql_query = text(sql_text)

            st.code(str(sql_query), language="sql")

            # 5. Executar a consulta
            result = conn.execute(sql_query, query_params)
            
            
            df_result = pd.DataFrame(result.fetchall(), columns=result.keys())
            df_result = df_result.loc[:, ~df_result.columns.str.endswith('_normalizado')]
            st.session_state.df_leads_gerados = df_result
            
            
            st.success(f"Foram encontrados {df_result.shape[0]} registros.")
            st.dataframe(df_result)
    except Exception as e:
        st.error(f"Erro ao executar a consulta: {e}")

else:
    st.info("Clique no botão acima para gerar os leads.")

# --- Botões de Exportação e Salvamento ---
col_ex, col_save = st.columns(2)
with col_ex:
    if not st.session_state.df_leads_gerados.empty:
        excel_data = to_excel(st.session_state.df_leads_gerados)
        st.download_button(
            "📥 Baixar Excel",
            data=excel_data,
            file_name="leads.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        st.success("Excel gerado e pronto para download.")
        #st.dataframe(st.session_state.df_leads_gerados, use_container_width=True) -- gera o segundo dataframe desnecessário
    else:
        st.warning("Nenhum lead foi gerado ainda. Clique em 'Gerar Leads' antes de exportar.")
with col_save:
    if not st.session_state.df_leads_gerados.empty:
        if st.button("Salvar Leads no Banco de Dados"):
            try:
                df_leads = st.session_state.df_leads_gerados.copy()
                ensure_leads_table_exists(df_leads, LEADS_TABLE, EXPECTED_COLS, engine)
                df_leads['pontuacao'] = score
                df_leads['data_geracao'] = datetime.now()
                df_leads['cliente_referencia'] = cliente_referencia
                
                
                # Remover CNPJs já salvos para o mesmo cliente
                with engine.connect() as conn:
                    query = text(f"SELECT cnpj FROM {LEADS_TABLE} WHERE cliente_referencia = :cliente")
                    result = conn.execute(query, {"cliente": cliente_referencia})
                    cnpjs_ja_salvos = set([row[0] for row in result.fetchall()])

                # Filtrar somente novos
                df_leads = df_leads[~df_leads['cnpj'].isin(cnpjs_ja_salvos)]

                if df_leads.empty:
                    st.warning("Nenhum novo lead para salvar: todos já foram salvos anteriormente para este cliente.")

                else:
                    df_leads.to_sql(LEADS_TABLE, con=engine, if_exists='append', index=False)
                    st.success(f"{df_leads.shape[0]} novos leads salvos para o cliente '{cliente_referencia}'.")
                
                st.success(f"Leads salvos na tabela '{LEADS_TABLE}' com sucesso!")
                st.dataframe(st.session_state.df_leads_gerados, use_container_width=True)
            except Exception as e:
                st.error(f"Erro ao salvar os leads: {e}")
    else:
        st.warning("Nenhum lead para salvar. Clique primeiro em 'Gerar Leads'.")

st.markdown("### Fim da Geração de Leads")
