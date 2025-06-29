import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime
import re
from collections import Counter
from unidecode import unidecode

if "df_cnpjs" in st.session_state and "dados_cliente" not in st.session_state: # trecho adicionado para reforçar
    st.session_state.dados_cliente = st.session_state.df_cnpjs # trecho adicionado para reforçar

def etapa2():
    st.header("2️⃣ Análise Gráfica dos Dados Enriquecidos")

    df = st.session_state.get("dados_cliente")

    if df is None or df.empty or not isinstance(df, pd.DataFrame):
        st.warning("Nenhum dado válido carregado. Por favor, volte para a Etapa 1 e carregue os CNPJs.")
        st.stop()

    # Certifica-se de que as colunas necessárias para as análises existentes são numéricas/datetime
    df['capital_social'] = pd.to_numeric(df['capital_social'], errors='coerce').fillna(0)
    df['data_inicio_atividade'] = pd.to_datetime(df['data_inicio_atividade'], errors='coerce')

    hoje = pd.Timestamp.today()
    df['idade'] = (hoje - df['data_inicio_atividade']).dt.days // 365

    bins_idade = [0, 1, 2, 3, 5, 10, float('inf')]
    labels_idade = ["≤1", "1-2", "2-3", "3-5", "5-10", ">10"]
    df['faixa_idade'] = pd.cut(df['idade'], bins=bins_idade, labels=labels_idade, right=False)

    st.markdown("---")
    if st.button("📊 Ir para IA Generator"):
        st.session_state.dados_cliente = st.session_state.df_cnpjs  # 🔁 reforça persistência
        st.switch_page("pages/3_IA_Generator.py")

    # --- Definição das Abas Principais ---
    tab_titles = [
        "Palavras Chave (Nome Fantasia)",
        "Localização",
        "CNAE (Principal e Secundário)", 
        "Porte",
        "Situação Cadastral",
        "Capital Social",
        "Idade da Empresa",
        "Qualificação Sócio",
        "Faixa Etária Sócio"
    ]

    tabs = st.tabs(tab_titles)
    
    # --- Aba 1: Palavras Chave (Nome Fantasia) ---
    with tabs[0]:
        st.subheader("📊 Análise de Palavras-Chave no Nome Fantasia")

        if 'nome_fantasia' in df.columns and not df['nome_fantasia'].empty:
            stop_words = set(unidecode(word.lower()) for word in [
                "e", "de", "do", "da", "dos", "das", "o", "a", "os", "as", "um", "uma", "uns", "umas",
                "para", "com", "sem", "em", "no", "na", "nos", "nas", "ao", "aos", "à", "às",
                "por", "pelo", "pela", "pelos", "pelas", "ou", "nem", "mas", "mais", "menos",
                "desde", "até", "após", "entre", "sob", "sobre", "ante", "após", "contra",
                "desde", "durante", "entre", "mediante", "perante", "salvo", "sem", "sob", "sobre", "trás",
                "s.a", "sa", "ltda", "me", "eireli", "epp", "s.a.", "ltda.", "me.", "eireli.", "epp.",
            ])

            def clean_and_tokenize(text):
                if pd.isna(text):
                    return []
                text = unidecode(str(text)).lower()
                text = re.sub(r'[^\w\s]', '', text)
                text = re.sub(r'\d+', '', text)
                words = [word for word in text.split() if word not in stop_words and len(word) > 1]
                return words

            all_words = df['nome_fantasia'].apply(clean_and_tokenize).explode()
            all_words = all_words.dropna()

            if not all_words.empty:
                word_counts = Counter(all_words)
                top_n = st.slider("Número de palavras para exibir:", min_value=10, max_value=50, value=20, key="top_words_slider")
                top_words = word_counts.most_common(top_n)
                df_top_words = pd.DataFrame(top_words, columns=['Palavra', 'Frequência'])

                fig_words = px.bar(
                    df_top_words,
                    x='Palavra',
                    y='Frequência',
                    title=f'Top {top_n} Palavras Mais Frequentes no Nome Fantasia',
                    labels={'Palavra': 'Palavra', 'Frequência': 'Contagem'},
                    color='Frequência',
                    color_continuous_scale=px.colors.sequential.Viridis
                )
                fig_words.update_layout(xaxis_tickangle=-45)
                st.plotly_chart(fig_words, use_container_width=True)

                st.markdown(f"**Observação:** Esta análise exibe as {top_n} palavras mais comuns nos nomes fantasia dos seus clientes, excluindo termos genéricos e sufixos legais. Isso pode ajudar a identificar tendências e segmentos de mercado para novas prospecções.")
                
                with st.expander("Ver dados brutos das palavras-chave"):
                    st.dataframe(df_top_words, use_container_width=True)
            else:
                st.info("Nenhum nome fantasia válido encontrado para análise de palavras-chave.")
        else:
            st.warning("Coluna 'nome_fantasia' não encontrada ou está vazia no DataFrame de clientes. Certifique-se de que sua base de clientes possui esta coluna.")

    # --- Aba 2: Localização (UF, Município, Bairro) ---
    with tabs[1]:
        st.subheader("📍 Análise por Localização")
        loc_tabs = st.tabs(["Por UF", "Por Município", "Por Bairro"])

        # Análise por UF
        with loc_tabs[0]:
            if 'uf' in df.columns and not df['uf'].empty:
                uf_counts = df['uf'].value_counts().reset_index()
                uf_counts.columns = ['UF', 'Total']
                fig_uf = px.bar(uf_counts, x='UF', y='Total', title='Empresas por UF', color='Total', template='plotly_dark')
                st.plotly_chart(fig_uf, use_container_width=True)
            else:
                st.info("Coluna 'uf' não encontrada ou está vazia.")

        # Análise por Município
        with loc_tabs[1]:
            if 'municipio' in df.columns and not df['municipio'].empty:
                municipio_counts = df['municipio'].value_counts()
                
                # Para visualização, considere agrupar municípios menos frequentes em "Outros"
                top_municipios_n = st.slider("Número de municípios para exibir:", min_value=10, max_value=50, value=20, key="top_municipios_slider")
                top_municipios = municipio_counts.head(top_municipios_n)
                outros_municipios = municipio_counts.iloc[top_municipios_n:].sum()
                
                if outros_municipios > 0:
                    final_municipios_data = pd.concat([top_municipios, pd.Series({'Outros': outros_municipios})]).reset_index()
                else:
                    final_municipios_data = top_municipios.reset_index()

                final_municipios_data.columns = ['Município', 'Total']
                
                fig_municipio = px.pie(
                    final_municipios_data,
                    names='Município',
                    values='Total',
                    title=f'Empresas por Município (Top {top_municipios_n} + Outros)',
                    template='seaborn'
                )
                st.plotly_chart(fig_municipio, use_container_width=True)
            else:
                st.info("Coluna 'municipio' não encontrada ou está vazia.")

        # Análise por Bairro
        with loc_tabs[2]:
            if 'bairro' in df.columns and not df['bairro'].empty:
                def normalizar_bairro(bairro):
                    return unidecode(str(bairro).upper().split('/')[0].strip())

                df_temp = df.copy()
                df_temp['bairro_normalizado'] = df_temp['bairro'].apply(normalizar_bairro)
                
                bairro_counts = df_temp['bairro_normalizado'].value_counts()
                
                top_bairros_n = st.slider("Número de bairros para exibir:", min_value=10, max_value=50, value=20, key="top_bairros_slider")
                top_bairros = bairro_counts.head(top_bairros_n)
                outros_bairros = bairro_counts.iloc[top_bairros_n:].sum()
                
                if outros_bairros > 0:
                    final_bairros_data = pd.concat([top_bairros, pd.Series({'Outros': outros_bairros})]).reset_index()
                else:
                    final_bairros_data = top_bairros.reset_index()

                final_bairros_data.columns = ['Bairro', 'Total']
                
                fig_bairro = px.pie(
                    final_bairros_data,
                    names='Bairro',
                    values='Total',
                    title=f'Empresas por Bairro (Top {top_bairros_n} + Outros)',
                    template='seaborn'
                )
                st.plotly_chart(fig_bairro, use_container_width=True)
            else:
                st.info("Coluna 'bairro' não encontrada ou está vazia no DataFrame de clientes.")

    # --- INÍCIO DA ABA PARA CNAE (AGORA COM GRÁFICO HORIZONTAL E COM CÓDIGO NO DATAFRAME BRUTO) ---
# --- INÍCIO DA ABA PARA CNAE (AGORA COM GRÁFICO HORIZONTAL E COM CÓDIGO NO DATAFRAME BRUTO) ---
    with tabs[2]: # O índice 2 é para a aba CNAE
        st.subheader("📊 Análise de CNAEs (Principal e Secundário)")

        cnae_type = st.radio(
            "Selecione o tipo de CNAE para analisar:",
            ('CNAE Principal', 'CNAEs Secundários', 'Ambos'),
            key="cnae_type_radio"
        )

        all_cnaes_info = [] # Armazenará tuplas (código, descrição)

        # REMOVIDA: A função parse_cnae_string não é mais necessária

        # Processa CNAE Principal
        if cnae_type == 'CNAE Principal' or cnae_type == 'Ambos':
            # AGORA USAMOS 'cod_cnae_principal' E 'cnae_principal'
            if 'cod_cnae_principal' in df.columns and 'cnae_principal' in df.columns and \
               not df['cod_cnae_principal'].empty and not df['cnae_principal'].empty:
                
                temp_df = df[['cod_cnae_principal', 'cnae_principal']].dropna().astype(str)
                for _, row in temp_df.iterrows():
                    code = row['cod_cnae_principal'].strip()
                    description = row['cnae_principal'].strip()
                    if code and description: 
                        all_cnaes_info.append((code, description))
            else:
                st.info("Colunas 'cod_cnae_principal' ou 'cnae_principal' não encontradas ou estão vazias.")

        # Processa CNAEs Secundários
        if cnae_type == 'CNAEs Secundários' or cnae_type == 'Ambos':
            # AGORA USAMOS 'cod_cnae_secundario' E 'cnae_secundario'
            if 'cod_cnae_secundario' in df.columns and 'cnae_secundario' in df.columns and \
               not df['cod_cnae_secundario'].empty and not df['cnae_secundario'].empty:
                
                temp_df = df[['cod_cnae_secundario', 'cnae_secundario']].dropna().astype(str)
                for _, row in temp_df.iterrows():
                    # Supondo que tanto o código quanto a descrição podem vir múltiplos, separados por '; '
                    # e que a ordem se mantém entre eles.
                    codes = row['cod_cnae_secundario'].split('; ') 
                    descriptions = row['cnae_secundario'].split('; ') 

                    for i in range(min(len(codes), len(descriptions))):
                        code = codes[i].strip()
                        description = descriptions[i].strip()
                        if code and description:
                            all_cnaes_info.append((code, description))
            else:
                st.info("Colunas 'cod_cnae_secundario' ou 'cnae_secundario' não encontradas ou estão vazias.")
        
        if all_cnaes_info:
            cnae_pair_counts = Counter(all_cnaes_info)
            
            top_n_cnae = st.slider("Número de CNAEs para exibir:", min_value=10, max_value=50, value=20, key="top_cnaes_slider_horizontal")
            top_cnaes_pairs_freq = cnae_pair_counts.most_common(top_n_cnae)

            df_top_cnaes_data = []
            for (code, description), frequency in top_cnaes_pairs_freq:
                df_top_cnaes_data.append({'CNAE Código': code, 'CNAE Descrição': description, 'Frequência': frequency})
            
            df_top_cnaes = pd.DataFrame(df_top_cnaes_data)

            # Ordenar por frequência para o gráfico e a tabela
            df_top_cnaes = df_top_cnaes.sort_values('Frequência', ascending=False)

            fig_cnaes = px.bar(
                df_top_cnaes,
                x='Frequência',          # Frequência no eixo X
                y='CNAE Descrição',      # CNAE Descrição no eixo Y (vertical)
                orientation='h',         # Gráfico de barras horizontal
                title=f'Top {top_n_cnae} CNAEs Mais Frequentes ({cnae_type})',
                labels={'CNAE Descrição': 'CNAE (Descrição Completa)', 'Frequência': 'Contagem'}, 
                color='Frequência',
                color_continuous_scale=px.colors.sequential.Plasma,
                hover_data=['CNAE Código', 'CNAE Descrição', 'Frequência'] 
            )
            # Ajustes para melhor visualização do texto no eixo Y (CNAEs)
            fig_cnaes.update_layout(yaxis={'categoryorder':'total ascending'}) 
            st.plotly_chart(fig_cnaes, use_container_width=True)

            st.markdown(f"**Observação:** Esta análise exibe os {top_n_cnae} CNAEs mais comuns (principais e/ou secundários, dependendo da sua seleção). Os rótulos completos são visíveis diretamente no gráfico.")

            with st.expander("Ver dados brutos dos CNAEs"):
                st.dataframe(df_top_cnaes, use_container_width=True) 
        else:
            st.info("Nenhum CNAE válido encontrado para análise. Verifique as colunas de CNAE e CNAE Código.")
    # --- FIM DA ABA CNAE ---

    # --- Abas Existentes (Índices ajustados) ---
    with tabs[3]: 
        st.subheader("📊 Análise por Porte da Empresa")
        porte_counts = df['porte_empresa'].value_counts().reset_index()
        porte_counts.columns = ['Porte da Empresa', 'Total']
        fig_porte = px.pie(porte_counts, names='Porte da Empresa', values='Total', title='Empresas por Porte', template='seaborn')
        st.plotly_chart(fig_porte, use_container_width=True)

    with tabs[4]: 
        st.subheader("📊 Análise por Situação Cadastral")
        situacao_counts = df['situacao_cadastral'].value_counts().reset_index()
        situacao_counts.columns = ['Situação Cadastral', 'Total']
        fig_situacao = px.bar(situacao_counts, x='Situação Cadastral', y='Total', color='Total', template='plotly_dark')
        st.plotly_chart(fig_situacao, use_container_width=True)

    with tabs[5]: 
        st.subheader("📊 Análise por Faixa de Capital Social")
        bins_capital = [0, 1000, 10000, 50000, 100000, 500000, 1000000, float('inf')]
        labels_capital = ["<1k", "1k-10k", "10k-50k", "50k-100k", "100k-500k", "500k-1M", ">1M"]
        df['faixa_capital'] = pd.cut(df['capital_social'], bins=bins_capital, labels=labels_capital, right=False)

        cap_counts = df['faixa_capital'].value_counts().sort_index().reset_index()
        cap_counts.columns = ['Faixa de Capital', 'Quantidade']
        st.plotly_chart(px.bar(cap_counts, x='Faixa de Capital', y='Quantidade', color='Quantidade', title='Empresas por Faixa de Capital Social', template='plotly_dark'))

    with tabs[6]: 
        st.subheader("📊 Análise por Faixa de Idade da Empresa")
        idade_counts = df['faixa_idade'].value_counts().sort_index().reset_index()
        idade_counts.columns = ['Faixa de Idade', 'Quantidade']
        st.plotly_chart(px.bar(idade_counts, x='Faixa de Idade', y='Quantidade', color='Quantidade', title='Empresas por Faixa de Idade', template='plotly_dark'))

    with tabs[7]: 
        st.subheader("📊 Análise por Qualificação do Sócio")
        if 'qualificacoes' in df.columns and not df['qualificacoes'].isna().all():
            df_q = df[['qualificacoes']].dropna().copy()
            df_q['qualificacoes'] = df_q['qualificacoes'].astype(str).str.split('|')
            df_q_exploded = df_q.explode('qualificacoes')
            df_q_exploded['qualificacoes'] = df_q_exploded['qualificacoes'].str.strip()
            qual_counts = df_q_exploded['qualificacoes'].value_counts().reset_index()
            qual_counts.columns = ['Qualificação', 'Total']
            fig_qual = px.bar(qual_counts, x='Qualificação', y='Total', color='Total', title='Qualificação do Sócio', template='plotly_dark')
            st.plotly_chart(fig_qual, use_container_width=True)
        else:
            st.info("Coluna 'qualificacoes' não encontrada ou está vazia.")

    with tabs[8]: 
        st.subheader("📊 Análise por Faixa Etária do Sócio")
        if 'faixas_etarias' in df.columns and not df['faixas_etarias'].isna().all():
            df_fe = df[['faixas_etarias']].dropna().copy()
            df_fe['faixas_etarias'] = df_fe['faixas_etarias'].astype(str).str.split('|')
            df_fe_exploded = df_fe.explode('faixas_etarias')
            df_fe_exploded['faixas_etarias'] = df_fe_exploded['faixas_etarias'].str.strip()
            fe_counts = df_fe_exploded['faixas_etarias'].value_counts().reset_index()
            fe_counts.columns = ['Faixa Etária', 'Total']
            fig_fe = px.bar(fe_counts, x='Faixa Etária', y='Total', color='Total', title='Faixa Etária do Sócio', template='plotly_dark')
            st.plotly_chart(fig_fe, use_container_width=True)
        else:
            st.info("Coluna 'faixa_etaria_socio' não encontrada ou está vazia.")



etapa2()
