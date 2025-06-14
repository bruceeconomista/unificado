# Arquivo para utilidades e session_state compartilhado

# utils.py

import pandas as pd
from unidecode import unidecode

# --- CONSTANTES DE PREÇO CENTRALIZADAS ---
PRECO_POR_CNPJ_ENRIQUECIDO = 0.05 
CUSTO_BASE_POR_CNPJ_OPORTUNIDADE = 0.10
CUSTO_ADICIONAL_POR_BAIRRO = 0.03
CUSTO_ADICIONAL_POR_MUNICIPIO = 0.07

def normalizar_bairro(bairro):
    """Normaliza o nome do bairro para comparação, removendo acentos e convertendo para maiúsculas."""
    if isinstance(bairro, str):
        return unidecode(bairro.upper().split('/')[0].strip())
    return bairro # Retorna como está se não for string (e.g., NaN)

def calcular_custo_oportunidades(df_oportunidades_calc):
    """
    Calcula o custo simulado para o download de oportunidades não atendidas.
    A lógica considera custo base por CNPJ e custos adicionais por bairros e municípios únicos.
    """
    if df_oportunidades_calc.empty:
        return 0.0, 0, 0, 0 # Custo, num_cnpjs, num_bairros, num_municipios

    num_cnpjs = len(df_oportunidades_calc)
    
    # Assegurar que as colunas estão limpas e normalizadas antes de contar únicos
    # É importante que esta normalização ocorra aqui se a função for usada de forma independente
    # ou que a normalização já tenha ocorrido no DataFrame antes de ser passado para cá.
    # No seu 4_Mapa_de_Oportunidades.py, a normalização já ocorre antes, o que é ideal.
    
    # Se 'bairro' e 'municipio' já foram normalizados na etapa4(),
    # essas linhas podem ser omitidas aqui para evitar reprocessamento,
    # mas mantê-las garante robustez se a função for chamada de outro lugar.
    # Para o seu caso, como a etapa4() já normaliza, podemos assumir que estão prontos.
    
    num_bairros_unicos = df_oportunidades_calc['bairro'].nunique()
    num_municipios_unicos = df_oportunidades_calc['municipio'].nunique()

    custo_total = (num_cnpjs * CUSTO_BASE_POR_CNPJ_OPORTUNIDADE) + \
                  (num_bairros_unicos * CUSTO_ADICIONAL_POR_BAIRRO) + \
                  (num_municipios_unicos * CUSTO_ADICIONAL_POR_MUNICIPIO)
    
    return custo_total, num_cnpjs, num_bairros_unicos, num_municipios_unicos

# Se houver outras funções utilitárias, adicione-as aqui.