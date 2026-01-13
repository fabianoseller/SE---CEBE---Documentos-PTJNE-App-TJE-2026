import os
import datetime as dt # Adicionado para usar a data no nome do arquivo

# ========================================================================
# 1. TRATAMENTO ESSENCIAL DE DLLs (Prioridade no Windows)
# Adiciona o caminho das DLLs do clidriver ANTES de qualquer importação
# que as utilize (ibm_db ou sqlalchemy)
# ========================================================================
dll_path = "C:/Users/fabiano-antunes/AppData/Local/Programs/Python/Python312/Lib/site-packages/clidriver/bin"

if os.name == 'nt' and os.path.isdir(dll_path):
    try:
        os.add_dll_directory(dll_path)
        print(f"Caminho da DLL do DB2 adicionado: {dll_path}")
    except AttributeError:
        print("Aviso: Falha ao usar os.add_dll_directory. Verifique a versão do Python.")
elif os.name != 'nt':
    print("Sistema operacional não-Windows. Ignorando a adição de caminho de DLL.")

# ========================================================================


# 2. IMPORTS ESSENCIAIS
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import ibm_db 
# O import xlwt não é mais necessário, pois usamos a função nativa do pandas com openpyxl


# Oculta warnings do tipo mudar tipo de coluna sem utilizar o loc
pd.options.mode.chained_assignment = None
pd.set_option('display.max_columns', None)


# 3. DADOS DA CONEXÃO
dsn_database = "BIGSQL"
dsn_hostname = "bigsql.pro.intra.rs.gov.br"
dsn_port = "32051"
dsn_uid = "SVC-HADOOP-PDPSE"
dsn_pwd = "jFH49mdldg.123"

# Criação da string de conexão do SQLAlchemy
dsn = (
    f"ibm_db_sa://{dsn_uid}:{dsn_pwd}@{dsn_hostname}:{dsn_port}/{dsn_database}"
)


# 4. FUNÇÃO DE CONSULTA
def consulta_datalake(query):
    """
    Recebe uma string SQL, conecta ao Datalake e retorna um DataFrame.
    """
    try:
        engine = create_engine(dsn)
        df = pd.read_sql(query, engine) 
        engine.dispose()
        # Saída corrigida (sem emojis)
        print("\n--- Consulta executada com sucesso. ---")
        return df

    except Exception as e:
        # Saída corrigida (sem emojis)
        print("\n--- ERRO ao executar a consulta! ---")
        print(f"Detalhes do Erro: {e}")
        return pd.DataFrame({'ERRO' : [str(e)]})


# 5. FUNÇÃO DE EXPORTAÇÃO PARA EXCEL (CORRIGIDA)
def exportar_para_excel(df, pasta_destino, nome_base_arquivo):
    """
    Salva o DataFrame em um arquivo Excel (.xlsx) no caminho especificado.
    """
    try:
        # 1. Cria a pasta se ela não existir
        if not os.path.exists(pasta_destino):
            os.makedirs(pasta_destino)
            print(f"Pasta criada: {pasta_destino}")
            
        # 2. Gera um nome de arquivo único com data e hora
        timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        nome_arquivo = f"{nome_base_arquivo}_{timestamp}.xlsx"
        caminho_completo = os.path.join(pasta_destino, nome_arquivo)
        
        # 3. Salva o DataFrame no Excel
        # index=False evita salvar a coluna de índice do DataFrame no Excel
        df.to_excel(caminho_completo, index=False, engine='openpyxl')
        
        # SAÍDA CORRIGIDA (Texto simples)
        print(f"\n[SUCESSO] Exportação para Excel concluída com sucesso!")
        print(f"Arquivo salvo em: {caminho_completo}")

    except Exception as e:
        # SAÍDA CORRIGIDA (Texto simples)
        print("\n[ERRO] Durante a exportação para Excel!")
        print(f"Detalhes do Erro: {e}")


# 6. CONSULTA SQL
query_sql = """
WITH AlunosUnicos AS (
    SELECT DISTINCT
        IE.DENOMINACAO,
        IE.IDT_ESTAB,
        IAT.NRO_INT_ALUNO_ESTADO,
        TS.DS_SITUACAO
        FROM PDP_SE_STG.ISE_TURMA IT
    LEFT JOIN PDP_SE_STG.ISE_ALUNO_TURMA IAT ON IT.NRO_INT_TURMA = IAT.NRO_INT_TURMA
    LEFT JOIN PDP_SE_STG.ISE_ESTABELECIMENTO IE ON IT.IDT_ESTAB = IE.IDT_ESTAB
    INNER JOIN PDP_SE_STG.ISE_TIPO_SITUACAO_ALUNO TS ON IAT.NRO_INT_SITUACAO = TS.NRO_INT_SITUACAO
    INNER JOIN PDP_SE_STG.ISE_CALENDARIO_ESTAB CE 
        ON CE.NRO_INT_CALEND_ESTAB = IT.NRO_INT_CALEND_ESTAB 
        AND CURRENT DATE BETWEEN CE.DT_INICIO_ATIV AND CE.DT_FIM_ATIV
    WHERE 
        IT.IDT_ESTAB IN (1968, 11061, 17439, 3836, 16001, 21330, 7904, 21320, 13729, 13728)
        AND TS.DS_SITUACAO IN ('Matriculado', 'Infrequente')
)
SELECT 
    IDT_ESTAB,
    DENOMINACAO,
    COUNT(DISTINCT NRO_INT_ALUNO_ESTADO) AS QTD_ALUNOS
FROM AlunosUnicos
GROUP BY DENOMINACAO, IDT_ESTAB
ORDER BY QTD_ALUNOS DESC;
"""

# 7. EXECUÇÃO PRINCIPAL
df_resultado = consulta_datalake(query_sql)
print("\n--- Resultado do DataFrame ---")
print(df_resultado)


# 8. EXPORTAÇÃO
if 'ERRO' not in df_resultado.columns:
    # Se a coluna 'ERRO' não existir, significa que a consulta foi bem-sucedida.
    pasta = r"C:\v14\Teste_excel" # Usando 'r' para strings brutas (raw string)
    nome = "Alunos_Unicos"
    exportar_para_excel(df_resultado, pasta, nome)
else:
    print("\nNão foi possível exportar para Excel devido a um erro na consulta.")