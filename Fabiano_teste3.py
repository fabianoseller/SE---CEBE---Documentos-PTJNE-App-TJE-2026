import os
import datetime as dt 


import ibm_db

conn_str = (
    "DATABASE=BIGSQL;"
    "HOSTNAME=bigsql.pro.intra.rs.gov.br;"
    "PORT=32051;"
    "PROTOCOL=TCPIP;"
    "UID=SVC-HADOOP-PDPSE;"
    "PWD=jFH49mdldg.123;"
)

try:
    conn = ibm_db.connect(conn_str, "", "")
    print("Conexão OK!")
    stmt = ibm_db.exec_immediate(conn, "SELECT 1 FROM SYSIBM.SYSDUMMY1")
    row = ibm_db.fetch_tuple(stmt)
    print("Resultado:", row)
    ibm_db.close(conn)
except Exception as e:
    print("ERRO:", e)










# Adicionado 'dt' para usar a data e hora no nome do arquivo Excel

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
# O import xlwt foi removido.

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


# 5. FUNÇÃO DE EXPORTAÇÃO PARA EXCEL (Sem caracteres Unicode problemáticos)
def exportar_para_excel(df, pasta_destino, nome_base_arquivo):
    """
    Salva o DataFrame em um arquivo Excel (.xlsx) no caminho especificado.
    Requer a instalação da biblioteca 'openpyxl'.
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
        df.to_excel(caminho_completo, index=False, engine='openpyxl')
        
        # SAÍDA CORRIGIDA (Texto simples)
        print(f"\n[SUCESSO] Exportação para Excel concluída!")
        print(f"Arquivo salvo em: {caminho_completo}")

    except Exception as e:
        # SAÍDA CORRIGIDA (Texto simples)
        print("\n[ERRO] Durante a exportação para Excel!")
        print(f"Detalhes do Erro: {e}")


# 6. CONSULTA SQL (Nova consulta fornecida pelo usuário)
query_sql = """
SELECT
    A.CPF,
    A.NM_ALUNO,
    B.ANO_BASE,
    B.DATA_REFERENCIA,
    B.IDT_ESTAB,
    B.NRO_INT_CURSO,
    B.NRO_INT_TURMA,
    B.CD_TURMA,
    B.CD_TURMA_NORMALIZADO,
    B.CHAVE_CONTAGEM_TURMA,
    B.CHAVE_TPO_ENSINO,
    B.TURNO,
    B.IN_BOLSA_FAMILIA,
    B.IN_TJNE,
    B.CD_SEXO,
    B.DS_RACA,
    B.IN_AUTODECLARADO_QUILOMBOLA,
    B.IDADE,
    B.NRO_INT_NECES_ESP,
    B.IDADE_INICIAL_SERIE,
    B.IDADE_FINAL_SERIE,
    B.IN_DISTORCAO_IDADE,
    B.CHAVE_CONTAGEM_MATRICULA,
    B.CHAVE_CONTAGEM_ALUNO
FROM
    PDP_SE_DW.SEDUC_DWF_ALUNO_TURMA B
INNER JOIN PDP_SE_STG.ISE_TURMA T ON
    T.NRO_INT_TURMA = B.NRO_INT_TURMA
INNER JOIN PDP_SE_STG.ISE_ALUNO A ON
    A.NRO_INT_ALUNO_ESTADO = B.CHAVE_CONTAGEM_ALUNO
WHERE
    T.CD_TIPO_ENSINO IN ('R2', 'I2', 'E2')
"""

# 7. EXECUÇÃO PRINCIPAL
df_resultado = consulta_datalake(query_sql)
print("\n--- Resultado do DataFrame ---")
print(df_resultado)


# 8. EXPORTAÇÃO
if 'ERRO' not in df_resultado.columns:
    # A pasta e o nome base do arquivo
    pasta = r"C:\v14\Teste_excel"
    nome = "Detalhe_Alunos_Ensino_Especifico" # Nome do arquivo ajustado para a nova consulta
    
    exportar_para_excel(df_resultado, pasta, nome)
else:
    print("\nNão foi possível exportar para Excel devido a um erro na consulta ou conexão.")