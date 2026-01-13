import os
# ========================================================================
# 1. TRATAMENTO ESSENCIAL DE DLLs (DEVE SER A PRIMEIRA AÇÃO)
# Adiciona o caminho das DLLs do clidriver antes de qualquer importação
# que as utilize (como ibm_db/ibm_db_sa)
# ========================================================================
# O 'try/except' é uma boa prática para evitar falhas em ambientes diferentes do Windows
try:
    os.add_dll_directory("C:/Users/fabiano-antunes/AppData/Local/Programs/Python/Python312/Lib/site-packages/clidriver/bin")
    print("Caminho da DLL do DB2 adicionado.")
except AttributeError:
    # Para sistemas operacionais mais antigos ou Linux/macOS, onde esta função não existe
    print("A função os.add_dll_directory não é suportada ou necessária.")
# ========================================================================


# 2. IMPORTS ESSENCIAIS (Feitos apenas uma vez)
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, inspect
import ibm_db # Necessário para o driver 'ibm_db_sa'
import xlwt # Para manipulação de Excel, se for usar

# 3. CONFIGURAÇÕES GLOBAIS DE PANDAS
pd.options.mode.chained_assignment = None  # Oculta warnings de 'SettingWithCopyWarning'
pd.set_option('display.max_columns', None)

import ibm_db
dsn_db = "BIGSQL"
dsn_hostname = "pedrs.intra.rs.gov.br"
dsn_port = "31653"
dsn_ID = "fa5077419@estado.intra.rs.gov.br"
dsn_pwd = "c9QZf38Dye2XHb!" 

conn_str = f"DATABASE = {dsn_db};HOSTNAME={dsn_hostname};PORT={dsn_port};PROTOCOL=TCPIP;UID={dsn_ID};PWD={dsn_pwd};"
conn = ibm_db.connect(conn_str, "", "")


# 5. FUNÇÃO DE CONSULTA
def consulta_datalake(query):
    # Recebe uma string que contém uma consulta em sql e retorna um dataframe com a tabela obtida
    try:
        engine = create_engine(dsn)
        # O pandas cuida da abertura e fechamento da conexão automaticamente com read_sql
        df = pd.read_sql(query, engine) 
        # Força o fechamento de todas as conexões no pool após o uso
        engine.dispose()
        print("Consulta executada com sucesso.")
        return df

    except Exception as e:
        print(" Erro ao executar a consulta:")
        print(e)
        return pd.DataFrame({'ERRO' : [str(e)]}) # Garante que o retorno é sempre um DataFrame
    
# 6. EXECUÇÃO DA CONSULTA
query_sql = f"""
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

df = consulta_datalake(query_sql)
print(df)

# Bloco de teste de conexão removido para simplificação.
# Se precisar, use a função 'consulta_datalake' com uma query simples como 'SELECT 1 FROM sysibm.sysdummy1'.