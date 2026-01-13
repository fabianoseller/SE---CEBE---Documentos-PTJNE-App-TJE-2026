import os
import datetime as dt 
# Bibliotecas para Big Data:

# ========================================================================
# 1. TRATAMENTO ESSENCIAL DE DLLs (DEVE VIR PRIMEIRO)
# ========================================================================
dll_path = "C:/Users/fabiano-antunes/AppData/Local/Programs/Python/Python312/Lib/site-packages/clidriver/bin"

print("--- Verificando e adicionando o caminho das DLLs do DB2 ---")
if os.name == 'nt' and os.path.isdir(dll_path):
    try:
        os.add_dll_directory(dll_path)
        print(f"Caminho da DLL do DB2 adicionado com sucesso: {dll_path}")
    except AttributeError:
        print("Aviso: Falha ao usar os.add_dll_directory. Verifique a versão do Python.")
elif os.name != 'nt':
    print("Sistema operacional não-Windows. Ignorando a adição de caminho de DLL.")
print("-----------------------------------------------------------\n")


# 2. IMPORTS ESSENCIAIS (APÓS A CORREÇÃO DE DLL)
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import ibm_db 
import pyarrow 


# 3. CONFIGURAÇÕES & DADOS DA CONEXÃO
pd.options.mode.chained_assignment = None 
pd.set_option('display.max_columns', None)

dsn_database = "BIGSQL"
dsn_hostname = "bigsql.pro.intra.rs.gov.br"
dsn_port = "32051"
dsn_uid = "SVC-HADOOP-PDPSE"
dsn_pwd = "jFH49mdldg.123"

dsn = (
    f"ibm_db_sa://{dsn_uid}:{dsn_pwd}@{dsn_hostname}:{dsn_port}/{dsn_database}"
)


# 4. DICIONÁRIO DE COLUNAS (CadÚnico - Validado)
# Lista de colunas a SEREM LIDOS do CSV CadÚnico (Usecols):
COLUNAS_PARA_MANTER = [
    'd.cd_ibge', 'd.cod_familiar_fam', 'd.nom_localidade_fam', 'd.nom_tip_logradouro_fam', 
    'd.nom_titulo_logradouro_fam', 'd.nom_logradouro_fam', 'd.num_logradouro_fam', 
    'd.des_complemento_fam', 'd.des_complemento_adic_fam', 'd.num_cep_logradouro_fam', 
    'd.vlr_renda_media_fam', 'd.fx_rfpc', 'd.vlr_renda_total_fam', 'd.marc_pbf', 
    'd.num_ddd_contato_1_fam', 'd.num_tel_contato_1_fam', 'd.num_ddd_contato_2_fam', 
    'd.num_tel_contato_2_fam', 'p.nom_pessoa', 'p.num_nis_pessoa_atual', 
    'p.dta_nasc_pessoa', 'p.num_cpf_pessoa', 'p.cod_parentesco_rf_pessoa', 
    'p.grau_instrucao'
]
# Mapeamento de Renomeação (NOVO_NOME: NOME_ORIGINAL_NO_CSV)
MAP_RENOMEACAO = {
    'p.num_cpf_pessoa': 'CPF_CADUNICO', 
    'p.nom_pessoa': 'NOME_PESSOA_CADUNICO',
    'p.dta_nasc_pessoa': 'DATA_NASC_CADUNICO',
    'd.vlr_renda_media_fam': 'RENDA_MEDIA_FAM',
    'd.vlr_renda_total_fam': 'RENDA_TOTAL_FAM',
    'd.marc_pbf': 'IND_PAB_FAM',
    'd.num_cep_logradouro_fam': 'CEP',
    'd.cod_familiar_fam': 'COD_FAMILIAR',
}


# 5. FUNÇÕES DE OPERAÇÃO

def consulta_datalake(query):
    """Conecta ao Data Lake via SQLAlchemy e retorna um DataFrame."""
    try:
        engine = create_engine(dsn)
        df = pd.read_sql(query, engine) 
        engine.dispose()
        
        print("\n--- Consulta DATA LAKE executada com sucesso. ---")
        return df
    except Exception as e:
        print("\n--- ERRO ao executar a consulta no DATA LAKE! ---")
        print(f"Detalhes do Erro: {e}")
        return pd.DataFrame({'ERRO' : [str(e)]})


def processar_cadunico_grande(caminho_csv, caminho_parquet_saida, colunas_interesse, map_renomeacao, chunk_size=500000):
    """Lê o CSV grande em blocos, filtra, padroniza CPF e salva em Parquet."""
    if os.path.exists(caminho_parquet_saida):
        print(f"\n[INFO] Arquivo Parquet já existe: {caminho_parquet_saida}. Pulando o processamento do CSV.")
        return True
        
    print(f"\n--- Iniciando o processamento do CadÚnico em blocos ({chunk_size} linhas/bloco) ---")
    try:
        csv_iterator = pd.read_csv(
            caminho_csv, sep=';', encoding='latin-1', dtype=str,
            usecols=colunas_interesse, chunksize=chunk_size,
            on_bad_lines='skip', iterator=True
        )
        df_chunks = []
        for i, chunk in enumerate(csv_iterator):
            print(f"  > Processando Bloco {i + 1}...", end='\r') 
            chunk.rename(columns=map_renomeacao, inplace=True)
            if 'CPF_CADUNICO' in chunk.columns:
                chunk['CPF_PADRONIZADO'] = chunk['CPF_CADUNICO'].astype(str).str.replace(r'[^\d]', '', regex=True)
            df_chunks.append(chunk)
        
        print(f"\n  > Processamento de todos os blocos concluído.")
        df_final = pd.concat(df_chunks, ignore_index=True)
        os.makedirs(os.path.dirname(caminho_parquet_saida), exist_ok=True)
        df_final.to_parquet(caminho_parquet_saida, index=False)
        print(f"\n[SUCESSO] Base CadÚnico processada e salva em Parquet. Linhas Salvas: {len(df_final)}")
        return True
    except FileNotFoundError:
        print(f"\n[ERRO FATAL] Arquivo CadÚnico CSV não encontrado: {caminho_csv}")
        return False
    except Exception as e:
        print(f"\n[ERRO FATAL] Falha no processamento do CSV: {e}")
        return False
        
def exportar_para_excel(df, pasta_destino, nome_base_arquivo):
    """Salva o DataFrame em um arquivo Excel (.xlsx) na pasta especificada. (Sem Unicode)"""
    try:
        if not os.path.exists(pasta_destino):
            os.makedirs(pasta_destino)
            print(f"Pasta criada: {pasta_destino}")
            
        timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        nome_arquivo = f"{nome_base_arquivo}_{timestamp}.xlsx"
        caminho_completo = os.path.join(pasta_destino, nome_arquivo)
        
        df.to_excel(caminho_completo, index=False, engine='openpyxl')
        
        print(f"\n[SUCESSO] Exportação para Excel concluída!")
        print(f"Arquivo salvo em: {caminho_completo}")

    except Exception as e:
        print("\n[ERRO] Durante a exportação para Excel!")
        print(f"Detalhes do Erro: {e}")


# 6. CONSULTA SQL (Base de Alunos - REDUZIDA PARA OTIMIZAÇÃO DE MEMÓRIA)
query_sql = """
SELECT
    A.CPF AS cpf,
    A.NM_ALUNO AS nm_aluno,
    B.IDT_ESTAB AS idt_estab,
    B.NRO_INT_CURSO AS nro_int_curso,
    B.NRO_INT_TURMA AS nro_int_turma,
    B.ANO_BASE AS ano_base,
    B.DATA_REFERENCIA AS data_referencia,
    B.IN_BOLSA_FAMILIA AS in_bolsa_familia,
    B.IN_TJNE AS in_tjne,
    B.IDADE AS idade,
    B.IN_DISTORCAO_IDADE AS in_distorcao_idade,
    B.CHAVE_CONTAGEM_ALUNO AS chave_contagem_aluno
FROM
    PDP_SE_DW.SEDUC_DWF_ALUNO_TURMA B
INNER JOIN PDP_SE_STG.ISE_TURMA T ON
    T.NRO_INT_TURMA = B.NRO_INT_TURMA
INNER JOIN PDP_SE_STG.ISE_ALUNO A ON
    A.NRO_INT_ALUNO_ESTADO = B.CHAVE_CONTAGEM_ALUNO
WHERE
    T.CD_TIPO_ENSINO IN ('R2', 'I2', 'E2')
"""


# 7. EXECUÇÃO PRINCIPAL E CRUZAMENTO
pasta_excel = r"C:\v14\Teste_excel"
caminho_cadunico_csv = r"C:\tab_cad_12062025_43_20250618 (1).csv"
caminho_parquet = r"C:\v14\dataframes\cadunico_base.parquet" 


# 7.1. PRIMEIRO: Obter base de alunos do Data Lake (SQL)
print("Etapa 1: Lendo dados SQL do Data Lake (Colunas Reduzidas)...")
df_alunos = consulta_datalake(query_sql)

if 'ERRO' in df_alunos.columns:
    print("\nExecução abortada devido a erro na consulta SQL.")
    exit()


# 7.2. SEGUNDO: Processar e carregar o CadÚnico (CSV/Parquet)
print("\nEtapa 2: Processando/Carregando base CadÚnico...")
sucesso_cadunico = processar_cadunico_grande(
    caminho_cadunico_csv, 
    caminho_parquet, 
    COLUNAS_PARA_MANTER, 
    MAP_RENOMEACAO
)

if not sucesso_cadunico:
    print("\nExecução abortada devido a erro no processamento do CadÚnico CSV.")
    exit()

# Carregar o CadÚnico (versão leve em Parquet)
try:
    df_cadunico = pd.read_parquet(caminho_parquet)
    print(f"[INFO] CadÚnico carregado da base Parquet: {len(df_cadunico)} linhas.")
except Exception as e:
    print(f"\n[ERRO FATAL] Falha ao carregar o Parquet: {e}")
    exit()


# 7.3. Realizar cruzamento (MERGE)
print("\nEtapa 3: Realizando o cruzamento das bases...")
    
# 7.3.1. Padronizar CPF da base de alunos
# Corrigido para usar 'cpf' (minúsculo) conforme o diagnóstico.
df_alunos['CPF_PADRONIZADO'] = df_alunos['cpf'].astype(str).str.replace(r'[^\d]', '', regex=True)

# 7.3.2. Merge: Junção interna (Inner Join) - **OPERAÇÃO MAIS LEVE**
MapeadosTJEprojeto = pd.merge(
    df_alunos, 
    df_cadunico.drop(columns=['CPF_CADUNICO'], errors='ignore'),
    on='CPF_PADRONIZADO', 
    how='inner', 
    suffixes=('_ALUNO', '_CADUNICO')
)

# 7.3.3. Limpeza Final
MapeadosTJEprojeto = MapeadosTJEprojeto.drop(columns=['CPF_PADRONIZADO'])

print("\n=======================================================")
print(f"✅ CRUZAMENTO CONCLUÍDO!")
print(f"Linhas mapeadas (MapeadosTJEprojeto): {len(MapeadosTJEprojeto)}")
print("=======================================================")

# Exibe as colunas chave do resultado
colunas_chave = ['cpf', 'nm_aluno', 'DATA_NASC_CADUNICO', 'RENDA_MEDIA_FAM', 'CEP']
print(MapeadosTJEprojeto[[c for c in colunas_chave if c in MapeadosTJEprojeto.columns]].head())

# 7.4. Exportação
nome_exportacao = "MapeadosTJEprojeto_Cadunico"
exportar_para_excel(MapeadosTJEprojeto, pasta_excel, nome_exportacao)