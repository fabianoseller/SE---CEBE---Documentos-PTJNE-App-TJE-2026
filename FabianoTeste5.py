import pandas as pd
import os

# --- Configurações de Entrada ---
# Use o mesmo caminho de arquivo do seu script principal
caminho_cadunico_csv = r"C:\tab_cad_12062025_43_20250618 (1).csv" 

# As configurações de leitura de CSV geralmente corretas para o CadÚnico:
CSV_SEP = ';'
CSV_ENCODING = 'latin-1' 

# --- Função de Diagnóstico ---
def inspecionar_csv_cadunico(caminho):
    """Lê apenas o cabeçalho e as primeiras 5 linhas do CSV para diagnóstico."""
    
    if not os.path.exists(caminho):
        print(f"\n[ERRO FATAL] Arquivo não encontrado: {caminho}")
        return

    # Saída corrigida (sem emojis)
    print(f"--- Iniciando a inspeção do arquivo: {os.path.basename(caminho)} ---")
    
    try:
        # 1. Leitura do CSV (apenas o cabeçalho e 5 linhas)
        df_head = pd.read_csv(
            caminho, 
            sep=CSV_SEP, 
            encoding=CSV_ENCODING, 
            nrows=5,          # Lê apenas 5 linhas
            on_bad_lines='skip',
            dtype=str         # Força tudo como string para evitar erros de tipo
        )
        
        # Saída corrigida (sem emojis)
        print("\n[SUCESSO] Leitura do cabeçalho bem-sucedida!")
        
        # 2. Exibir o nome exato das colunas
        print("\n>> Nomes EXATOS das Colunas (Copie e Cole para o script principal!):")
        print("-------------------------------------------------------------------")
        # Imprime cada nome de coluna em uma linha para fácil visualização
        for col in df_head.columns:
            print(f"'{col}',") 
        print("-------------------------------------------------------------------")
        
        # 3. Exibir as 3 primeiras linhas (para verificar o conteúdo e a separação)
        print("\n>> Amostra das 3 Primeiras Linhas:")
        print(df_head.head(3).to_markdown(index=False))
        
    except UnicodeDecodeError:
        print("\n[ERRO] Falha de decodificação. Tente mudar o CSV_ENCODING para 'iso-8859-1' ou 'utf-8'.")
    except Exception as e:
        # Se houver uma falha na leitura do CSV, ela será impressa aqui
        print(f"\n[ERRO] Falha durante a leitura do CSV: {e}")

# --- Execução ---
inspecionar_csv_cadunico(caminho_cadunico_csv)