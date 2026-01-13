Path_Base = '/Users/nsweige/Desktop/Tudo/'
Path_Downloads = '/Users/fabiano-antunes/Desktop/Tudo'
import os
 
os.add_dll_directory("C:/Users/fabiano-antunes/AppData/Local/Programs/Python/Python312/Lib/site-packages/clidriver/bin")
# configuração da conexão ao banco de dados
import ibm_db

# manipulação de dados
import numpy as np
import pandas as pd
# oculta warnings do tipo mudar tipo de coluna sem utilizar o loc
pd.options.mode.chained_assignment = None  # default='warn'
pd.set_option('display.max_columns', None)


from sqlalchemy import create_engine, inspect

# manipulacion excel
import xlwt

# para limpar célula
import IPython

import re

import xlsxwriter # pyright: ignore[reportMissingImports]

# Configuração da conexão ao banco de dados
dsn_driver = "{IBM DB2 ODBC DRIVER}"
dsn_database = "BIGSQL"
dsn_hostname = "bigsql.pro.intra.rs.gov.br"
dsn_port = "32051"
dsn_protocol = "TCPIP"
dsn_uid = "SVC-HADOOP-PDPSE"
dsn_pwd = "jFH49mdldg.123"

# Criação da string de conexão do SQLAlchemy
dsn = (
    f"ibm_db_sa://{dsn_uid}:{dsn_pwd}@{dsn_hostname}:{dsn_port}/{dsn_database}"
)

# renomeia todas colunas do df, transformando em maiusculas
def upper_columns(df):
    for col in df.columns:
        df = df.rename(columns={col: col.upper()})
    return df

# guarda um dataframe em um arquivo excel, e da um auto ajuste nos tamanhos de colunas
def salva_com_auto_fit_colunas(df, path, sheet_name):
    # salva arquivo, transforma em tabela e ajusta a largura das colunas
    writer = pd.ExcelWriter(path, engine='xlsxwriter')

    # caso passe um dataframe só, faz o procedimento normal
    # é tratado assim, para manter compatibilidade, além de não precisar criar uma lista de um elemento sempre que for salvar um dataframe só(maioria das vezes)
    if type(df) != list:
        df.to_excel(writer, sheet_name=sheet_name, index=False)

        worksheet = writer.sheets[sheet_name] 
        workbook = writer.book
        
        workbook.set_properties(
            {
                "author": "Nikolas Weige",
                "company": "CEBE",
                "comments": "CEBE - Criação do arquivo automatizada com Python"
            }
        )

        if type(df) == pd.core.series.Series:
            col_names = [{'header': df.name}]
            worksheet.add_table(0, 0, df.shape[0], 0, {'columns': col_names, 'style': 'Table Style Medium 3'})
        else:
            col_names = [{'header': col_name} for col_name in df.columns]
            worksheet.add_table(0, 0, df.shape[0], df.shape[1]-1, {'columns': col_names, 'style': 'Table Style Medium 3'})

        worksheet.autofit()
        worksheet.set_tab_color("#FF2D00")

    # se df passado for uma lista
    else:
        # para conseguir salvar múltiplos dataframes em uma só planilha, o número de nomes de planilhas deve ser igual o número de dataframes
        if(len(df) != len(sheet_name)) or type(sheet_name) != list:
            print("Tamanho da lista de dataframes é diferente da lista de nomes de planilhas")
        
        else:
            for i in range(len(df)):
                df[i].to_excel(writer, sheet_name=sheet_name[i], index=False)

                worksheet = writer.sheets[sheet_name[i]] 
                workbook = writer.book
                
                workbook.set_properties(
                    {
                        "author": "Nikolas Weige",
                        "company": "CEBE",
                        "comments": "CEBE - Criação do arquivo automatizada com Python"
                    }
                )

                if type(df[i]) == pd.core.series.Series:
                    col_names = [{'header': df[i].name}]
                    worksheet.add_table(0, 0, df[i].shape[0], 0, {'columns': col_names, 'style': 'Table Style Medium 3'})
                else:
                    col_names = [{'header': col_name} for col_name in df[i].columns]
                    worksheet.add_table(0, 0, df[i].shape[0], df[i].shape[1]-1, {'columns': col_names, 'style': 'Table Style Medium 3'})

                worksheet.autofit()
                worksheet.set_tab_color("#FF2D00")

    # da warning mas é necessário, se não aparece como se Python estivesse utilizando arquivo
    writer.close()

# guarda um dataframe em um arquivo excel, e da um auto ajuste nos tamanhos de colunas
properties = {
    "author": "Nikolas Weige",
    "company": "CEBE",
    "comments": "CEBE - Criação do arquivo automatizada com Python"
}

# guarda um dataframe em um arquivo excel, e da um auto ajuste nos tamanhos de colunas
def salva_com_auto_fit_colunas_grafico(df, path, sheet_name, graph_type, graph_subtype, data_label, titulo, legenda):
    # para cada planilha preenchida é anotado até qual linha foi preenchida
    # caso apareça outro df para a mesma planilha, começa a escrevê-lo apartir da última linha preenchida
    dict_start_row = {}
    
    # salva arquivo, transforma em tabela e ajusta a largura das colunas
    writer = pd.ExcelWriter(path, engine='xlsxwriter')
    workbook = writer.book          
    workbook.set_properties(properties)

    # caso passe um dataframe só, faz o procedimento normal
    # é tratado assim, para manter compatibilidade, além de não precisar criar uma lista de um elemento sempre que for salvar um dataframe só(maioria das vezes)
    if type(df) != list:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        worksheet = writer.sheets[sheet_name] 

        if type(df) == pd.core.series.Series:
            worksheet.add_table(0, 0, df.shape[0], 0, {'columns': df.name, 'style': 'Table Style Dark 11'})
        else:
            col_names = [{'header': col_name} for col_name in df.columns]
            worksheet.add_table(0, 0, df.shape[0], df.shape[1]-1, {'columns': col_names, 'style': 'Table Style Dark 11'})

        # se type for passado, significa que um gráfico deve ser criado
        # só foi feito para funcionar para gráficos do tipo column, mas talvez funcione para outros
        if graph_type != False:
            # cria um gráfico do tipo informado
            if graph_subtype == False:
                chart = workbook.add_chart({'type': graph_type})
            else:
                chart = workbook.add_chart({'type': graph_type, 'subtype': graph_subtype})
            
            # número de linhas da tabela de bolsas por série pivotada
            linhas = df.shape[0]

            ## configura o gráfico, name contém a legenda, categories contém cada valor do eixo x, nesse caso os meses, e values os valores para cada série
            for j in range(len(df.columns) - 1):
                letra = chr(j + 66)
                chart.add_series({'name': f'={sheet_name}!${letra}$1', 'categories': f'={sheet_name}!$A$2:$A${2+linhas-1}', 'values': f'={sheet_name}!${letra}$2:${letra}${2+linhas-1}', 'data_labels': {'value': data_label}})
            
            # insere o gráfico criado
            if titulo != False:
                chart.set_title({'name': titulo})
            worksheet.insert_chart(f'{chr(j + 68)}2', chart)

        worksheet.autofit()
        worksheet.set_tab_color("#FF2D00")

    # se df passado for uma lista
    else:
        # para conseguir salvar múltiplos dataframes em uma só planilha, o número de nomes de planilhas deve ser igual o número de dataframes
        if(len(df) != len(sheet_name)) or type(sheet_name) != list:
            print("Tamanho da lista de dataframes é diferente da lista de nomes de planilhas")
        
        else:
            for i in range(len(df)):
                if type(df[i]) == str:
                    worksheet = workbook.add_worksheet(sheet_name[i])
                    worksheet.insert_textbox('B2', df[i], {'font': {'italic': True, 'size': 13}, 'width': 1024, 'height': 512})
                    worksheet.set_tab_color("#FF2D00")
                else:
                    # se planilha ja foi preenchida, preenche depois da última linha preenchida
                    if sheet_name[i] in dict_start_row:
                        df[i].to_excel(writer, sheet_name=sheet_name[i], startrow=dict_start_row[sheet_name[i]]+1, index=False)
                    else:
                        df[i].to_excel(writer, sheet_name=sheet_name[i], index=False)
    
                    worksheet = writer.sheets[sheet_name[i]]
                    
                    if sheet_name[i] in dict_start_row:
                        if type(df[i]) == pd.core.series.Series:
                            worksheet.add_table(f'A{dict_start_row[sheet_name[i]]+2}:A{dict_start_row[sheet_name[i]]+df[i].shape[0]+2}', {'columns': df[i].name, 'style': 'Table Style Dark 11'})
                        else:
                            col_names = [{'header': col_name} for col_name in df[i].columns]
                            letra_fim = chr(df[i].shape[1] + 64)
                            worksheet.add_table(f'A{dict_start_row[sheet_name[i]]+2}:{letra_fim}{dict_start_row[sheet_name[i]]+df[i].shape[0]+2}', {'columns': col_names, 'style': 'Table Style Dark 11'})
                    else:
                        if type(df[i]) == pd.core.series.Series:
                            worksheet.add_table(0, 0, df[i].shape[0], 0, {'columns': df[i].name, 'style': 'Table Style Medium 3'})
                        else:
                            col_names = [{'header': col_name} for col_name in df[i].columns]
                            worksheet.add_table(0, 0, df[i].shape[0], df[i].shape[1]-1, {'columns': col_names, 'style': 'Table Style Medium 3'})
    
                    # se type for passado, significa que um gráfico deve ser criado
                    # só foi feito para funcionar para gráficos do tipo column, mas talvez funcione para outros
                    if graph_type[i] != False:
                        # cria um gráfico do tipo informado
                        if graph_subtype[i] == False:
                            chart = workbook.add_chart({'type': graph_type[i]})
                        else:
                            chart = workbook.add_chart({'type': graph_type[i], 'subtype': graph_subtype[i]})
                        
                        # número de linhas da tabela de bolsas por série pivotada
                        linhas = df[i].shape[0]
    
                        if sheet_name[i] in dict_start_row:
                            base = dict_start_row[sheet_name[i]] + 1
                
                            ## configura o gráfico, name contém a legenda, categories contém cada valor do eixo x, nesse caso os meses, e values os valores para cada série
                            for j in range(len(df[i].columns) - 1):
                                letra = chr(j + 66)
                                chart.add_series({'name': f'={sheet_name[i]}!${letra}${1 + base}', 'categories': f'={sheet_name[i]}!$A${base+2}:$A${2+base+linhas-1}', 'values': f'={sheet_name[i]}!${letra}${2+base}:${letra}${2+base+linhas-1}', 'data_labels': {'value': data_label[i]}})
                        
                            # insere o gráfico criado
                            worksheet.insert_chart(f'{chr(j + 69)}{dict_start_row[sheet_name[i]]}', chart)
                        else:
                            ## configura o gráfico, name contém a legenda, categories contém cada valor do eixo x, nesse caso os meses, e values os valores para cada série
                            for j in range(len(df[i].columns) - 1):
                                letra = chr(j + 66)
                                chart.add_series({'name': f'={sheet_name[i]}!${letra}$1', 'categories': f'={sheet_name[i]}!$A$2:$A${2+linhas-1}', 'values': f'={sheet_name[i]}!${letra}$2:${letra}${2+linhas-1}', 'data_labels': {'value': data_label[i]}})
                        
                            # insere o gráfico criado
                            if titulo[i] != False:
                                chart.set_title({'name': titulo[i]})
                            if legenda[i] != False:
                                chart.set_legend({'none': True})
                            worksheet.insert_chart(f'{chr(j + 69)}2', chart)
    
                    # atualiza dicionário das linhas preenchidas por planilha
                    if sheet_name[i] in dict_start_row:
                        dict_start_row[sheet_name[i]] = dict_start_row[sheet_name[i]] + df[i].shape[0] + 3
                    else:
                        dict_start_row[sheet_name[i]] = df[i].shape[0] + 2
    
                    worksheet.autofit()
                    worksheet.set_tab_color("#FF2D00")

    # da warning mas é necessário, se não aparece como se Python estivesse utilizando arquivo
    writer.close()

# query para obter frequências do ensino médio no datalake
def query_freq_ens_med(incluir_2022=False):
    if incluir_2022 == False:
        query = f"""
        SELECT *
        FROM(
            SELECT *, ROUND((1 - ((CAST(FALTAS_TOTAIS AS DECIMAL) - (CAST(FALTAS_JUSTIFICADAS_TOTAIS AS DECIMAL))) / CAST(AULAS_TOTAIS AS DECIMAL))), 7) AS FREQ_DATALAKE
            FROM(
                SELECT NRO_INT_ALUNO_ESTADO, ANO, MES, 
                        SUM(AULAS) AS AULAS_TOTAIS, 
                        SUM(FALTAS) AS FALTAS_TOTAIS,
                        SUM(FALTAS_JUSTIFICADAS) AS FALTAS_JUSTIFICADAS_TOTAIS
                FROM (SELECT NRO_INT_ALUNO_ESTADO, ANO, MES, AULAS, FALTAS, FALTAS_JUSTIFICADAS
                        FROM (
                            SELECT NRO_INT_ALUNO_ESTADO, ANO, MES, AULAS, FALTAS, FALTAS_JUSTIFICADAS
                            FROM PDP_SE_DW.SEDUC_DWF_FREQUENCIA_MENSAL_ALUNO_SIT AS A
                            LEFT JOIN (SELECT NRO_INT_TURMA, CD_TIPO_ENSINO
                                    FROM PDP_SE_STG.ISE_TURMA) AS B
                            ON (A.NRO_INT_TURMA = B.NRO_INT_TURMA)
                            WHERE (CD_TIPO_ENSINO = 'I2' OR CD_TIPO_ENSINO = 'E2' OR CD_TIPO_ENSINO = 'R2')
                            AND ANO IN (2023, 2024, 2025)
                            ))
                GROUP BY NRO_INT_ALUNO_ESTADO, ANO, MES)
        )
        """
    else:
        query = f"""
        SELECT *
        FROM(
            SELECT *, ROUND((1 - ((CAST(FALTAS_TOTAIS AS DECIMAL) - (CAST(FALTAS_JUSTIFICADAS_TOTAIS AS DECIMAL))) / CAST(AULAS_TOTAIS AS DECIMAL))), 7) AS FREQ_DATALAKE
            FROM(
                SELECT NRO_INT_ALUNO_ESTADO, ANO, MES, 
                        SUM(AULAS) AS AULAS_TOTAIS, 
                        SUM(FALTAS) AS FALTAS_TOTAIS,
                        SUM(FALTAS_JUSTIFICADAS) AS FALTAS_JUSTIFICADAS_TOTAIS
                FROM (SELECT NRO_INT_ALUNO_ESTADO, ANO, MES, AULAS, FALTAS, FALTAS_JUSTIFICADAS
                        FROM (
                            SELECT NRO_INT_ALUNO_ESTADO, ANO, MES, AULAS, FALTAS, FALTAS_JUSTIFICADAS
                            FROM PDP_SE_DW.SEDUC_DWF_FREQUENCIA_MENSAL_ALUNO_SIT AS A
                            LEFT JOIN (SELECT NRO_INT_TURMA, CD_TIPO_ENSINO
                                    FROM PDP_SE_STG.ISE_TURMA) AS B
                            ON (A.NRO_INT_TURMA = B.NRO_INT_TURMA)
                            WHERE (CD_TIPO_ENSINO = 'I2' OR CD_TIPO_ENSINO = 'E2' OR CD_TIPO_ENSINO = 'R2')
                            ))
                GROUP BY NRO_INT_ALUNO_ESTADO, ANO, MES)
        )
        """
    return query

# recebe uma string que contém uma consulta em sql e retorna um dataframe com a tabela obtida
def consulta_datalake(query):
    # Tentativa de conexão ao banco de dados
    try:
        engine = create_engine(dsn)
        df = pd.read_sql(query, engine)
        
        # Fechar a conexão
        engine.dispose()
        return df

    except Exception as e:
        print(e)
        return pd.DataFrame({'ERRO' : []})
    
# mesma ideia da fix_data_comp(função acima), só que recebe somente o mes em texto, e caso possua um digito retorna com um zero antes(ex.: 1 -> 01)
def formata_mes(mes):
    if len(mes) == 1:
        mes = '0' + mes
    return mes

# arredonda e transforma a frequência em inteiro
def formata_freq_int(freq):
    freq = np.ceil(100 * freq)
    freq = int(freq)
    freq = str(freq) + '%'
    return freq
    
# formata frequência pós-consulta datalake no formato normalmente utilizado
def formata_frequencia(freq_mensal, inteira=False):
    # converte meses em 2 dígitos (ex.: 1 => 01, 2 => 02, ...)
    freq_mensal['mes'] = freq_mensal['mes'].astype(str)
    freq_mensal['mes'] = freq_mensal['mes'].apply(formata_mes)


    # concatena ano e mes
    #freq_mensal['data_comp'] = freq_mensal[{'ano', 'mes'}].apply(lambda row: ''.join(row.values.astype(str)), axis=1)
    freq_mensal['ano'] = freq_mensal['ano'].astype(str)
    freq_mensal['data_comp'] = freq_mensal["ano"] + '/'+freq_mensal["mes"]

    # remove colunas, deixando só data_comp, freq_datalake e matricula
    freq_mensal = freq_mensal.drop(columns=['ano', 'mes', 'aulas_totais', 'faltas_totais', 'faltas_justificadas_totais'])
    freq_mensal = freq_mensal.rename(columns={'nro_int_aluno_estado': 'MATRICULA',
                                            'data_comp': 'DATA_COMPETENCIA',
                                            'freq_datalake': 'FREQ_DATALAKE'})
    freq_mensal['FREQ_DATALAKE'] = freq_mensal['FREQ_DATALAKE'].clip(0, 1)
    
    # se flag inteira for verdadeira então retorna a frequência no formato de inteiro, e não de porcentagem como usual
    if inteira == True:
        freq_mensal['FREQ_DATALAKE'] = freq_mensal['FREQ_DATALAKE'].apply(formata_freq_int)

    return freq_mensal

# função que lê o cad e retorna um dataframe com as colunas informadas
def ler_cad(versao='agosto', colunas=['COD_CPF', 'NOME', 'COD_NIS', 'COD_FAM', 'DTA_NASC', 'COD_PARENTESCO', 'NOME_MAE', 'NOME_PAI', 'RG', 'VLR_RENDA_MEDIA'], manter_cpfs_duplicados=False):
    # se for texto da verão, converte arquivo baseado no texto, caso contrário, trata como caminho para o arquivo
    if versao == 'agosto':
        arquivo = Path_Base + 'cads/tab_cad_08082025_43_20250909.csv'
    elif versao == 'junho':
        arquivo = Path_Base + 'cads/tab_cad_12062025_43_20250618.csv'
    elif versao == 'fevereiro':
        arquivo = Path_Base + 'cads/tab_cad_08022025_43_20250306.csv'
    elif versao == 'agosto_24':
        arquivo = Path_Base + 'cads/tab_cad_17082024_43_20240910.csv'
    elif versao == 'maio':
        arquivo = Path_Base + 'cads/tab_cad_11052024_43_20240604.csv'
    elif versao == 'janeiro':
        arquivo = Path_Base + 'cads/tab_cad_13012024_43_20240206.csv'
    elif versao == 'novembro':
        arquivo = Path_Base + 'cads/tab_cad_09112024_43_20241203.csv'
    elif versao == 'dezembro':
        arquivo = Path_Base + 'cads/tab_cad_14122024_43_20250107.csv'
    else:
        arquivo = versao

    # ao ler o arquivo puro do cad as colunas estarão com o nome base, mas normalmente se pede com outros nome, que são tratados nesse dicionario
    dict_colunas = {
                    'COD_CPF': 'p.num_cpf_pessoa',
                    'NOME': 'p.nom_pessoa',
                    'COD_NIS': 'p.num_nis_pessoa_atual',
                    'COD_FAM': 'd.cod_familiar_fam',
                    'DTA_NASC': 'p.dta_nasc_pessoa',
                    'COD_PARENTESCO': 'p.cod_parentesco_rf_pessoa',
                    'NOME_MAE': 'p.nom_completo_mae_pessoa',
                    'NOME_PAI': 'p.nom_completo_pai_pessoa',
                    'RG': 'p.num_identidade_pessoa',
                    'VLR_RENDA_MEDIA': 'd.vlr_renda_media_fam',
                    'VLR_RENDA_TOTAL': 'd.vlr_renda_total_fam',
                    'IND_PAB': 'd.marc_pbf',
                    'BAIRRO': 'd.nom_localidade_fam',
                    'TIPO_END': 'd.nom_tip_logradouro_fam',
                    'TITULO_END': 'd.nom_titulo_logradouro_fam',
                    'NOME_END': 'd.nom_logradouro_fam',
                    'NUM_END': 'd.num_logradouro_fam',
                    'COMPLEM_1': 'd.des_complemento_fam',
                    'COMPLEM_2': 'd.des_complemento_adic_fam',
                    'CEP': 'd.num_cep_logradouro_fam',
                    'CD_IBGE': 'd.cd_ibge',
                    'DDD_CONTATO1': 'd.num_ddd_contato_1_fam',
                    'NUM_TEL_CONTATO1': 'd.num_tel_contato_1_fam',
                    'DDD_CONTATO2': 'd.num_ddd_contato_2_fam',
                    'NUM_TEL_CONTATO2': 'd.num_tel_contato_2_fam',
                    'FX_RFPC': 'd.fx_rfpc',
                    'NOME_PESSOA': 'p.nom_pessoa',
                    'VLR_RENDA_MEDIA_FAM': 'd.vlr_renda_media_fam',
                    'VLR_RENDA_TOTAL_FAM': 'd.vlr_renda_total_fam',
                    'COD_FAMILIAR_FAM': 'd.cod_familiar_fam',
                    'IND_PBF': 'd.marc_pbf',
                    'COD_INEP': 'd.cd_ibge'
    }

    # lê só a primeira linha para pegar os nomes das colunas
    colunas_cad = pd.read_csv(arquivo, delimiter=';', encoding='1252', nrows=0).columns

    # busca verdadeiro nome da coluna informada
    colunas_finais = []
    dict_colunas_finais = {}
    for col in colunas:
        # se aparece nas chaves é pq foi pedido coluna ja com nome final, então lê nome base e adiciona ao dict da renomeação nome informado
        if col in dict_colunas.keys():
            colunas_finais.append(dict_colunas[col])
            dict_colunas_finais[dict_colunas[col]] = col
        # se aparece nos valores é pq se quer o nome puro da coluna, então só adiciona a lista de leitura, sem precisar renomear
        elif (col in dict_colunas.values()) or (col in colunas_cad):
            colunas_finais.append(col)

    cad = pd.read_csv(arquivo, usecols=colunas_finais, sep=';', encoding='1252')
    # se tiver alguma coluna pra renomear, renomeia
    if dict_colunas_finais.keys():
        cad = cad.rename(columns=dict_colunas_finais)

    # remove cpfs duplicados
    if manter_cpfs_duplicados == False:
        # remove todos cpfs duplicados mantendo oq possui data de atualização mais recente, valores nulos não são considerados duplicados então mantém
        cad_sem_na = pd.read_csv(arquivo, usecols=['p.num_cpf_pessoa', 'd.dat_atual_fam'], sep=';', encoding='1252').dropna(subset='p.num_cpf_pessoa')
        cad_sem_na['d.dat_atual_fam'] = pd.to_datetime(cad_sem_na['d.dat_atual_fam'], format='%Y-%m-%d')
        cad_sem_na_ordenado = cad_sem_na.sort_values(by='d.dat_atual_fam', ascending=False)

        # calcula cpfs duplicados
        duplicados = cad_sem_na_ordenado[cad_sem_na_ordenado.duplicated(subset='p.num_cpf_pessoa', keep=False)]
        # calcula cpfs duplicados de atualização mais recente 
        cpfs_duplicados_manter = duplicados.drop_duplicates(subset='p.num_cpf_pessoa', keep='first')
        # todos que forem duplicados e não são o mais recente, são removidos
        cpfs_duplicados_remover = duplicados.drop(index=cpfs_duplicados_manter.index)
        cad = cad.drop(index=cpfs_duplicados_remover.index)

    # usa a ordem informada no chamado da função, só considerando as colunas válidas
    lista_ordenada_colunas = sorted(cad.columns, key=lambda x: colunas.index(x))
    return cad[lista_ordenada_colunas]

# busca tabelas que contenham colunas informadas por regex
def buscar_tabelas(esquema, regex_colunas):
    engine = create_engine(dsn)
    inspector = inspect(engine)
    tabelas_com_colunas = {}

    for tabela in inspector.get_table_names(schema=esquema):
        colunas = [col["name"] for col in inspector.get_columns(tabela, schema=esquema)]
        
        colunas_encontradas = {regex: [col for col in colunas if re.search(regex, col, re.IGNORECASE)] for regex in regex_colunas}

        # retorna apenas tabelas que tenham colunas correspondentes a TODOS os regex
        if all(colunas_encontradas.values()):
            tabelas_com_colunas[tabela] = sum(colunas_encontradas.values(), [])

    return tabelas_com_colunas