<img width="1908" height="937" alt="TELA1" src="https://github.com/user-attachments/assets/957a6567-80ab-4710-b7f6-6dbe8a18daa6" />
<img width="1894" height="759" alt="TELA2" src="https://github.com/user-attachments/assets/e2930043-221b-4974-b288-77ac89fc12e1" />
# TJE Control • Cartões  
## Documentação Oficial do Projeto

**Projeto:** TJE Control – Cartões  
**Documento:** Documentação Oficial Consolidada  
**Versão:** v1.0  
**Data:** 2025-11-13  
**Responsável Técnico:** Fabiano Seller  
**Stack:** Python · FastAPI · DB2 · DuckDB · Pandas · HTML · CSS · JavaScript  

---

## 1. Contexto do Projeto

O **TJE Control • Cartões** nasceu da necessidade de **processar, cruzar, auditar e gerar bases oficiais de beneficiários** a partir de:

- **Base de alunos do Data Lake (DB2)**
- **CadÚnico (arquivos CSV grandes / Parquet)**

O sistema deve garantir:
- leitura **completa** dos dados,
- **cruzamento confiável por CPF**,
- **eliminação de duplicidades**,
- **monitoramento técnico em tempo real**,
- **exportação oficial de arquivos para pagamento**.

Desde o início, ficou claro que **simulações não são aceitáveis** para o núcleo do sistema.

---

## 2. Objetivo Central (Cerne do Sistema)

O objetivo principal do app é executar, de ponta a ponta, o seguinte fluxo **real**:

1. Conectar ao **DB2**
2. Ler **todos os alunos elegíveis**
3. Ler **todo o CadÚnico**
4. Normalizar CPFs
5. Realizar **merge (cruzamento)**
6. Remover duplicidades
7. Gerar bases finais oficiais
8. Permitir auditoria técnica completa

Tudo isso com **logs, métricas e rastreabilidade**.

---

## 3. Evolução do Projeto (Resumo do Chat)

### Principais desafios enfrentados

- Erros de DLL do DB2 (clidriver em caminhos diferentes)
- Diferença entre ambientes Windows
- SQLAlchemy indisponível em alguns contextos
- Rollbacks DB2 (`SQL1229N`, `SQL1042C`)
- Estouro de memória ao tentar ler todos os alunos
- Falhas de merge por ausência de coluna `cpf`
- Frontend não refletindo o estado real do backend
- Regressões (funções sumindo entre versões)

### Decisões importantes tomadas

- **DuckDB adotado** para merge em disco (escala)
- **Leitura em lotes** dos alunos do DB2
- **Fallback automático** para CSV/Parquet
- **Persistência intermediária** (evitar memória)
- **Painel técnico** no frontend
- **Polling de logs e métricas**
- **Manutenção obrigatória de funcionalidades já existentes**

---

## 4. Arquitetura Geral

### 4.1 Backend

- **FastAPI**
- **ibm_db / ibm_db_dbi** (DB2 real)
- **DuckDB** (merge em disco)
- **Pandas**
- **OpenPyXL / XlsxWriter**
- **Logs persistentes + buffer em memória**

### 4.2 Frontend

- HTML + CSS + JS puro
- Painel técnico lateral fixo
- Polling de logs (2s)
- Painel de fases (6 etapas)
- Métricas em tempo real
- Tema claro/escuro
- Documentação embutida
- Animações de processamento (CSS)

---

## 5. Estrutura de Pastas Oficial


app/
├── app.py
├── index.html
├── _logs/
│   └── app_runtime.log
├── dataframes/
│   ├── cadunico_base.parquet
│   └── cadunico_base.csv
├── outputs/
│   └── {MESANO}/
│       ├── MapeadosTJEprojeto_Matriculas_*.xlsx
│       └── MapeadosTJEprojeto_PessoasUnicas_*.xlsx



# Documentação Oficial do Projeto: Fluxo de Processamento

## 1. Fluxo Oficial de Processamento (6 Fases)

### Fase 1 – Leitura de Alunos (DB2)
* **Conexão:** Real via DB2 com query ao Data Lake.
* **Processamento:** Leitura em lotes com escrita em **DuckDB (disco)**.
* **Normalização de colunas:** `cpf`, `nm_aluno`, `matricula`.
* **Logs:** Registro detalhado por lote.
* **Problemas Resolvidos:** * Estouro de memória.
    * Rollback de transação DB2.
    * Ambientes sem SQLAlchemy.

### 🔹 Fase 2 – Leitura do CadÚnico
* **Prioridade de Arquivos:** Parquet > CSV grande (em chunks).
* **Configuração de Leitura:**
    * `sep=';'`, `encoding='latin-1'`, `dtype=str`, `on_bad_lines='skip'`.
* **Ações:**
    * Criação de `CPF_PADRONIZADO`.
    * Renomeações úteis (nome, renda, CEP).

### 🔹 Fase 3 – Cruzamento (Merge)
* **Lógica:** Merge *inner* entre `Alunos.CPF_PADRONIZADO` × `CadÚnico.CPF_PADRONIZADO`.
* **Motor:** Executado em DuckDB.
* **Alertas de Log:** CPF ausente, coluna não encontrada ou resultado vazio.

### 🔹 Fase 4 – Pessoas Únicas
* **Derivação:** Aplicação de `drop_duplicates(subset=['cpf'])`.
* **Garantia:** Um CPF por linha no dataset final.

### 🔹 Fase 5 – Exportações
* **Geração Automática (XLSX):**
    * `MapeadosTJEprojeto_Matriculas_*`
    * `MapeadosTJEprojeto_PessoasUnicas_*`
* **Local de Destino:** `C:\TJE_uploads\outputs\{MESANO}`

### 🔹 Fase 6 – Finalização
* Sumário retornado ao frontend.
* Exibição dos caminhos dos arquivos gerados.
* Atualização de métricas e encerramento do processo.

---

## 2. Monitoramento Técnico

### Logs
* **Buffer:** Mantido em memória.
* **Persistência:** `_logs/app_runtime.log`.
* **Endpoint:** `GET /api/logs`.

### Métricas
* **Endpoint:** `GET /api/metrics`.
* **Indicadores:** * Alunos lidos
    * CadÚnico lido
    * Registros cruzados
    * Pessoas únicas
    * Progresso percentual por fase (%)

---

## 3. Frontend (index.html)
* **Painel Técnico:** Lateral fixa com *polling* a cada 2 segundos.
* **Interface:** Indicadores visuais por fase e spinner de processamento (CSS `lds-grid`).
* **Funcionalidades:**
    * Botões: Processar, Testar Conexão DB2, Listar Saídas.
    * Toggle de tema claro/escuro.
    * Guia "Evolução do Projeto" (Documentação embutida).

---

## 4. Conectividade DB2
* **Estratégia:** Múltiplos caminhos de `clidriver`, `DB2CODEPAGE=1208` e `os.add_dll_directory`.
* **Testes (Smoke Test):** Realizados na inicialização e sob demanda.
    * DNS / TCP / SELECT 1 / Queries reais.

---

## 5. Limitações e Próximos Passos

### ⚠️ Limitações Atuais (Pendentes)
* **Dicionário Final de Colunas:** Origem CPF, Nome Completo, Nome Cartão, Endereço, Responsável.
* **Bases:** Beneficiários Mapeados, Inativos, Acumulados.
* **Formatos:** CPFs únicos no formato Banrisul.
* **UI/UX:** API de status estruturado e gráficos de auditoria.

### 🚀 Próximos Passos Oficiais
1. Consolidar dicionário final.
2. Criar endpoint `/api/export/pessoas_unicas`.
3. Implementar histórico acumulado.
4. Estruturar `/api/status` e evoluir frontend (v24/v25).

---

## 6. Conclusão
Este documento é a **fonte oficial de verdade** do projeto. Ele registra decisões técnicas, erros enfrentados, soluções aplicadas e o estado atual.

> **Aviso:** Qualquer evolução futura não pode remover funcionalidades já consolidadas.

**Status Atual:**
* 🟡 **Estrutura sólida**
* 🟡 **Monitoramento funcional**
* 🔴 **Geração final de beneficiários ainda em evolução**
* 🟢 **Caminho técnico correto definido**


**********************************************
ATUALIZAÇÃO ABRIL 2026
**********************************************


# 📘 PROJETO DICMS – DOCUMENTAÇÃO TÉCNICA E EVOLUÇÃO --- 

## 1. 🎯 Objetivo do Projeto Gerar o layout oficial de carga de beneficiários do DICMS a partir da integração de múltiplas fontes de dados: - Base de alunos (DB2) - CadÚnico (CSV massivo) - Dados complementares (ISE) Aplicando regras de elegibilidade e definição correta do responsável familiar. ---

## 2. 🧱 Arquitetura do Pipeline O pipeline é dividido em 5 fases principais: --- ### 🔹 Fase 1 – Leitura DB2 - Extração dos alunos - Normalização de dados: - CPF - Nome - Nome da mãe - Volume médio: - ~249.000 registros --- ### 🔹 Fase 2 – Integração CadÚnico - Leitura de CSV massivo - Normalização: - CPF - Nome - Nome da mãe - Identificação de famílias (`COD_FAM`) --- 

### 🔹 Fase 3 – Chaves de Mapeamento Aplicação sequencial de regras: - CPF direto - Nome + mãe - Nome + data de nascimento - Combinações adicionais Resultado: - ~144.000 mapeados - ~104.000 não mapeados --- 

### 🔹 Fase 4 – Regras de Negócio #### ✔ Regra de Renda - Corte: ≤ 660 - Resultado: - ~94.000 elegíveis - ~50.000 removidos --- #### ✔ Regra de Município - Ajuste via base ISE - Sem impacto significativo de perda --- 

### 🔹 Fase 5 – Geração Layout DICMS Campos finais gerados: - `COD_CPF` - `NOME_BENEFICIARIO` - `DT_NASC_BENEFICIARIO` - `NOME_MAE_BENEFICIARIO` - `COD_MUNIC_IBGE` - Endereço completo - Telefones - Email - `VLR_RENDA_MEDIA` - `ORIGEM_CPF` - `CPF_RESP` Total final: - ~94.000 registros ---

## 3. 📊 Evolução do Projeto | Etapa | Resultado | |------|----------| | Script original | ~96.000 | | Primeira versão app | ~94.000 | | Diferença inicial | 5.193 | | Versão atual | ~2.071 | ✔ Redução significativa de divergência --- ## 4. ⚠️ Principais Problemas Encontrados --- 

### 4.1 Problemas de Encoding **Problema:** - Acentuação corrompida (Ã, ç, etc.) **Solução:** - Uso de `utf-8-sig` no CSV final --- ### 4.2 CPF Inconsistente Problemas identificados: - CPFs inválidos - CPFs incompletos - CPFs zerados - CPFs com formatação irregular Impacto: - Falha de match - Divergência com script - Registros descartados --- ### 4.3 Responsável Familiar (Problema Principal) **Regra correta:**



Problemas encontrados:

- Escolha incorreta do responsável
- Uso de CPF errado dentro da família
- Inconsistência entre script e aplicação

Impacto:

- Divergência em `ORIGEM_CPF`
- Diferença entre outputs

---

### 4.4 Estrutura do CadÚnico

Problemas:

- CSV muito grande
- Colunas inconsistentes
- Parsing instável

Impacto:

- Erros de leitura
- Perda de dados
- Dificuldade de processamento

---

### 4.5 Ordem do Pipeline

Problema recorrente:

- Aplicação de regras no momento errado

Impacto:

- Sobrescrita de CPF
- Inconsistência final
- Resultados divergentes

---

## 5. 📈 Situação Atual

### ✔ Métricas finais

- 94.193 CPFs em comum
- 2.071 apenas no script
- 396 apenas no app

---

### ✔ Qualidade atual

- ~97% aderência ao script
- Pipeline funcional
- Layout válido para uso

---

## 6. 🚧 Dívida Técnica Identificada

---

### 🔴 Alta

- Regra de responsável ainda sensível a dados inconsistentes
- Dependência forte da qualidade do CadÚnico

---

### 🟠 Média

- Código duplicado em partes do pipeline
- Falta de centralização das regras de CPF

---

### 🟡 Baixa

- Logs poderiam ser mais estruturados
- Ausência de testes automatizados

---

## 7. 🧠 Conclusão Técnica

O sistema atingiu um nível de maturidade operacional adequado:

- Processamento completo
- Resultado consistente
- Diferença residual explicável por dados

---

## 8. 🚀 Recomendações Futuras

- Criar validação automática de CPF
- Centralizar regra de responsável
- Criar testes comparativos automatizados
- Versionar regras de negócio
- Criar pipeline incremental

---

## 9. 📎 Artefatos Gerados

- `DICMS_Layout_Carga_Beneficiarios_*.csv`
- `Comparação Script vs Aplicação`
- `DEBUG_NAO_MAPEADOS`
- `DEBUG_DICMS`
- `ANALISE_FINAL`

---

## 10. 📌 Consideração Final

O principal limitador atual não é o código, mas:

> ✔ Qualidade e consistência dos dados de origem

---

## 11. 🧩 Status do Projeto

✔ Pipeline funcional  
✔ Resultado auditável  
✔ Diferença controlada  
✔ Pronto para uso assistido  

---

## 12. 🧠 Resumo Executivo

- Problema complexo de integração de dados
- Forte dependência de qualidade externa (CadÚnico)
- Evolução consistente ao longo do projeto
- Resultado final confiável dentro do cenário real

---



# 🧪 ROTEIRO DE TESTES – PROJETO DICMS

---

## 🎯 1. Objetivo

Garantir que o sistema (backend + frontend) está:

- Processando corretamente os dados
- Aplicando corretamente as regras de negócio
- Gerando o layout DICMS válido
- Mantendo consistência com o script oficial

---

## 🧱 2. Escopo

### Backend (app.py / pipeline)
- Leitura de dados
- Integração CadÚnico
- Aplicação de regras
- Geração de arquivos

### Frontend
- Upload / execução
- Visualização de métricas
- Download de arquivos

---

## 🧪 3. Tipos de Teste

| Tipo | Objetivo |
|------|--------|
| Unitário | Funções isoladas |
| Integração | Pipeline completo |
| Regressão | Comparação com script |
| Validação de dados | Qualidade dos dados |
| UI | Interface funcional |

---

## 🔹 4. Testes Backend

---

### ✅ 4.1 Teste de Leitura de Dados

**Objetivo:** Validar leitura do DB2 e CSV

**Entrada:**
- Base de alunos
- Arquivo CadÚnico

**Validação:**

assert len(df_alunos) > 0
assert "CPF" in df_alunos.columns

# ESTRUTURA DE TESTES

/project
│
├── app/
│   ├── app.py
│   ├── pipeline.py
│
├── tests/
│   ├── test_pipeline.py
│   ├── test_regressao.py
│   ├── test_dados.py
│
├── outputs/
│
├── requirements.txt
├── pytest.ini
├── .github/
│   └── workflows/
│       └── ci.yml

# ATUALIZAÇÃO 24/04/2026 #


# Projeto DICMS – Documentação Técnica e Executiva
**Data:** 23/04/2024  
**Status:** Pronto para uso assistido  
**Classificação:** Técnico / Executivo / Auditável

---

## 1. 🎯 Objetivo do Projeto

Gerar o **layout oficial de carga de beneficiários do DICMS**, a partir da integração de múltiplas fontes de dados, assegurando aderência às regras do **script oficial**, rastreabilidade e confiabilidade do resultado.

**Fontes integradas:**
- Base de alunos (DB2)
- CadÚnico (CSV massivo)
- Dados complementares (ISE)

---

## 2. 🧱 Arquitetura do Pipeline

O pipeline foi estruturado em **cinco fases principais**, permitindo evolução incremental, validação contínua e rastreabilidade dos resultados.

### 🔹 Fase 1 – Leitura DB2
- Extração de alunos
- Normalização de:
  - CPF
  - Nome
  - Nome da mãe
- Volume médio: ~249.000 registros

### 🔹 Fase 2 – Integração CadÚnico
- Leitura de CSV massivo
- Normalização de:
  - CPF
  - Nome
  - Nome da mãe
- Identificação de famílias (`COD_FAM`)

### 🔹 Fase 3 – Chaves de Mapeamento
Aplicação sequencial de regras de vinculação:
- CPF direto
- Nome + nome da mãe
- Nome + data de nascimento
- Combinações adicionais

**Resultado:**
- ~144.000 registros mapeados
- ~104.000 registros não mapeados inicialmente

### 🔹 Fase 4 – Regras de Negócio
**Regra de Renda**
- Corte: renda média ≤ 660
- Resultado:
  - ~94.000 elegíveis
  - ~50.000 removidos

**Regra de Município**
- Ajustes via base ISE
- Sem impacto relevante de perda

### 🔹 Fase 5 – Geração do Layout DICMS
Campos finais gerados:
- `COD_CPF`
- `NOME_BENEFICIARIO`
- `DT_NASC_BENEFICIARIO`
- `NOME_MAE_BENEFICIARIO`
- `COD_MUNIC_IBGE`
- Endereço completo
- Telefones
- E-mail
- `VLR_RENDA_MEDIA`
- `ORIGEM_CPF`
- `CPF_RESP`

**Total final:** ~94.000 registros

---

## 3. 📊 Evolução do Projeto

| Etapa                  | Quantidade |
|------------------------|------------|
| Script oficial         | ~96.000    |
| Primeira versão do app | ~94.000    |
| Diferença inicial      | 5.193      |
| Diferença atual        | ~2.071     |

✅ Observa-se **redução significativa das divergências**, indicando maturidade do pipeline e convergência com o script oficial.

---

## 4. ⚠️ Principais Problemas Encontrados

### 4.1 Problemas de Encoding
**Problema:**
- Acentuação corrompida (ex.: Ã, ç)

**Solução:**
- Padronização do CSV final em `UTF-8-SIG`

---

### 4.2 Inconsistência de CPF
**Problemas identificados:**
- CPFs inválidos
- CPFs incompletos
- CPFs zerados
- Formatação irregular

**Impacto:**
- Falhas de match
- Divergências em relação ao script
- Descarte de registros

---

### 4.3 Responsável Familiar (Problema Principal)
**Problemas encontrados:**
- Escolha incorreta do responsável familiar
- Uso de CPF errado dentro do núcleo familiar
- Diferenças de critério entre script e aplicação

**Impacto:**
- Divergências no campo `ORIGEM_CPF`
- Diferença entre outputs finais

---

### 4.4 Estrutura do CadÚnico
**Problemas:**
- CSV muito grande
- Colunas inconsistentes
- Parsing instável

**Impacto:**
- Erros de leitura
- Perda de dados
- Dificuldade de processamento

---

### 4.5 Ordem do Pipeline
**Problema recorrente:**
- Aplicação de regras em momento inadequado

**Impacto:**
- Sobrescrita de CPF
- Inconsistência final
- Resultados divergentes

---

## 5. 📈 Situação Atual

**Métricas finais:**
- 94.193 CPFs em comum
- 2.071 apenas no script
- 396 apenas na aplicação

**Qualidade atual:**
- ~97% de aderência ao script oficial
- Pipeline funcional
- Layout válido para uso

---

## 6. 🚧 Dívida Técnica Identificada

**🔴 Alta**
- Regra de responsável familiar ainda sensível a dados inconsistentes
- Forte dependência da qualidade do CadÚnico

**🟠 Média**
- Código duplicado em partes do pipeline
- Falta de centralização das regras de CPF

**🟡 Baixa**
- Logs pouco estruturados
- Ausência de testes automatizados

---

## 7. 🧠 Conclusão Técnica

O sistema atingiu **nível adequado de maturidade operacional**, apresentando:
- Processamento completo
- Resultado consistente
- Diferença residual explicável por dados

---

## 8. 🚀 Recomendações Futuras
- Criar validação automática de CPF
- Centralizar regra de responsável familiar
- Criar testes comparativos automatizados
- Versionar regras de negócio
- Evoluir para pipeline incremental

---

## 9. 📎 Artefatos Gerados
- `DICMS_Layout_Carga_Beneficiarios_*.csv`
- Comparação Script × Aplicação
- `DEBUG_NAO_MAPEADOS`
- `DEBUG_DICMS`
- `ANALISE_FINAL`

---

## 10. 📌 Consideração Final

O principal limitador atual do projeto **não é tecnológico**, mas sim a **qualidade e consistência dos dados de origem**.

---

## 11. 🧩 Status do Projeto
✔ Pipeline funcional  
✔ Resultado auditável  
✔ Diferença controlada  
✔ Pronto para uso assistido  

---

## 12. ✅ Resumo Executivo (Ultraexecutivo)

- **Resultado:** ~97% de aderência ao script oficial, com divergências significativamente reduzidas.
- **Esforço:** Concentrado em integração, mapeamento e regras de negócio, refletindo limitações dos dados externos.
- **Conclusão:** Sistema confiável, defensável e adequado ao uso institucional dentro do cenário real de dados.

---





