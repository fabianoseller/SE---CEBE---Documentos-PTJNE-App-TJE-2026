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

```text
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

### 🔹 Fase 1 – Leitura de Alunos (DB2)
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



