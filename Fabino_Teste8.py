import os
import io
import uuid
import zipfile
import calendar
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
from fastapi import (
    FastAPI,
    Request,
    HTTPException,
    UploadFile,
    File,
    Form,
    BackgroundTasks,
    Query,
)
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# ============================================================
# 1. CONFIGURAÇÃO DB2 (DLL + CONEXÃO)
# ============================================================

# Caminho da pasta que contém as DLLs do DB2 no Windows
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

# Importação crítica dos drivers DB2 + SQLAlchemy
try:
    import ibm_db
    import ibm_db_sa
    from sqlalchemy import create_engine, text
except Exception as e:
    print(f"[ERRO CRÍTICO] Falha ao importar DB2/ibm_db_sa: {e}")
    raise

# Credenciais reais do DB2 BIGSQL
dsn_database = "BIGSQL"
dsn_hostname = "bigsql.pro.intra.rs.gov.br"
dsn_port = "32051"
dsn_uid = "SVC-HADOOP-PDPSE"
dsn_pwd = "jFH49mdldg.123"

# String de conexão DB2 (SQLAlchemy)
DB2_CONN_STRING = (
    f"ibm_db_sa://{dsn_uid}:{dsn_pwd}@{dsn_hostname}:{dsn_port}/{dsn_database}"
)

# ============================================================
# 2. CONFIGURAÇÃO BÁSICA DO APP
# ============================================================

BASE_DIR = os.path.dirname(__file__)
INDEX_PATH = os.path.join(BASE_DIR, "index.html")
STATIC_DIR = os.path.join(BASE_DIR, "static")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Caminho do Parquet do CadÚnico (ajuste se necessário)
CADUNICO_PARQUET_PATH = r"C:\v14\dataframes\cadunico_base.parquet"

app = FastAPI(title="TJE Control • Cartões", version="25.1")

# Se existir pasta static, monta em /static
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# Logging "de verdade" (arquivo/console)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ============================================================
# 3. ESTRUTURAS EM MEMÓRIA PARA LOGS E MÉTRICAS
# ============================================================

GLOBAL_LOGS: List[str] = []
SESSION_LOGS: Dict[str, List[str]] = {}
SESSION_METRICS: Dict[str, Dict] = {}


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log_global(msg: str) -> None:
    line = f"{_now_str()} - {msg}"
    GLOBAL_LOGS.append(line)
    if len(GLOBAL_LOGS) > 5000:
        del GLOBAL_LOGS[:1000]
    logger.info(msg)


def log_session(session_id: str, msg: str) -> None:
    line = f"{_now_str()} - {msg} session={session_id}"
    SESSION_LOGS.setdefault(session_id, []).append(line)
    if len(SESSION_LOGS[session_id]) > 5000:
        del SESSION_LOGS[session_id][:1000]
    logger.info(msg)


def set_metrics(session_id: str, **kwargs) -> None:
    SESSION_METRICS.setdefault(session_id, {})
    SESSION_METRICS[session_id].update(kwargs)


# ============================================================
# 4. UTILITÁRIOS
# ============================================================

def gerar_session_id() -> str:
    return str(uuid.uuid4())


def mesano_para_refdate(mesano: str) -> str:
    """YYYYMM -> último dia do mês como 'YYYY-MM-DD'."""
    ano = int(mesano[:4])
    mes = int(mesano[4:6])
    ultimo_dia = calendar.monthrange(ano, mes)[1]
    return datetime(ano, mes, ultimo_dia).strftime("%Y-%m-%d")


# ============================================================
# 5. ACESSO AO DATALAKE / DB2
# ============================================================

def consulta_datalake(sql: str, usar_duckdb: bool = True) -> pd.DataFrame:
    """
    Executa consulta no DB2 BIGSQL usando ibm_db + SQLAlchemy.
    O parâmetro usar_duckdb é ignorado neste projeto (mantido apenas
    para compatibilidade com a interface).
    """
    try:
        engine = create_engine(DB2_CONN_STRING)
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn)
        return df
    except Exception as e:
        raise RuntimeError(f"[DB2 ERRO] Falha ao executar SQL: {e}\nSQL={sql[:300]}...")


def testar_conexao_db2() -> Dict:
    """
    Teste rápido usado pelo botão 'Testar DB2' na interface.
    """
    try:
        engine = create_engine(DB2_CONN_STRING)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 AS TESTE FROM SYSIBM.SYSDUMMY1"))
            rows = result.fetchall()

        return {
            "ok": True,
            "mensagem": "Conexão ao DB2 BIGSQL funcionando.",
            "rows": len(rows),
        }
    except Exception as e:
        return {
            "ok": False,
            "erro": str(e),
        }


def carregar_cadunico_filtrado(session_id: str, renda_maxima: float = 660.0) -> pd.DataFrame:
    log_session(session_id, f"[INFO] Fase 2: carregando CadÚnico (Parquet) de {CADUNICO_PARQUET_PATH}...")
    df = pd.read_parquet(CADUNICO_PARQUET_PATH)
    antes = len(df)
    log_session(session_id, f"[INFO] Aplicando filtro RENDA_MEDIA_FAM <= {renda_maxima} no CadÚnico.")
    df = df[df["RENDA_MEDIA_FAM"] <= renda_maxima]
    depois = len(df)
    log_session(session_id, f"[INFO] CadÚnico filtrado por renda: {antes} -> {depois} linhas.")
    return df


# ============================================================
# 6. QUERIES TJE (FASE 1)
# ============================================================

def montar_query_alunos(mesano: str) -> str:
    """
    Query base de alunos, agora usando REF_DATE derivada de MESANO,
    em vez de CURRENT DATE.
    """
    ref_date = mesano_para_refdate(mesano)
    return f"""
WITH PARAM AS (
    SELECT DATE('{ref_date}') AS REF_DATE
    FROM SYSIBM.SYSDUMMY1
)
SELECT DISTINCT IAT.NRO_INT_ALUNO_ESTADO
FROM PDP_SE_STG.ISE_ALUNO_TURMA IAT
INNER JOIN PDP_SE_STG.ISE_TURMA T 
    ON T.NRO_INT_TURMA = IAT.NRO_INT_TURMA
INNER JOIN PDP_SE_STG.ISE_CALENDARIO_ESTAB CE 
    ON CE.NRO_INT_CALEND_ESTAB = IAT.NRO_INT_CALEND_ESTAB
INNER JOIN PDP_SE_STG.ISE_ESTABELECIMENTO IE 
    ON IAT.IDT_ESTAB = IE.IDT_ESTAB 
   AND IE.IN_ESC_PRISIONAL <> 'S' 
   AND IE.NRO_INT_DESIGNACAO NOT IN (10, 489)
INNER JOIN PDP_SE_STG.ISE_ALUNO IA 
    ON IAT.NRO_INT_ALUNO_ESTADO = IA.NRO_INT_ALUNO_ESTADO
CROSS JOIN PARAM P
WHERE IAT.NRO_INT_SITUACAO IN (1, 8)
  AND P.REF_DATE BETWEEN CE.DT_INICIO_ATIV AND CE.DT_FIM_ATIV
  AND (
        (T.CD_TIPO_ENSINO IN ('I2', 'E2', 'R2')
         AND INTEGER((DAYS(P.REF_DATE) - DAYS(IA.DT_NASCIMENTO)) / 365.25) <= 21)
     OR (T.CD_TIPO_ENSINO IN ('S2', 'E9')
         AND INTEGER((DAYS(P.REF_DATE) - DAYS(IA.DT_NASCIMENTO)) / 365.25) <= 29)
  )
"""


def montar_query_documentos(ids: List[int]) -> str:
    """
    Query de documentos dos alunos, validada:
    - mesma tabela: PDP_SE_STG.ISE_ALUNO
    - relacionamento por NRO_INT_ALUNO_ESTADO (chave natural que você já usa)
    """
    # Remove duplicados e ordena só para manter um IN mais estável
    ids_unicos = sorted(set(int(x) for x in ids))
    ids_str = ",".join(str(x) for x in ids_unicos)

    return f"""
SELECT DISTINCT
       NRO_INT_ALUNO_ESTADO,
       NM_ALUNO_PURO,
       DT_NASCIMENTO,
       CPF,
       CPF_PAI,
       CPF_MAE,
       CPF_RESPONSAVEL,
       NM_MAE_PURO,
       NR_IDENT_MAE,
       NM_PAI_PURO,
       NR_IDENT_PAI,
       NR_DOC_MAE,
       NR_DOC_PAI 
FROM PDP_SE_STG.ISE_ALUNO
WHERE NRO_INT_ALUNO_ESTADO IN ({ids_str})
"""


def fase1_carregar_alunos(session_id: str, mesano: str, usar_duckdb: bool) -> Tuple[pd.DataFrame, pd.DataFrame]:
    set_metrics(session_id, fase=1, status="running")
    log_session(session_id, "Fase 1: lendo alunos do DB2 (Data Lake)...")

    sql_alunos = montar_query_alunos(mesano)
    try:
        df_alunos = consulta_datalake(sql_alunos, usar_duckdb=usar_duckdb)
    except Exception as e:
        log_session(session_id, f"[ERRO] Falha na consulta de alunos: {e}")
        set_metrics(session_id, status="error")
        return pd.DataFrame(), pd.DataFrame()

    log_session(
        session_id,
        f"Fase 1: df_alunos retornou {df_alunos.shape[0]} linhas."
    )

    if df_alunos.empty:
        log_session(session_id, "[ERRO] df_alunos veio vazio da consulta ao DB2.")
        set_metrics(session_id, status="error")
        return df_alunos, pd.DataFrame()

    # documentos (opcional, mas bem útil para cruzar por CPF)
    documentos = pd.DataFrame()
    try:
        ids = df_alunos["NRO_INT_ALUNO_ESTADO"].dropna().unique().tolist()
        if ids and len(ids) <= 5000:
            sql_docs = montar_query_documentos(ids)
            documentos = consulta_datalake(sql_docs, usar_duckdb=usar_duckdb)
            log_session(
                session_id,
                f"documentos_alunos retornou {documentos.shape[0]} linhas e {documentos.shape[1]} colunas."
            )
        else:
            log_session(session_id, "Pulando consulta de documentos (lista vazia ou grande demais).")
    except Exception as e:
        log_session(session_id, f"[WARN] Falha ao buscar documentos dos alunos: {e}")

    return df_alunos, documentos


# ============================================================
# 7. CRUZAMENTO COM CADÚNICO
# ============================================================

def cruzar_alunos_cadunico(
    session_id: str,
    df_alunos: pd.DataFrame,
    df_cad: pd.DataFrame,
    documentos: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    set_metrics(session_id, fase=4, status="running")

    if df_alunos.empty:
        log_session(session_id, "[ERRO] df_alunos vazio em cruzar_alunos_cadunico.")
        return pd.DataFrame()

    # Preferência: cruzar por CPF se documentos e CadÚnico tiverem CPF
    if documentos is not None and not documentos.empty and "CPF" in documentos.columns and "CPF" in df_cad.columns:
        log_session(session_id, "Cruzando por CPF usando documentos_alunos.")
        docs = documentos.copy()
        cad = df_cad.copy()
        docs["CPF"] = docs["CPF"].astype(str).str.strip()
        cad["CPF"] = cad["CPF"].astype(str).str.strip()
        mapeados = pd.merge(
            docs,
            cad,
            on="CPF",
            how="inner",
            suffixes=("_ALUNO", "_CAD"),
        )
    else:
        log_session(session_id, "Sem documentos válidos. Tentando cruzar por NRO_INT_ALUNO_ESTADO.")
        if "NRO_INT_ALUNO_ESTADO" not in df_cad.columns:
            log_session(session_id, "[ERRO] CadÚnico não possui NRO_INT_ALUNO_ESTADO para cruzar.")
            return pd.DataFrame()
        mapeados = pd.merge(
            df_alunos,
            df_cad,
            on="NRO_INT_ALUNO_ESTADO",
            how="inner",
        )

    log_session(session_id, f"Cruzamento gerou {mapeados.shape[0]} linhas de mapeados.")
    return mapeados


# ============================================================
# 8. GERAÇÃO DE ARQUIVOS
# ============================================================

def salvar_arquivos_mapeados(session_id: str, mesano: str, df_mapeados: pd.DataFrame) -> Dict[str, str]:
    set_metrics(session_id, fase=5, status="running")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = f"MapeadosTJEprojeto_{mesano}_{ts}"
    xlsx_path = os.path.join(OUTPUT_DIR, base_name + ".xlsx")
    csv_path = os.path.join(OUTPUT_DIR, base_name + ".csv")

    if df_mapeados.empty:
        log_session(session_id, "[WARN] DataFrame de mapeados vazio. Arquivos serão gerados sem linhas.")

    with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:
        df_mapeados.to_excel(writer, index=False, sheet_name="MAPEADOS")

    df_mapeados.to_csv(csv_path, index=False, sep=";")

    log_session(session_id, f"Arquivos gerados: {xlsx_path} e {csv_path}.")
    return {"xlsx": xlsx_path, "csv": csv_path}


def listar_outputs_por_mesano(mesano: str) -> List[Dict[str, str]]:
    files = []
    if not os.path.isdir(OUTPUT_DIR):
        return files
    for fname in os.listdir(OUTPUT_DIR):
        if mesano in fname:
            files.append(
                {
                    "name": fname,
                    "path": os.path.join(OUTPUT_DIR, fname),
                }
            )
    return files


# ============================================================
# 9. PIPELINE PRINCIPAL (BACKGROUND)
# ============================================================

def pipeline_tje(session_id: str, mesano: str, usar_duckdb: bool) -> None:
    try:
        log_global(f"Processamento iniciado. session={session_id}")
        log_session(session_id, f"Pedido recebido mesano={mesano} params={{\"usar_duckdb\": {usar_duckdb}}}")
        set_metrics(session_id, fase=1, status="running", mesano=mesano)

        # Fase 1: DB2
        df_alunos, documentos = fase1_carregar_alunos(session_id, mesano, usar_duckdb)
        if df_alunos.empty:
            log_session(session_id, "Abortando: df_alunos vazio.")
            set_metrics(session_id, status="error")
            return

        set_metrics(session_id, fase=2)
        # Fase 2: CadÚnico
        df_cad = carregar_cadunico_filtrado(session_id)

        set_metrics(session_id, fase=3)
        log_session(session_id, "Fase 3: pré-processamentos (se houver).")

        # Fase 4: cruzamento
        df_mapeados = cruzar_alunos_cadunico(session_id, df_alunos, df_cad, documentos)

        # Fase 5: geração de saídas
        arquivos = salvar_arquivos_mapeados(session_id, mesano, df_mapeados)

        # Fase 6: finalização
        set_metrics(
            session_id,
            fase=6,
            status="done",
            qtd_alunos=int(df_alunos.shape[0]),
            qtd_mapeados=int(df_mapeados.shape[0]),
            arquivos=arquivos,
        )
        log_session(session_id, "Processamento finalizado.")
        log_global(f"Processamento finalizado. session={session_id}")
    except Exception as e:
        log_session(session_id, f"[ERRO] Exceção no pipeline: {e}")
        set_metrics(session_id, status="error")


# ============================================================
# 10. ROTAS FRONT-END (INDEX)
# ============================================================

@app.get("/")
async def serve_index():
    if not os.path.exists(INDEX_PATH):
        raise HTTPException(status_code=500, detail="index.html não encontrado.")
    return FileResponse(INDEX_PATH, media_type="text/html")


@app.get("/index.html")
async def serve_index_html():
    return await serve_index()


# ============================================================
# 11. ENDPOINTS DE PROCESSAMENTO
# ============================================================

@app.post("/api/processar")
async def api_processar(request: Request, background_tasks: BackgroundTasks):
    """
    Espera um JSON como:
      { "mesano": "202507", "params": { "usar_duckdb": true } }
    conforme index.html.
    """
    body = await request.json()
    mesano = body.get("mesano")
    params = body.get("params", {}) or {}
    usar_duckdb = bool(params.get("usar_duckdb", True))

    if not mesano or len(mesano) != 6 or not str(mesano).isdigit():
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": "Parâmetro 'mesano' deve ser YYYYMM."},
        )

    session_id = gerar_session_id()
    SESSION_LOGS[session_id] = []
    SESSION_METRICS[session_id] = {"status": "pending", "fase": 0, "mesano": mesano}

    log_session(session_id, "Chamando pipeline em background.")
    background_tasks.add_task(pipeline_tje, session_id, mesano, usar_duckdb)

    return {"ok": True, "session_id": session_id}


@app.post("/api/testar_db2")
async def api_testar_db2():
    result = testar_conexao_db2()
    return result


# ============================================================
# 12. ENDPOINTS DE LOGS E MÉTRICAS
# ============================================================

@app.get("/api/logs/{session_id}")
async def api_logs_session(session_id: str):
    logs = SESSION_LOGS.get(session_id, [])
    return {"ok": True, "logs": logs}


@app.get("/api/logs/global")
async def api_logs_global():
    return {"ok": True, "logs": GLOBAL_LOGS}


@app.get("/api/metrics/{session_id}")
async def api_metrics(session_id: str):
    m = SESSION_METRICS.get(session_id)
    if not m:
        return {"ok": False, "error": "Sessão não encontrada."}
    return {"ok": True, "metrics": m}


# ============================================================
# 13. ENDPOINTS DE SAÍDAS E DOWNLOAD
# ============================================================

@app.get("/api/outputs/{mesano}")
async def api_outputs(mesano: str):
    if not mesano or len(mesano) != 6:
        return {"ok": False, "error": "mesano inválido"}
    files = listar_outputs_por_mesano(mesano)
    return {"ok": True, "files": files}


@app.get("/api/download")
async def api_download(path: str = Query(..., description="Caminho completo do arquivo")):
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Arquivo não encontrado.")
    return FileResponse(path, filename=os.path.basename(path))


@app.get("/api/download_zip")
async def api_download_zip(mesano: str = Query(..., description="MESANO para filtrar saídas")):
    files = listar_outputs_por_mesano(mesano)
    if not files:
        raise HTTPException(status_code=404, detail="Nenhum arquivo encontrado para esse MESANO.")

    mem = io.BytesIO()
    with zipfile.ZipFile(mem, mode="w", compression=zipfile.ZIP_DEFLATED) as z:
        for f in files:
            z.write(f["path"], arcname=f["name"])
    mem.seek(0)

    arquivo_nome = f"{mesano}_outputs.zip"
    return FileResponse(
        mem,
        media_type="application/zip",
        filename=arquivo_nome,
    )


# ============================================================
# 14. PREVIEW RÁPIDO DO CADÚNICO
# ============================================================

@app.post("/api/cadunico/fast_preview")
async def api_cadunico_fast_preview(
    file: UploadFile = File(...),
    preview_rows: int = Form(10),
):
    try:
        content = await file.read()
        buf = io.BytesIO(content)

        name = (file.filename or "").lower()

        if name.endswith(".parquet"):
            df = pd.read_parquet(buf)
        elif name.endswith(".csv"):
            df = pd.read_csv(buf, sep=";", engine="python")
        elif name.endswith(".xlsx") or name.endswith(".xls"):
            df = pd.read_excel(buf)
        else:
            return {"ok": False, "error": "Formato não suportado para preview (use CSV, XLSX ou Parquet)."}

        df_preview = df.head(preview_rows).copy()
        cols = list(df_preview.columns)
        rows = df_preview.to_dict(orient="records")

        return {
            "ok": True,
            "preview": {
                "columns": cols,
                "rows": rows,
            },
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}
