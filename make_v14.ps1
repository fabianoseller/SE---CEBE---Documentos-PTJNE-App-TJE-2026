# ================== make_v14.ps1 (versão simples) ==================
$ErrorActionPreference = "Stop"

function Write-UTF8($path, $content) {
  $dir = Split-Path $path -Parent
  if ($dir -and !(Test-Path $dir)) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }
  [System.IO.File]::WriteAllText($path, $content, [System.Text.UTF8Encoding]::new($false))
}

# -------- requirements.txt --------
$requirements = @"
fastapi==0.115.0
uvicorn[standard]==0.30.6
python-multipart==0.0.9
pandas==2.2.2
pyarrow==16.1.0
numpy==1.26.4
chardet==5.2.0
"@
Write-UTF8 ".\requirements.txt" $requirements

# -------- start.bat --------
$startBat = @"
@echo off
setlocal enabledelayedexpansion
title TJE Cartões v14 (porta 8000)
where python >nul 2>nul || (echo Python nao encontrado no PATH & pause & exit /b 1)
pip install -r requirements.txt
set PORT=8000
echo Iniciando na porta %PORT%...
python main.py
"@
Write-UTF8 ".\start.bat" $startBat

# -------- main.py --------
$mainPy = @"
from app.app import app

if __name__ == '__main__':
    import uvicorn
    uvicorn.run('app.app:app', host='0.0.0.0', port=8000, reload=True)
"@
Write-UTF8 ".\main.py" $mainPy

# -------- app/__init__.py --------
Write-UTF8 ".\app\__init__.py" "# package"

# -------- index.html --------
$indexHtml = @"
<!doctype html>
<html lang="pt-BR" data-theme="blue">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>TJE Control – Cartões v14</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin/>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap" rel="stylesheet"/>
<style>
:root{--radius:14px;--chip:#eef2ff;--chip-b:#c7d2fe;--ok:#16a34a;--warn:#f59e0b;--err:#ef4444}
[data-theme=blue]{--bg:linear-gradient(180deg,#eef2ff 0%,#f7faff 60%,#fff 100%);--panel:#ffffffcc;--b:#e6ebff;--fg:#0a1228;--muted:#4b5563;--accent:#2563eb}
[data-theme=light]{--bg:#f6f7fb;--panel:#fff;--b:#e8ecf5;--fg:#0f172a;--muted:#667085;--accent:#2563eb}
[data-theme=dark]{--bg:#0b1020;--panel:#0e1630cc;--b:#1b2c58;--fg:#eaf0ff;--muted:#a9b6d8;--accent:#7aa2ff}
*{box-sizing:border-box}body{margin:0;background:var(--bg);font-family:Inter,Segoe UI,system-ui;color:var(--fg)}
header{position:sticky;top:0;z-index:10;background:rgba(255,255,255,.6);backdrop-filter:blur(12px);border-bottom:1px solid var(--b);display:flex;gap:10px;align-items:center;padding:12px 16px}
.logo{width:36px;height:36px;border-radius:10px;background:linear-gradient(135deg,var(--accent),#60a5fa)}
.wrap{max-width:1200px;margin:22px auto;padding:0 16px}
.section{margin:22px 0}
.section h4{margin:0 0 8px;font-size:13px;letter-spacing:.06em;text-transform:uppercase;color:var(--muted)}
.grid{display:grid;gap:14px}.g-2{grid-template-columns:repeat(2,1fr)}.g-3{grid-template-columns:repeat(3,1fr)}@media(max-width:980px){.g-2,.g-3{grid-template-columns:1fr}}
.card{background:var(--panel);border:1px solid var(--b);border-radius:14px;padding:16px}
.title{margin:0 0 8px;display:flex;justify-content:space-between;align-items:center}
.badge{padding:4px 8px;border-radius:999px;border:1px solid var(--b);font-size:12px;background:var(--chip)}
.b-ok{background:#ecfdf5;border-color:#a7f3d0;color:#065f46}
.b-warn{background:#fffbeb;border-color:#fde68a;color:#92400e}
.muted{color:var(--muted);font-size:13px}
.btn{display:inline-flex;gap:8px;align-items:center;padding:10px 14px;border:1px solid var(--b);border-radius:12px;background:#fff;cursor:pointer}
input[type=file],input,select{width:100%;padding:10px 12px;border:1px solid var(--b);border-radius:10px;background:#fff}
.pre{white-space:pre-wrap;background:#fff;border:1px solid var(--b);border-radius:12px;padding:12px;font-size:12px;max-height:260px;overflow:auto}
.theme-toggle{margin-left:auto;display:flex;gap:8px}
.theme-toggle button{border:1px solid var(--b);background:var(--chip);border-radius:999px;padding:6px 10px;cursor:pointer}
</style>
</head>
<body>
<header>
  <div class="logo"></div>
  <h3 style="margin:0">TJE Control – Cartões</h3>
  <span class="badge">v14</span>
  <div class="theme-toggle">
    <button onclick="setTheme('light')">Claro</button>
    <button onclick="setTheme('blue')">Azul</button>
    <button onclick="setTheme('dark')">Escuro</button>
  </div>
</header>

<div class="wrap">
  <div class="section">
    <h4>Histórico e Propósito</h4>
    <div class="card">
      <div class="title"><strong>Programa Todo Jovem na Escola</strong><span class="badge b-ok">Ativo</span></div>
      <p class="muted">Trilha operacional do Cartão Cidadão: elegibilidade, geração de arquivos de pagamento, conferências (remessa/trailer), conciliação e auditoria; suporte a exceções e indicadores.</p>
    </div>
  </div>

  <div class="section">
    <h4>Documentação do Processo de Cartões</h4>
    <div class="grid g-3">
      <div class="card"><div class="title"><span><b>1) Finalidade e vínculo</b></span><span class="badge b-ok">Pronto</span></div><p class="muted">Cartão como meio de pagamento, emitido nominalmente (nomeCartao) ao beneficiário.</p></div>
      <div class="card"><div class="title"><span><b>2) Pré-requisitos</b></span><span class="badge b-warn" id="st2">Aguardando</span></div><p class="muted">Validação CPF/NIS/matrícula, frequência e elegibilidade; bloqueios resolvidos.</p></div>
      <div class="card"><div class="title"><span><b>3) Geração (DICMS/Carga)</b></span><span class="badge b-warn">Pendente</span></div><p class="muted">Produção dos arquivos para o banco e conferências.</p></div>
      <div class="card"><div class="title"><span><b>4) Emissão bancária</b></span><span class="badge b-warn">Pendente</span></div><p class="muted">Produção e logística do Banrisul.</p></div>
      <div class="card"><div class="title"><span><b>5) Entrega e ativação</b></span><span class="badge b-warn">Pendente</span></div><p class="muted">Entrega com conferência documental; ativação via telefone/app.</p></div>
      <div class="card"><div class="title"><span><b>6) Recargas mensais</b></span><span class="badge b-warn" id="st6">Aguardando</span></div><p class="muted">Envio das cargas, checagem de trailer e reconciliação.</p></div>
      <div class="card"><div class="title"><span><b>7) Suporte e exceções</b></span><span class="badge b-warn">Pendente</span></div><p class="muted">Bloqueios, 2ª via, reemissões.</p></div>
      <div class="card"><div class="title"><span><b>8) Auditoria e conformidade</b></span><span class="badge b-warn">Pendente</span></div><p class="muted">Logs e cruzamentos (Banrisul, DICMS, Conferência).</p></div>
      <div class="card"><div class="title"><span><b>9) Indicadores e prazos</b></span><span class="badge b-warn">Pendente</span></div><p class="muted">Emitidos/entregues/ativados e tempos médios.</p></div>
    </div>
  </div>

  <div class="section">
    <h4>Entradas – Preview rápido</h4>
    <div class="grid g-2">
      <div class="card">
        <div class="title"><b>CadÚnico (FAST – leitura parcial)</b></div>
        <label>CSV CadÚnico <input type="file" id="f_cad"/></label>
        <div style="margin-top:10px;display:flex;gap:8px;flex-wrap:wrap">
          <button class="btn" onclick="previewFast('/api/preview/cadunico_fast','f_cad','#out_cad',200,'')">Preview (200)</button>
        </div>
        <pre id="out_cad" class="pre"></pre>
      </div>
      <div class="card">
        <div class="title"><b>Frequência / Beneficiários</b></div>
        <label>CSV Frequência <input type="file" id="f_freq"/></label>
        <label style="margin-top:8px">CSV Beneficiários <input type="file" id="f_ben"/></label>
        <div style="margin-top:10px;display:flex;gap:8px;flex-wrap:wrap">
          <button class="btn" onclick="preview('/api/preview/frequencia','f_freq','#out_freq')">Preview Frequência</button>
          <button class="btn" onclick="preview('/api/preview/beneficiarios','f_ben','#out_ben')">Preview Beneficiários</button>
        </div>
        <pre id="out_freq" class="pre"></pre>
        <pre id="out_ben" class="pre"></pre>
      </div>
    </div>
  </div>

  <div class="section">
    <h4>Conferência / Trailer</h4>
    <div class="card">
      <label>CSV Remessa/Conferência <input type="file" id="f_conf"/></label>
      <div style="margin-top:10px"><button class="btn" onclick="preview('/api/preview/conferencia_trailer','f_conf','#out_conf')">Ler Trailer</button></div>
      <pre id="out_conf" class="pre"></pre>
    </div>
  </div>

  <div class="section">
    <h4>Dicionário CadÚnico</h4>
    <div class="card">
      <button class="btn" onclick="loadDict()">Ver dicionário</button>
      <pre id="out_dict" class="pre"></pre>
    </div>
  </div>
</div>

<script>
function setTheme(k){document.documentElement.setAttribute('data-theme',k);localStorage.setItem('tje_theme',k)}
const saved = localStorage.getItem('tje_theme'); if(saved) setTheme(saved);

async function preview(url, inputId, outSel){
  const f = document.getElementById(inputId).files[0]; const o = document.querySelector(outSel);
  if(!f){o.textContent='Escolha um arquivo.';return}
  const fd = new FormData(); fd.append('file', f); o.textContent='Carregando...';
  try { const r = await fetch(url,{method:'POST',body:fd}); const j = await r.json(); o.textContent = JSON.stringify(j,null,2); }
  catch(e){ o.textContent = 'Erro: '+e }
}
async function previewFast(url, inputId, outSel, preview_rows, ano_mes){
  const f = document.getElementById(inputId).files[0]; const o = document.querySelector(outSel);
  if(!f){o.textContent='Escolha um arquivo.';return}
  const fd = new FormData(); fd.append('file', f); fd.append('preview_rows', String(preview_rows||200)); if(ano_mes){fd.append('ano_mes', ano_mes)}
  o.textContent='Carregando...';
  try { const r = await fetch(url,{method:'POST',body:fd}); const j = await r.json(); o.textContent = JSON.stringify(j,null,2); }
  catch(e){ o.textContent = 'Erro: '+e }
}
async function loadDict(){ const o = document.querySelector('#out_dict'); o.textContent='Carregando...'; const r=await fetch('/api/dicionario/cadunico'); o.textContent = JSON.stringify(await r.json(),null,2) }
</script>
</body>
</html>
"@
Write-UTF8 ".\index.html" $indexHtml

# -------- app/app.py (backend) --------
$appPy = @"
import io, os, csv
from typing import Optional, List, Dict, Any
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
import pandas as pd
import numpy as np
import chardet

APP_TITLE = 'TJE Control – Cartões v14'
app = FastAPI(title=APP_TITLE, version='14.0')

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

DICT_COLUNAS = {
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
NEEDED_CAD = list(DICT_COLUNAS.keys())

def sniff_delimiter_and_encoding(sample: bytes) -> Dict[str, Any]:
    enc = chardet.detect(sample or b'').get('encoding') or 'utf-8'
    try_list = [enc, 'utf-8-sig', 'cp1252', 'latin1']
    delim = ';'
    try:
        dialect = csv.Sniffer().sniff(sample.decode(try_list[0], errors='ignore'), delimiters=[',',';','|','\t'])
        delim = dialect.delimiter
    except Exception:
        text = sample.decode(try_list[0], errors='ignore')
        counts = {d:text.count(d) for d in [',',';','|','\t']}
        delim = max(counts, key=counts.get) if counts else ';'
    return {'encoding_candidates': try_list, 'delimiter': delim}

def pd_read_preview(file_bytes: bytes, usecols: Optional[List[str]]=None, nrows: int=200):
    meta = sniff_delimiter_and_encoding(file_bytes[:200_000])
    last_err = None
    for enc in meta['encoding_candidates']:
        try:
            df = pd.read_csv(io.BytesIO(file_bytes), sep=meta['delimiter'], encoding=enc, nrows=nrows, dtype=str)
            if usecols:
                cols = [c for c in usecols if c in df.columns]
                if cols: df = df[cols]
            return {'ok': True, 'df': df, 'meta': {'encoding': enc, 'delimiter': meta['delimiter']}}
        except Exception as e:
            last_err = str(e)
            continue
    return {'ok': False, 'error': f"Falha ao ler CSV: {last_err or 'erro desconhecido'}"}

def df_to_preview_json(df: pd.DataFrame, meta: Dict[str, Any]):
    return {
        'ok': True,
        'rows': int(df.shape[0]),
        'cols': list(df.columns),
        'sample': df.head(5).to_dict(orient='records'),
        'meta': meta
    }

@app.get('/', response_class=HTMLResponse)
def home():
    return open('index.html', 'r', encoding='utf-8').read()

@app.get('/api/dicionario/cadunico')
def get_dict():
    return {'ok': True, 'dicionario': DICT_COLUNAS, 'campos_esperados': NEEDED_CAD}

@app.post('/api/preview/cadunico_fast')
async def cadunico_fast(file: UploadFile = File(...), preview_rows: int = Form(200), ano_mes: str = Form('')):
    raw = await file.read()
    res = pd_read_preview(raw, usecols=NEEDED_CAD, nrows=preview_rows)
    if not res['ok']:
        return JSONResponse(status_code=400, content={'ok': False, 'error': res.get('error','falha')})
    df = res['df']
    rename_map = {k:k for k in df.columns}
    for k in df.columns:
        if k in DICT_COLUNAS:
            rename_map[k] = DICT_COLUNAS[k]
    df = df.rename(columns=rename_map)
    out = df_to_preview_json(df, res['meta'])
    return out

@app.post('/api/preview/frequencia')
async def preview_freq(file: UploadFile = File(...)):
    raw = await file.read()
    res = pd_read_preview(raw, nrows=200)
    if not res['ok']:
        return JSONResponse(status_code=400, content={'ok': False, 'error': res.get('error','falha')})
    return df_to_preview_json(res['df'], res['meta'])

@app.post('/api/preview/beneficiarios')
async def preview_ben(file: UploadFile = File(...)):
    raw = await file.read()
    res = pd_read_preview(raw, nrows=200)
    if not res['ok']:
        return JSONResponse(status_code=400, content={'ok': False, 'error': res.get('error','falha')})
    return df_to_preview_json(res['df'], res['meta'])

@app.post('/api/preview/conferencia_trailer')
async def preview_trailer(file: UploadFile = File(...)):
    raw = await file.read()
    meta = sniff_delimiter_and_encoding(raw[:200_000])
    last_err = None
    for enc in meta['encoding_candidates']:
        try:
            text = raw.decode(enc, errors='ignore').splitlines()
            if not text: return {'ok': False, 'error': 'Arquivo vazio.'}
            tail = text[-2:] if len(text)>=2 else text[-1:]
            reader = csv.reader([tail[-1]], delimiter=meta['delimiter'])
            trailer = list(reader)[0]
            return {'ok': True, 'meta': {'encoding': enc, 'delimiter': meta['delimiter']}, 'trailer_raw': tail[-1], 'trailer_fields': trailer}
        except Exception as e:
            last_err = str(e)
            continue
    return JSONResponse(status_code=400, content={'ok': False, 'error': f'Falha trailer: {last_err or "desconhecido"}'})

@app.get('/api/health')
def health():
    return {'ok': True, 'app': APP_TITLE}
"@
Write-UTF8 ".\app\app.py" $appPy

# -------- gerar ZIP --------
$zipPath = Join-Path (Get-Location) "tje_cartoes_app_v14.zip"
if (Test-Path $zipPath) { Remove-Item $zipPath -Force }
Compress-Archive -Path .\* -DestinationPath $zipPath -Force
Write-Host "OK! ZIP gerado em: $zipPath"
# ================================================================
