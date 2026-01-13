@echo off
setlocal enabledelayedexpansion
title TJE CartÃµes v14 (porta 8000)
where python >nul 2>nul || (echo Python nao encontrado no PATH & pause & exit /b 1)
pip install -r requirements.txt
set PORT=8000
echo Iniciando na porta %PORT%...
python main.py