# =========================
# FASE 3 — MERGE ROBUSTO
# =========================
import re
import pandas as pd

def _normalize_cpf_series(s: pd.Series) -> pd.Series:
    """Remove tudo que não for dígito e padroniza vazio como NaN."""
    if s is None:
        return pd.Series(dtype="object")
    s = s.astype(str).str.replace(r"[^\d]", "", regex=True)
    s = s.where(s.str.len() > 0)  # vazio -> NaN
    return s

def ensure_cpf_padronizado(df: pd.DataFrame,
                           col_candidates=("cpf", "CPF", "CPF_ALUNO", "CPF_CADUNICO",
                                           "p.num_cpf_pessoa", "num_cpf_pessoa")) -> pd.DataFrame:
    """
    Garante coluna 'CPF_PADRONIZADO' no DataFrame.
    Tenta primeiro achar 'CPF_PADRONIZADO'; se não existir, procura uma coluna
    de CPF nas candidatas e normaliza.
    """
    df = df.copy()
    if "CPF_PADRONIZADO" in df.columns:
        df["CPF_PADRONIZADO"] = _normalize_cpf_series(df["CPF_PADRONIZADO"])
        return df

    # tenta achar uma coluna de CPF
    found = None
    for c in col_candidates:
        if c in df.columns:
            found = c
            break

    # procura por regex se necessário (qualquer coluna que tenha 'cpf' no nome)
    if found is None:
        regex_cols = [c for c in df.columns if re.search(r"cpf", c, flags=re.I)]
        if regex_cols:
            found = regex_cols[0]

    if found is None:
        raise ValueError("Nenhuma coluna de CPF encontrada no DataFrame para gerar CPF_PADRONIZADO.")

    df["CPF_PADRONIZADO"] = _normalize_cpf_series(df[found])
    return df

def fase3_merge_alunos_cad(df_alunos: pd.DataFrame,
                           df_cad: pd.DataFrame,
                           log_prefix: str = "Fase 3") -> dict:
    """
    Executa o merge (inner) entre alunos e cadunico por CPF_PADRONIZADO,
    com diagnósticos e amostras de CPFs faltantes. Retorna:
      {
        "merge": DataFrame,
        "pessoas_unicas": DataFrame,
        "metrics": {...}
      }
    """
    log(f"{log_prefix}: realizando merge (CPF_PADRONIZADO)...")

    # 1) Garantir CPF_PADRONIZADO nas duas bases
    try:
        df_alunos = ensure_cpf_padronizado(df_alunos, col_candidates=("cpf", "CPF_ALUNO", "CPF"))
    except Exception as e:
        raise ValueError(f"{log_prefix}: df_alunos sem coluna de CPF utilizável. Detalhes: {e}")

    try:
        df_cad = ensure_cpf_padronizado(df_cad, col_candidates=("CPF_CADUNICO", "p.num_cpf_pessoa", "CPF"))
    except Exception as e:
        raise ValueError(f"{log_prefix}: df_cad sem coluna de CPF utilizável. Detalhes: {e}")

    # 2) Diagnóstico pré-merge
    n_alunos = len(df_alunos)
    n_cad = len(df_cad)
    n_alunos_cpf = df_alunos["CPF_PADRONIZADO"].notna().sum()
    n_cad_cpf = df_cad["CPF_PADRONIZADO"].notna().sum()

    log(f"{log_prefix}: alunos={n_alunos} (com CPF={n_alunos_cpf}), cadunico={n_cad} (com CPF={n_cad_cpf})")

    if n_alunos_cpf == 0:
        raise ValueError(f"{log_prefix}: df_alunos sem CPFs válidos após normalização.")
    if n_cad_cpf == 0:
        raise ValueError(f"{log_prefix}: df_cad sem CPFs válidos após normalização.")

    # 3) Amostras de CPFs faltantes para auditoria (até 10 exemplos)
    amostra_alunos_sem_cpf = df_alunos[df_alunos["CPF_PADRONIZADO"].isna()].head(10)
    amostra_cad_sem_cpf = df_cad[df_cad["CPF_PADRONIZADO"].isna()].head(10)
    if len(amostra_alunos_sem_cpf):
        log(f"{log_prefix}: atenção — {len(df_alunos) - n_alunos_cpf} alunos sem CPF. Exemplo:")
        try:
            log(str(amostra_alunos_sem_cpf.iloc[:, :5]))
        except Exception:
            pass
    if len(amostra_cad_sem_cpf):
        log(f"{log_prefix}: atenção — {len(df_cad) - n_cad_cpf} cadunico sem CPF. Exemplo:")
        try:
            log(str(amostra_cad_sem_cpf.iloc[:, :5]))
        except Exception:
            pass

    # 4) Seleciona subconjuntos mínimos para merge (evita explosão de memória)
    keep_alunos = ["CPF_PADRONIZADO"]
    if "nm_aluno" in df_alunos.columns: keep_alunos.append("nm_aluno")
    if "idt_estab" in df_alunos.columns: keep_alunos.append("idt_estab")
    if "cpf" in df_alunos.columns: keep_alunos.append("cpf")  # manter cpf original se existir

    keep_cad = ["CPF_PADRONIZADO"]
    # Campos úteis mapeados anteriormente (ajuste conforme suas renomeações)
    for c in ("NOME_PESSOA_CADUNICO", "DATA_NASC_CADUNICO", "RENDA_MEDIA_FAM", "CEP",
              "p.num_cpf_pessoa", "NOME", "DTA_NASC", "VLR_RENDA_MEDIA"):
        if c in df_cad.columns and c not in keep_cad:
            keep_cad.append(c)

    dfA = df_alunos[keep_alunos].copy()
    dfB = df_cad[keep_cad].copy()

    # 5) MERGE
    merged = pd.merge(
        dfA, dfB,
        on="CPF_PADRONIZADO",
        how="inner",
        suffixes=("_ALUNO", "_CAD"),
        copy=False
    )
    n_merge = len(merged)
    log(f"{log_prefix}: merge OK → {n_merge} registros.")

    # 6) Pessoas Únicas por CPF (mantém a 1ª ocorrência)
    if "cpf" in merged.columns:
        pessoas_unicas = merged.drop_duplicates(subset=["cpf"], keep="first").copy()
    else:
        # Se a coluna 'cpf' original não existir, usa 'CPF_PADRONIZADO' como chave
        pessoas_unicas = merged.drop_duplicates(subset=["CPF_PADRONIZADO"], keep="first").copy()

    n_unicas = len(pessoas_unicas)
    log(f"{log_prefix}: pessoas únicas por CPF → {n_unicas}.")

    # 7) Métricas
    metrics = {
        "alunos_total": n_alunos,
        "alunos_com_cpf": n_alunos_cpf,
        "cad_total": n_cad,
        "cad_com_cpf": n_cad_cpf,
        "merge_linhas": n_merge,
        "pessoas_unicas": n_unicas,
    }

    return {
        "merge": merged,
        "pessoas_unicas": pessoas_unicas,
        "metrics": metrics
    }
