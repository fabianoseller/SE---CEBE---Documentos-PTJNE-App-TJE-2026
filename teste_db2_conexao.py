# teste_db2_conexao.py
# Teste de conexão DB2 seguindo a sua estrutura (DLL -> imports -> testes)

import os, glob, socket, sys
from datetime import datetime

# ========================================================================
# 1. TRATAMENTO ESSENCIAL DE DLLs (DEVE VIR PRIMEIRO)
# ========================================================================
dll_path = r"C:/Users/fabiano-antunes/AppData/Local/Programs/Python/Python312/Lib/site-packages/clidriver/bin"
clidriver_home = os.path.dirname(dll_path)  # .../site-packages/clidriver

print("================================================================")
print("INÍCIO DO TESTE:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
print("================================================================\n")

print("--- Verificando e adicionando o caminho das DLLs do DB2 ---")
if os.name == 'nt' and os.path.isdir(dll_path):
    try:
        # Ajuda o Windows a localizar as DLLs do DB2
        os.add_dll_directory(dll_path)
        print("OK: os.add_dll_directory aplicado:", dll_path)
    except AttributeError:
        print("Aviso: sua versão do Python não suporta os.add_dll_directory.")
else:
    print("Aviso: dll_path não existe ou SO != Windows:", dll_path)

# Variáveis de ambiente úteis
os.environ.setdefault("DB2CODEPAGE", "1208")  # força UTF-8
os.environ["IBM_DB_HOME"] = clidriver_home
os.environ["PATH"] = dll_path + os.pathsep + os.environ.get("PATH", "")

print("\nVerificação clidriver/bin existe? ", os.path.isdir(dll_path), dll_path)
try:
    itens = glob.glob(os.path.join(dll_path, "*"))
    print("Qtd de arquivos em clidriver/bin:", len(itens))
except Exception as e:
    print("Falha ao listar clidriver/bin:", e)

print("\n-----------------------------------------------------------\n")

# ========================================================================
# 2. IMPORTS CRÍTICOS (APÓS AJUSTE DE DLL)
# ========================================================================
print("Fazendo imports de ibm_db e sqlalchemy...")

try:
    import ibm_db
    print("OK: import ibm_db")
except Exception as e:
    print("ERRO: import ibm_db ->", repr(e))
    print("Encerrando, pois sem ibm_db não há como testar.")
    sys.exit(1)

try:
    import ibm_db_dbi
    print("OK: import ibm_db_dbi")
except Exception as e:
    print("ERRO: import ibm_db_dbi ->", repr(e))

try:
    from sqlalchemy import create_engine, text
    print("OK: import sqlalchemy.create_engine")
except Exception as e:
    print("ERRO: import sqlalchemy ->", repr(e))

try:
    import ibm_db_sa  # registra o dialect ibm_db_sa no SQLAlchemy
    print("OK: import ibm_db_sa (dialect SQLAlchemy)")
except Exception as e:
    print("ERRO: import ibm_db_sa ->", repr(e))

print("\n-----------------------------------------------------------\n")

# ========================================================================
# 3. DIAGNÓSTICO DE REDE (DNS/Porta)
# ========================================================================
host = "bigsql.pro.intra.rs.gov.br"
port = 32051
print("Diagnóstico DNS/Porta...")

try:
    ip = socket.gethostbyname(host)
    print("OK: DNS ->", host, "->", ip)
except Exception as e:
    print("ERRO DNS:", repr(e))

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(3.5)
try:
    s.connect((host, port))
    print("OK: Conexão TCP em", host, "porta", port)
    s.close()
except Exception as e:
    print("ERRO TCP:", repr(e))

print("\n-----------------------------------------------------------\n")

# ========================================================================
# 4. TESTE DIRETO COM ibm_db
# ========================================================================
print("Teste de conexão direta com ibm_db...")

conn_str = (
    "DATABASE=BIGSQL;"
    "HOSTNAME=bigsql.pro.intra.rs.gov.br;"
    "PORT=32051;"
    "PROTOCOL=TCPIP;"
    "UID=SVC-HADOOP-PDPSE;"
    "PWD=jFH49mdldg.123;"
)

try:
    conn = ibm_db.connect(conn_str, "", "")
    print("OK: Conectado via ibm_db")
    stmt = ibm_db.exec_immediate(conn, "SELECT 1 FROM SYSIBM.SYSDUMMY1")
    row = ibm_db.fetch_tuple(stmt)
    print("OK: SELECT 1 (ibm_db) ->", row)
    ibm_db.close(conn)
except Exception as e:
    print("ERRO ibm_db.connect/SELECT:", repr(e))

print("\n-----------------------------------------------------------\n")

# ========================================================================
# 5. TESTE VIA SQLAlchemy (ibm_db_sa)
# ========================================================================
print("Teste via SQLAlchemy (ibm_db_sa)...")

dsn_sqla = "ibm_db_sa://SVC-HADOOP-PDPSE:jFH49mdldg.123@bigsql.pro.intra.rs.gov.br:32051/BIGSQL"

try:
    engine = create_engine(dsn_sqla, pool_pre_ping=True)
    with engine.connect() as conn:
        r = conn.execute(text("SELECT 1 FROM SYSIBM.SYSDUMMY1"))
        print("OK: SELECT 1 (SQLAlchemy) ->", list(r))
    engine.dispose()
except Exception as e:
    print("ERRO SQLAlchemy:", repr(e))

print("\n-----------------------------------------------------------\n")

# ========================================================================
# 6. RESUMO FINAL
# ========================================================================
print("RESUMO FINAL:")
print("- DLL path usado:", dll_path)
print("- DB2CODEPAGE:", os.environ.get("DB2CODEPAGE"))
print("- IBM_DB_HOME:", os.environ.get("IBM_DB_HOME"))
print("- PATH contém clidriver/bin no início? ", os.environ.get("PATH", "").startswith(dll_path))
print("\nTeste concluído.")
