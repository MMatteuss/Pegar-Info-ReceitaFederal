import pandas as pd
import mysql.connector
import os

# CONFIGURAÇÕES
base_dir = os.path.dirname(os.path.abspath(__file__))  # Pasta do script
pasta_arquivos = os.path.join(base_dir, 'arquivo receita deszipados')
nome_tabela = "simples"  # Altere para a tabela que deseja importar
arquivo_csv = f"{nome_tabela}.csv"  # Nome do arquivo conforme sua imagem
chunksize = 100_000

print(f"🔍 Procurando arquivo: {os.path.join(pasta_arquivos, arquivo_csv)}")

# Verifica se o arquivo existe
caminho_arquivo = os.path.join(pasta_arquivos, arquivo_csv)
if not os.path.exists(caminho_arquivo):
    print(f"🚨 ERRO: Arquivo não encontrado: {caminho_arquivo}")
    print("Arquivos disponíveis na pasta:")
    for f in os.listdir(pasta_arquivos):
        print(f" - {f}")
    exit()

# Conexão com MySQL
try:
    conexao = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Mateus2007",
        database="cnpj_brasil"
    )
    cursor = conexao.cursor()
    print("✅ Conexão com MySQL estabelecida")
except mysql.connector.Error as err:
    print(f"❌ Erro ao conectar ao MySQL: {err}")
    exit()

# Verifica se a tabela existe
try:
    cursor.execute(f"SHOW TABLES LIKE '{nome_tabela}'")
    if not cursor.fetchone():
        print(f"🚨 Tabela '{nome_tabela}' não existe no banco de dados")
        print("Execute primeiro o script que cria as tabelas")
        cursor.close()
        conexao.close()
        exit()
        
    # Obtém colunas da tabela
    cursor.execute(f"DESCRIBE {nome_tabela}")
    colunas = [col[0] for col in cursor.fetchall()]
    print(f"🔎 Colunas da tabela: {colunas}")
    
except Exception as e:
    print(f"❌ Erro ao verificar tabela: {e}")
    cursor.close()
    conexao.close()
    exit()

# Processa o arquivo CSV
total_importados = 0
print(f"\n📄 Processando arquivo: {arquivo_csv}")
try:
    for chunk in pd.read_csv(
        caminho_arquivo,
        encoding='ISO-8859-1',
        delimiter=';',
        header=None,
        names=colunas,
        low_memory=False,
        skiprows=1,
        chunksize=chunksize,
        on_bad_lines='warn'
    ):
        # Prepara dados para inserção
        registros = []
        for _, row in chunk.iterrows():
            registros.append(tuple(None if pd.isna(value) else value for value in row))
        
        # Insere em lote
        colunas_str = ', '.join([f'`{col}`' for col in colunas])
        placeholders = ', '.join(['%s'] * len(colunas))
        query = f"INSERT IGNORE INTO {nome_tabela} ({colunas_str}) VALUES ({placeholders})"
        
        try:
            cursor.executemany(query, registros)
            conexao.commit()
            registros_inseridos = len(registros)
            total_importados += registros_inseridos
            print(f"   ✔️ {registros_inseridos:,} registros inseridos (Total: {total_importados:,})")
        except mysql.connector.Error as err:
            print(f"   ❌ Erro no lote: {err}")
            conexao.rollback()
            
    print(f"✅ Arquivo concluído! Total: {total_importados:,} registros")
    
except Exception as e:
    print(f"❌ Erro durante o processamento: {e}")

# Finalização
cursor.close()
conexao.close()
print(f"\n🎉 Importação finalizada! Total geral: {total_importados:,} registros")