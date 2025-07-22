import numpy as np
import re
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import mysql.connector
from tqdm import tqdm
import pandas as pd
import logging
import zipfile
import csv
import time
from datetime import datetime
import shutil

# Colocar conforme necessario (Só aqui que havera modificações)
nomeTabela = 'empresas'
DBsenha = 'SUA_SENHA_AQUI'
BDdataBase = 'SEU_BANCO_AQUI'

# Configurações
base_url = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj"
download_dir = "arquivo receita"

# Criar diretório se não existir
os.makedirs(download_dir, exist_ok=True)

# CONFIGURAÇÕES - BANCO DE DADOS
BASE_DIR = "dividir"
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": f"{DBsenha}",
    "database": f"{BDdataBase}"
}
CHUNKSIZE = 100_000
BATCH_SIZE = 10_000 

# Mapeamento de pastas para tabelas
FOLDER_TO_TABLE = {
    "10 PARTES EMPRESAS": "empresas",
    "10 PARTES ESTABELECIMENTOS": "estabelecimentos",
    "10 PARTES SOCIOS": "socios"
}

def get_latest_release_url(base_url):
    """Encontra a URL do release mais recente"""
    try:
        response = requests.get(base_url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        links = [a['href'] for a in soup.find_all('a') if a['href'].startswith('20') and a['href'].endswith('/')]
        
        if not links:
            raise ValueError("Nenhum link de release encontrado")
            
        # Ordena os links por data (mais recente primeiro)
        links.sort(reverse=True)
        latest_release = links[0]
        
        return urljoin(base_url + '/', latest_release)
        
    except Exception as e:
        print(f"Erro ao encontrar o release mais recente: {e}")
        return None

def get_file_links(release_url):
    """Obtém todos os links de arquivos ZIP no release"""
    try:
        response = requests.get(release_url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        file_links = []
        
        for a in soup.find_all('a'):
            href = a['href']
            if href.endswith('.zip'):
                file_links.append(urljoin(release_url, href))
                
        return file_links
        
    except Exception as e:
        print(f"Erro ao obter links dos arquivos: {e}")
        return []

def download_file(url, dest_dir):
    """Faz o download de um arquivo e salva no diretório de destino"""
    try:
        filename = os.path.basename(url)
        dest_path = os.path.join(dest_dir, filename)
        
        # Verificar se o arquivo já existe
        if os.path.exists(dest_path):
            print(f"Arquivo {filename} já existe, pulando...")
            return True
            
        print(f"Baixando {filename}...")
        
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(dest_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    
        print(f"{filename} baixado com sucesso!")
        return True
        
    except Exception as e:
        print(f"Erro ao baixar {url}: {e}")
        return False
def mainPegarDados():
    """Faz download apenas dos arquivos essenciais (Empresas, Estabelecimentos e Sócios)"""
    print("Iniciando processo de download dos arquivos essenciais...")
    
    # Obter URL do release mais recente
    latest_release = get_latest_release_url(base_url)
    if not latest_release:
        print("Não foi possível encontrar o release mais recente.")
        return
        
    print(f"Release mais recente encontrado: {latest_release}")
    
    # Obter links dos arquivos
    file_links = get_file_links(latest_release)
    if not file_links:
        print("Nenhum arquivo encontrado para download.")
        return
        
    # Filtrar apenas os arquivos essenciais
    arquivos_essenciais = {
        'Empresas': [],
        'Estabelecimentos': [],
        'Socios': []
    }
    
    for file_url in file_links:
        filename = os.path.basename(file_url)
        if filename.startswith('Empresas') and filename.endswith('.zip'):
            arquivos_essenciais['Empresas'].append(file_url)
        elif filename.startswith('Estabelecimentos') and filename.endswith('.zip'):
            arquivos_essenciais['Estabelecimentos'].append(file_url)
        elif filename.startswith('Socios') and filename.endswith('.zip'):
            arquivos_essenciais['Socios'].append(file_url)
    
    # Verificar se encontrou todos os arquivos essenciais
    for tipo, arquivos in arquivos_essenciais.items():
        if not arquivos:
            print(f"⚠️ Nenhum arquivo {tipo} encontrado para download!")
            return
    
    print("\n📁 Arquivos essenciais encontrados:")
    for tipo, arquivos in arquivos_essenciais.items():
        print(f" - {tipo}: {len(arquivos)} arquivos")
    
    # Criar diretório se não existir
    os.makedirs(download_dir, exist_ok=True)
    
    # Fazer download dos arquivos essenciais
    total_arquivos = sum(len(arquivos) for arquivos in arquivos_essenciais.values())
    success_count = 0
    
    print("\n⏳ Iniciando downloads...")
    
    for tipo, arquivos in arquivos_essenciais.items():
        print(f"\n🔽 Baixando arquivos de {tipo}:")
        for file_url in arquivos:
            filename = os.path.basename(file_url)
            print(f" - {filename}...", end=' ', flush=True)
            
            if download_file(file_url, download_dir):
                success_count += 1
                print("✅")
            else:
                print("❌ (Falha)")
    
    # Resumo final
    print(f"\n📊 Resultado:")
    print(f" - Total de arquivos essenciais: {total_arquivos}")
    print(f" - Baixados com sucesso: {success_count}")
    print(f" - Falhas: {total_arquivos - success_count}")
    
    if success_count == total_arquivos:
        print("\n🎉 Todos os arquivos essenciais foram baixados com sucesso!")
    else:
        print("\n⚠️ Alguns arquivos não foram baixados. Verifique os logs acima.")
    
    print(f"\nArquivos salvos em: {download_dir}")

def dezipar():
    """Função para descompactar e processar os arquivos baixados"""
    ########################## Configurações Padrão ##########################
    DEFAULT_CONFIG = {
        'base_url': 'http://200.152.38.155/CNPJ/dados_abertos_cnpj/',
        'csv_sep': ';',
        'csv_dec': ',',
        'csv_quote': '"',
        'csv_enc': 'cp1252',
        'export_format': 'csv',
        'dtypes': {
            'empresas': {
                'cnpj_basico': "str",
                'razao_social': "str",
                'natureza_juridica': "str",
                'qualificacao_do_responsavel': "str",
                'capital_social': "str",
                'porte_da_empresa': "str",
                'ente_federativo_resposavel': "str"
            },
            'estabelecimentos': {
                'cnpj_basico': "str",
                'cnpj_ordem': "str",
                'cnpj_dv': "str",
                'identificador_matriz_filial': "str",
                'nome_fantasia': "str",
                'situacao_cadastral': "str",
                'data_situacao_cadastral': "str",
                'motivo_situacao_cadastral': "str",
                'nome_da_cidade_no_exterior': "str",
                'pais': "str",
                'data_de_inicio_da_atividade': "str",
                'cnae_fiscal_principal': "str",
                'cnae_fiscal_secundaria': "str",
                'tipo_de_logradouro': "str",
                'logradouro': "str",
                'numero': "str",
                'complemento': "str",
                'bairro': "str",
                'cep': "str",
                'uf': "str",
                'municipio': "str",
                'situacao_cadastral': "str",
                'ddd1': "str",
                'telefone1': "str",
                'ddd2': "str",
                'telefone2': "str",
                'ddd_do_fax': "str",
                'fax': "str",
                'correio_eletronico': "str",
                'situacao_especial': "str",
                'data_da_situacao_especial': "str"
            },
            'socios': {
                'cnpj_basico': "str",
                'identificador_de_socio': "str",
                'nome_do_socio': "str",
                'cnpj_ou_cpf_do_socio': "str",
                'qualificacao_do_socio': "str",
                'data_de_entrada_sociedade': "str",
                'pais': "str",
                'representante_legal': "str",
                'nome_do_representante': "str",
                'qualificacao_do_representante_legal': "str",
                'faixa_etaria': "str"
            },
            'cnaes': {
                'cnpj_basico': "str",
                'cnae_fiscal_secundaria': "str"
            },
            'motivos': {
                'codigo_motivo': "str",
                'descricao_motivo': "str"
            },
            'municipios': {
                'codigo_municipio': "str",
                'nome_municipio': "str"
            },
            'natureza': {
                'codigo_natureza_juridica': "str",
                'descricao_natureza_juridica': "str"
            },
            'qualificacoes': {
                'codigo_qualificacao': "str",
                'descricao_qualificacao': "str"
            },
            'paises': {
                'codigo_pais': "str",
                'nome_pais': "str"
            },
            'simples': {
                'cnpj_basico': "str",
                'opcao_pelo_simples': "str",
                'data_opcao_simples': "str",
                'data_exclusao_simples': "str"
            }
        }
    }

    ########################## Inicialização ##########################
    data_incoming_foldername = "arquivo receita"
    data_outgoing_foldername = "arquivo receita deszipados"
    log_foldername = 'logs'
    log_filename = 'cnpj_merger.log'
    
    # Usa o diretório atual em vez de subir níveis
    path_script_dir = os.path.dirname(os.path.abspath(__file__))
    path_incoming = os.path.join(path_script_dir, data_incoming_foldername)
    path_outgoing = os.path.join(path_script_dir, data_outgoing_foldername)
    path_log = os.path.join(path_script_dir, log_foldername, log_filename)
    
    # Cria pastas necessárias
    os.makedirs(path_outgoing, exist_ok=True)
    os.makedirs(os.path.dirname(path_log), exist_ok=True)

    # Configuração do logging
    logging.basicConfig(filename=path_log, level=logging.INFO,
                       format='%(asctime)s | %(name)s | %(levelname)s | %(message)s')
    logging.info('Starting script')
    print('Starting script')

    # Usa configurações padrão (elimina a necessidade do config.yaml)
    config = DEFAULT_CONFIG
    csv_sep = config['csv_sep']
    csv_dec = config['csv_dec']
    csv_quote = config['csv_quote']
    csv_enc = config['csv_enc']
    export_format = config['export_format']
    dtypes = config['dtypes']

    ########################## Funções Internas ##########################
    def process_and_merge_files(file_params, dtype_dict, prefix):
        """Processa e mescla arquivos de ZIPs"""
        dfs = []
        columns = list(dtype_dict.keys())

        for file_list in file_params:
            zip_file_path = file_list[0]
            zip_filename = file_list[1]
            logging.info(f'Reading from ZIP: {zip_filename}')
            print(f'Reading from ZIP: {zip_filename}')

            try:
                with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                    zip_file_list = zip_ref.namelist()
                    if len(zip_file_list) == 1:
                        with zip_ref.open(zip_file_list[0]) as csvfile:
                            try:
                                df_buff = pd.read_csv(
                                    csvfile,
                                    sep=csv_sep,
                                    decimal=csv_dec,
                                    quotechar=csv_quote,
                                    dtype=dtype_dict,
                                    encoding='utf-8',
                                    header=None,
                                    on_bad_lines='skip'
                                )
                            except UnicodeDecodeError:
                                df_buff = pd.read_csv(
                                    csvfile,
                                    sep=csv_sep,
                                    decimal=csv_dec,
                                    quotechar=csv_quote,
                                    dtype=dtype_dict,
                                    encoding='latin1',
                                    header=None,
                                    on_bad_lines='skip'
                                )

                        logging.info(f'Appending: {zip_filename}')
                        print(f'Appending: {zip_filename}')
                        dfs.append(df_buff)
                    else:
                        logging.warning(f'ZIP file {zip_filename} contains more than one file.')
                        print(f'Warning: ZIP file {zip_filename} contains more than one file.')

            except Exception as e:
                logging.error(f"Error processing {zip_filename}: {e}")
                print(f"Error processing {zip_filename}: {e}")

        if dfs:
            try:
                merged_df = pd.concat(dfs, ignore_index=True)
                merged_df.columns = columns
                return merged_df
            except Exception as e:
                logging.error(f"Error merging DataFrames: {e}")
                print(f"Error merging DataFrames: {e}")
                return pd.DataFrame(columns=columns)
        return pd.DataFrame(columns=columns)

    def export_dataframe(df, export_path):
        """Exporta o DataFrame"""
        if export_path.endswith(".csv"):
            df.to_csv(export_path, index=False, sep=csv_sep, encoding='utf-8', quotechar=csv_quote)
        else:
            raise ValueError("Formato de exportação não suportado")

    ########################## Processamento Principal ##########################
    try:
        # Mapear arquivos ZIP
        file_params = {prefix: [] for prefix in dtypes.keys()}
        
        for root, _, files in os.walk(path_incoming):
            for filename in files:
                if filename.endswith(".zip"):
                    zip_file_path = os.path.join(root, filename)
                    file_with_no_ext = filename.split('.')[0].lower()
                    
                    for prefix in dtypes.keys():
                        if file_with_no_ext.startswith(prefix):
                            file_params[prefix].append([zip_file_path, filename, file_with_no_ext])

        # Processar e exportar cada tipo de arquivo
        for prefix, params in file_params.items():
            if params:  # Só processa se houver arquivos
                df_merged = process_and_merge_files(params, dtypes[prefix], prefix)
                outgoing_file_path = os.path.join(path_outgoing, f"{prefix}.{export_format}")
                export_dataframe(df_merged, outgoing_file_path)
                print(f"Exported: {outgoing_file_path}")

        print("\nProcesso de descompactação concluído com sucesso!")
        
    except Exception as e:
        logging.error(f"Erro fatal no processo de descompactação: {e}")
        print(f"Erro fatal: {e}")
        raise

def dividir_csv(arquivo_csv, partes=10):
    # Mapeamento de palavras-chave para pastas
    pastas = {
        'EMPRESA': '10 PARTES EMPRESAS',
        'EMPRESAS': '10 PARTES EMPRESAS',
        'ESTABELECIMENTO': '10 PARTES ESTABELECIMENTOS',
        'ESTABELECIMENTOS': '10 PARTES ESTABELECIMENTOS',
        'SOCIO': '10 PARTES SOCIOS',
        'SOCIOS': '10 PARTES SOCIOS'
    }

    if not os.path.exists(arquivo_csv):
        print(f"⚠ Arquivo NÃO encontrado: {arquivo_csv}")
        return

    # Identifica a subpasta certa
    nome_arquivo = os.path.basename(arquivo_csv).upper()
    pasta_destino = None
    for chave, pasta in pastas.items():
        if chave in nome_arquivo:
            pasta_destino = pasta
            break

    if not pasta_destino:
        print(f"⚠ Nenhuma pasta correspondente encontrada para: {nome_arquivo}")
        return

    # Pasta base do destino
    pasta_base_saida = 'dividir'
    caminho_pasta = os.path.join(pasta_base_saida, pasta_destino)

    os.makedirs(caminho_pasta, exist_ok=True)

    # Conta total de linhas
    with open(arquivo_csv, 'r', encoding='latin1') as f:
        total_linhas = sum(1 for _ in f)

    print(f"\n➡ Dividindo '{nome_arquivo}' ({total_linhas} linhas) para a pasta '{pasta_destino}'...")

    linhas_por_parte = total_linhas // partes + 1

    with open(arquivo_csv, 'r', encoding='latin1') as f:
        leitor = csv.reader(f, delimiter=';')
        cabecalho = next(leitor)

        for i in range(partes):
            numero_arquivo = i + 1
            nome_saida = f"parte_{numero_arquivo}.csv"
            caminho_saida = os.path.join(caminho_pasta, nome_saida)

            while os.path.exists(caminho_saida):
                numero_arquivo += 1
                nome_saida = f"parte_{numero_arquivo}.csv"
                caminho_saida = os.path.join(caminho_pasta, nome_saida)

            with open(caminho_saida, 'w', newline='', encoding='latin1') as out:
                escritor = csv.writer(out, delimiter=';')
                escritor.writerow(cabecalho)

                linhas_escritas = 0
                while linhas_escritas < linhas_por_parte:
                    try:
                        linha = next(leitor)
                        escritor.writerow(linha)
                        linhas_escritas += 1
                    except StopIteration:
                        break

            print(f"✅ Criado: {caminho_saida} ({linhas_escritas} linhas)")

    print(f"✔ Arquivo '{nome_arquivo}' concluído!\n")

def processar_arquivos_deszipados():
    """Processa todos os arquivos CSV descompactados"""
    pasta_base = r"arquivo receita deszipados"

    arquivos = [f for f in os.listdir(pasta_base) if f.lower().endswith('.csv')]

    if not arquivos:
        print(f"⚠ Nenhum arquivo CSV encontrado em {pasta_base}")
    else:
        print(f"📂 Encontrados {len(arquivos)} arquivo(s) na pasta '{pasta_base}':")
        for nome in arquivos:
            print(" -", nome)

        for arquivo in arquivos:
            arquivo_completo = os.path.join(pasta_base, arquivo)
            dividir_csv(arquivo_completo)

        print("🎉 Todos os arquivos foram processados!")

def get_db_connection():
    """Estabelece conexão com o banco de dados"""
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except mysql.connector.Error as err:
        print(f"🚨 Erro ao conectar ao MySQL: {err}")
        return None

def get_table_columns(conn, table_name):
    """Obtém as colunas de uma tabela"""
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"DESCRIBE {table_name}")
            return [col[0] for col in cursor.fetchall()]
    except Exception as e:
        print(f"🚨 Erro ao obter colunas da tabela '{table_name}': {e}")
        return None

def process_folder(folder_path, table_name, conn):
    """Processa todos os arquivos CSV em uma pasta e importa para a tabela correspondente"""
    if not os.path.exists(folder_path):
        print(f"📌 Pasta não encontrada: {folder_path}")
        return 0
    
    file_paths = sorted([
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.startswith("parte_") and f.endswith(".csv")
    ])
    
    if not file_paths:
        print(f"📌 Nenhum arquivo CSV encontrado na pasta: {folder_path}")
        return 0
    
    colunas = get_table_columns(conn, table_name)
    if not colunas:
        return 0
    
    total_imported = 0
    cursor = conn.cursor()
    
    print(f"\n📂 Processando tabela: {table_name.upper()}")
    
    for file_path in tqdm(file_paths, desc="Arquivos", unit="file"):
        try:
            file_size = os.path.getsize(file_path) / (1024 * 1024)  # Tamanho em MB
            print(f"\n📄 Arquivo: {os.path.basename(file_path)} ({file_size:.2f} MB)")
            
            for chunk in pd.read_csv(
                file_path,
                encoding='ISO-8859-1',
                delimiter=';',
                header=None,
                names=colunas,
                low_memory=False,
                skiprows=1,
                chunksize=CHUNKSIZE,
                on_bad_lines='warn'
            ):
                # Pré-processamento dos dados
                chunk = chunk.where(pd.notnull(chunk), None)
                records = [tuple(None if pd.isna(value) else value for value in row) 
                        for _, row in chunk.iterrows()]
                
                # Inserção em lotes
                for i in range(0, len(records), BATCH_SIZE):
                    batch = records[i:i+BATCH_SIZE]
                    colunas_str = ', '.join([f'`{col}`' for col in colunas])
                    valores = ', '.join(['%s'] * len(colunas))
                    query = f"INSERT IGNORE INTO {table_name} ({colunas_str}) VALUES ({valores})"
                    
                    try:
                        cursor.executemany(query, batch)
                        conn.commit()
                        total_imported += len(batch)
                    except mysql.connector.Error as err:
                        print(f"⚠️ Erro ao inserir lote: {err}")
                        conn.rollback()
                
                print(f"   ✅ {len(records):,} registros processados | Total: {total_imported:,}")
                
        except Exception as e:
            print(f"🚨 Erro ao processar o arquivo {file_path}: {e}")
            continue
    
    cursor.close()
    return total_imported

def mainJogarNoBanco():
    start_time = time.time()
    conn = get_db_connection()
    if not conn:
        return
    
    total_global = 0
    
    try:
        for folder, table in FOLDER_TO_TABLE.items():
            # Corrigindo o caminho da pasta
            folder_path = os.path.join(BASE_DIR, folder)
            if not os.path.exists(folder_path):
                print(f"⚠️ Pasta não encontrada: {folder_path}")
                continue
                
            imported = process_folder(folder_path, table, conn)
            total_global += imported
            print(f"\n✔️ {table.upper()}: {imported:,} registros importados\n")
            
    finally:
        conn.close()
        elapsed = time.time() - start_time
        print(f"\n🎉 Importação concluída! Total: {total_global:,} registros")
        print(f"⏱️ Tempo total: {elapsed:.2f} segundos")
        
def recreate_database_tables():
    """Reinicia o banco de dados, dropando e recriando todas as tabelas"""
    conn = get_db_connection()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        print("\n🔧 Recriando estrutura do banco de dados...")
        
        # Lista de tabelas para dropar
        tables_to_drop = [
            'socios', 'estabelecimentos', 'empresas'
        ]
        
        # Dropar tabelas
        for table in tables_to_drop:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
                print(f"✅ Tabela {table} dropada com sucesso")
            except mysql.connector.Error as err:
                print(f"⚠️ Erro ao dropar tabela {table}: {err}")
        
        # Criar database se não existir
        cursor.execute("CREATE DATABASE IF NOT EXISTS cnpj_brasil")
        cursor.execute("USE cnpj_brasil")
        
        # Criar tabelas com sintaxe corrigida (parêntese adicionado)
        cursor.execute("""
        CREATE TABLE empresas (
            cnpj_basico VARCHAR(8),
            razao_social VARCHAR(150),
            natureza_juridica INT,
            qualificacao_do_responsavel INT,
            capital_social DECIMAL(20,2),
            porte_da_empresa INT,
            ente_federativo_responsavel VARCHAR(100)
        )""")  # Parêntese de fechamento adicionado aqui
        
        cursor.execute("""
        CREATE TABLE estabelecimentos (
            cnpj_basico VARCHAR(8),
            cnpj_ordem VARCHAR(4),
            cnpj_dv VARCHAR(2),
            identificador_matriz_filial INT,
            nome_fantasia VARCHAR(55),
            situacao_cadastral INT,
            data_situacao_cadastral DATE,
            motivo_situacao_cadastral INT,
            nome_da_cidade_no_exterior VARCHAR(55),
            pais VARCHAR(3),
            data_de_inicio_da_atividade DATE,
            cnae_fiscal_principal VARCHAR(7),
            cnae_fiscal_secundaria TEXT,
            tipo_de_logradouro VARCHAR(20),
            logradouro VARCHAR(60),
            numero VARCHAR(6),
            complemento VARCHAR(156),
            bairro VARCHAR(50),
            cep VARCHAR(8),
            uf VARCHAR(2),
            municipio INT,
            ddd1 VARCHAR(4),
            telefone1 VARCHAR(9),
            ddd2 VARCHAR(4),
            telefone2 VARCHAR(9),
            ddd_do_fax VARCHAR(4),
            fax VARCHAR(9),
            correio_eletronico VARCHAR(115),
            situacao_especial VARCHAR(23),
            data_da_situacao_especial DATE
        )""")
        
        cursor.execute("""
        CREATE TABLE socios (
            cnpj_basico VARCHAR(8),
            identificador_de_socio INT,
            nome_do_socio VARCHAR(150),
            cnpj_ou_cpf_do_socio VARCHAR(14),
            qualificacao_do_socio INT,
            data_de_entrada_sociedade DATE,
            pais VARCHAR(3),
            representante_legal VARCHAR(11),
            nome_do_representante VARCHAR(60),
            qualificacao_do_representante_legal INT,
            faixa_etaria INT
        )""")
        
        conn.commit()
        print("✅ Todas as tabelas foram recriadas com sucesso!")
        return True
        
    except mysql.connector.Error as err:
        print(f"🚨 Erro ao recriar tabelas: {err}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

def mainJogarNoBanco():
    start_time = time.time()
    conn = get_db_connection()
    if not conn:
        return
    
    total_global = 0
    
    try:
        for folder, table in FOLDER_TO_TABLE.items():
            # Caminho completo corrigido
            folder_path = os.path.join(BASE_DIR, folder)
            
            # Verificação mais robusta da pasta
            if not os.path.exists(folder_path):
                print(f"🚨 ERRO: Pasta não encontrada: {folder_path}")
                print("Verifique se a estrutura de pastas está correta:")
                print(f"Deve existir: {BASE_DIR}/{folder}/ com arquivos parte_1.csv a parte_10.csv")
                continue
                
            # Verifica se há arquivos CSV na pasta
            csv_files = [f for f in os.listdir(folder_path) if f.startswith('parte_') and f.endswith('.csv')]
            if not csv_files:
                print(f"🚨 ERRO: Nenhum arquivo CSV encontrado em {folder_path}")
                continue
                
            print(f"\n📂 Processando pasta: {folder_path}")
            print(f"📄 Arquivos encontrados: {len(csv_files)}")
            
            imported = process_folder(folder_path, table, conn)
            total_global += imported
            print(f"✔️ {table.upper()}: {imported:,} registros importados")
            
    except Exception as e:
        print(f"🚨 Erro durante a importação: {e}")
    finally:
        conn.close()
        elapsed = time.time() - start_time
        print(f"\n🎉 Importação concluída! Total: {total_global:,} registros")
        print(f"⏱️ Tempo total: {elapsed:.2f} segundos")

def create_and_export_empresas():
    """Cria tabela simplificada apenas com os campos essenciais"""
    conn = get_db_connection()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        print(f"\n🔧 Criando tabela simplificada {nomeTabela} com dados essenciais...")
        
        # 1. Desativa o ONLY_FULL_GROUP_BY temporariamente
        cursor.execute("SET SESSION sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))")
        
        # 2. Cria tabela temporária
        cursor.execute(f"DROP TABLE IF EXISTS {nomeTabela}")
        
        create_query = f"""
        CREATE TABLE {nomeTabela} AS
        SELECT
            CONCAT(e.cnpj_basico, LPAD(est.cnpj_ordem, 4, '0'), LPAD(est.cnpj_dv, 2, '0')) AS cnpj_completo,
            e.razao_social,
            CONCAT(
                IFNULL(CONCAT(est.tipo_de_logradouro, ' '), ''),
                IFNULL(CONCAT(est.logradouro, ', '), ''),
                IFNULL(CONCAT(est.numero, ', '), ''),
                IFNULL(CONCAT(est.complemento, ', '), ''),
                IFNULL(CONCAT(est.bairro, ', '), ''),
                IFNULL(CONCAT(mun.nome_municipio, ', '), ''),
                IFNULL(CONCAT(est.uf, ', '), ''),
                IFNULL(est.cep, '')
            ) AS endereco_completo,
            est.correio_eletronico AS email,
            YEAR(est.data_de_inicio_da_atividade) AS ano_abertura,
            est.cnae_fiscal_principal AS cnae_principal,
            nat.descricao_natureza_juridica AS natureza_juridica,
            CONCAT(
                IFNULL(CONCAT(est.ddd1, est.telefone1), ''),
                IFNULL(CONCAT(';', est.ddd2, est.telefone2), ''),
                IFNULL(CONCAT(';', est.ddd_do_fax, est.fax), '')
            ) AS telefones
        FROM empresas e
        INNER JOIN estabelecimentos est ON e.cnpj_basico = est.cnpj_basico
        LEFT JOIN municipios mun ON est.municipio = mun.codigo_municipio
        LEFT JOIN natureza nat ON e.natureza_juridica = nat.codigo_natureza_juridica
        WHERE est.situacao_cadastral = 2  -- Apenas empresas ativas
        AND est.uf IN ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE')  -- Apenas estados do Nordeste
        AND est.data_de_inicio_da_atividade BETWEEN '1955-01-01' AND '2025-12-31'
        GROUP BY 
            e.cnpj_basico,
            est.cnpj_ordem,
            est.cnpj_dv,
            e.razao_social,
            est.tipo_de_logradouro,
            est.logradouro,
            est.numero,
            est.complemento,
            est.bairro,
            mun.nome_municipio,
            est.uf,
            est.cep,
            est.correio_eletronico,
            est.data_de_inicio_da_atividade,
            est.cnae_fiscal_principal,
            nat.descricao_natureza_juridica,
            est.ddd1,
            est.telefone1,
            est.ddd2,
            est.telefone2,
            est.ddd_do_fax,
            est.fax
        """
        
        cursor.execute(create_query)
        conn.commit()
        print(f"✅ Tabela {nomeTabela} criada com sucesso!")
        
        # 3. Reativa o ONLY_FULL_GROUP_BY
        cursor.execute("SET SESSION sql_mode=(SELECT CONCAT(@@sql_mode,',ONLY_FULL_GROUP_BY'))")
        
        # 4. Exporta para CSV
        print("\n📤 Exportando dados para CSV...")
        
        export_dir = "export"
        os.makedirs(export_dir, exist_ok=True)
        csv_path = os.path.join(export_dir, f"{nomeTabela}.csv")
        
        # Exporta em chunks para evitar estouro de memória
        chunk_size = 50000
        offset = 0
        total_rows = 0
        
        # Primeiro escreve o cabeçalho
        with open(csv_path, 'w', encoding='utf-8', newline='') as f:
            f.write("CNPJ;RAZAO_SOCIAL;ENDERECO;EMAIL;ANO_ABERTURA;CNAE_PRINCIPAL;NATUREZA_JURIDICA;TELEFONES\n")
        
        # Exporta os dados em partes
        while True:
            cursor.execute(f"""
                SELECT 
                    cnpj_completo,
                    razao_social,
                    endereco_completo,
                    email,
                    ano_abertura,
                    cnae_principal,
                    natureza_juridica,
                    telefones
                FROM {nomeTabela}
                LIMIT {chunk_size} OFFSET {offset}
            """)
            chunk = cursor.fetchall()
            
            if not chunk:
                break
                
            # Escreve no arquivo CSV
            with open(csv_path, 'a', encoding='utf-8', newline='') as f:
                for row in chunk:
                    # Formata a linha corretamente para CSV
                    formatted_row = [
                        str(row[0]) if row[0] else '',  # CNPJ
                        str(row[1]) if row[1] else '',  # Razão Social
                        str(row[2]) if row[2] else '',  # Endereço
                        str(row[3]) if row[3] else '',  # Email
                        str(row[4]) if row[4] else '',  # Ano de abertura
                        str(row[5]) if row[5] else '',  # CNAE Principal (código)
                        str(row[6]) if row[6] else '',  # Natureza Jurídica
                        str(row[7]) if row[7] else ''   # Telefones
                    ]
                    line = ';'.join(formatted_row)
                    f.write(line + '\n')
            
            offset += chunk_size
            total_rows += len(chunk)
            print(f"  ✅ Exportados {total_rows:,} registros...")
        
        print(f"\n🎉 Exportação concluída! Total: {total_rows:,} registros")
        print(f"📁 Arquivo salvo em: {csv_path}")
        
        return True
        
    except mysql.connector.Error as err:
        print(f"🚨 Erro ao criar tabela simplificada: {err}")
        conn.rollback()
        return False
    except Exception as e:
        print(f"🚨 Erro inesperado: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def apagarDentroPasta(caminhoPasta):
    """
    Apaga todo o conteúdo de uma pasta específica, mantendo a pasta raiz.
    """
    try:
        # Verifica se o caminho existe e é uma pasta
        if not os.path.exists(caminhoPasta):
            print(f"⚠️ A pasta {caminhoPasta} não existe.")
            return False
        
        if not os.path.isdir(caminhoPasta):
            print(f"⚠️ O caminho {caminhoPasta} não é uma pasta.")
            return False
        
        print(f"\n🧹 Limpando pasta: {caminhoPasta}")
        
        # Lista todo o conteúdo da pasta
        for nome in os.listdir(caminhoPasta):
            caminho_completo = os.path.join(caminhoPasta, nome)
            
            try:
                if os.path.isfile(caminho_completo) or os.path.islink(caminho_completo):
                    os.unlink(caminho_completo)  # Remove arquivos e links simbólicos
                    print(f"🗑️ Arquivo removido: {nome}")
                elif os.path.isdir(caminho_completo):
                    shutil.rmtree(caminho_completo)  # Remove subpastas e seu conteúdo
                    print(f"🗑️ Pasta removida: {nome}")
            except Exception as e:
                print(f"⚠️ Falha ao deletar {caminho_completo}. Razão: {e}")
        
        print(f"✅ Pasta limpa com sucesso: {caminhoPasta}")
        return True
    
    except Exception as e:
        print(f"🚨 Erro ao limpar pasta {caminhoPasta}: {e}")
        return False
    
def apagar_arquivos_especificos():
    """Apaga arquivos CSV específicos dentro da pasta 'arquivo receita deszipados'"""
    pasta = "arquivo receita deszipados"
    arquivos_para_apagar = [
        "empresas.csv",
        "estabelecimentos.csv",
        "socios.csv"
    ]

    for arquivo in arquivos_para_apagar:
        caminho = os.path.join(pasta, arquivo)
        if os.path.exists(caminho):
            os.remove(caminho)
            print(f"🗑️ Apagado: {caminho}")
        else:
            print(f"⚠️ Arquivo não encontrado: {caminho}")

    print("✔️ Exclusão concluída.")

def apagarDentroPasta10Para():
    """
    Limpa o conteúdo das pastas de dados divididos
    """
    pastas = [
        os.path.join('dividir', '10 PARTES EMPRESAS'),
        os.path.join('dividir', '10 PARTES ESTABELECIMENTOS'), 
        os.path.join('dividir', '10 PARTES SOCIOS')
    ]
    
    for pasta in pastas:
        apagarDentroPasta(pasta)

def validar_cnpj(cnpj):
    """Valida CNPJ usando dígitos verificadores"""
    cnpj = re.sub(r'[^0-9]', '', str(cnpj))
    if len(cnpj) != 14 or cnpj == cnpj[0] * 14:
        return False
    
    # Cálculo dos dígitos verificadores
    pesos1 = [5,4,3,2,9,8,7,6,5,4,3,2]
    dv1 = 11 - (sum(int(cnpj[i])*pesos1[i] for i in range(12)) % 11)
    dv1 = 0 if dv1 >= 10 else dv1
    
    pesos2 = [6,5,4,3,2,9,8,7,6,5,4,3,2]
    dv2 = 11 - (sum(int(cnpj[i])*pesos2[i] for i in range(13)) % 11)
    dv2 = 0 if dv2 >= 10 else dv2
    
    return int(cnpj[12]) == dv1 and int(cnpj[13]) == dv2

def limpar_telefone(tel):
    """Extrai apenas números de telefone"""
    if pd.isna(tel) or tel == '':
        return np.nan
    nums = re.sub(r'[^0-9]', '', str(tel))
    return nums if len(nums) >= 10 else np.nan

def processar_arquivo_com_fallback(input_file):
    """Tenta ler o arquivo com diferentes abordagens"""
    try:
        # Primeira tentativa: leitura normal
        return pd.read_csv(input_file, sep=';', dtype=str)
    except pd.errors.ParserError:
        try:
            # Segunda tentativa: ignorando linhas problemáticas
            return pd.read_csv(input_file, sep=';', dtype=str, on_bad_lines='warn')
        except:
            # Terceira tentativa: leitura linha por linha
            with open(input_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # Processa manualmente mantendo as primeiras 7 colunas
            cleaned_lines = []
            for line in lines:
                parts = line.strip().split(';')
                cleaned_lines.append(';'.join(parts[:7]))
            
            return pd.read_csv(StringIO('\n'.join(cleaned_lines)), sep=';', dtype=str)

def fazerLeads():
    # Configurações
    nomeTabela = "empresa_202506"
    input_csv = f"export/{nomeTabela}.csv"
    output_dir = "LEADS"
    os.makedirs(output_dir, exist_ok=True)

    try:
        # Carrega o arquivo com tratamento robusto
        df = processar_arquivo_com_fallback(input_csv)
        
        # Mapeamento de colunas
        col_map = {
            'CNPJ_COMPLETO': 'CNPJ',
            'CNPJ': 'CNPJ',
            'RAZAO_SOCIAL': 'RAZAO_SOCIAL',
            'ENDERECO_COMPLETO': 'ENDERECO',
            'ENDERECO': 'ENDERECO',
            'CORREIO_ELETRONICO': 'EMAIL',
            'EMAIL': 'EMAIL',
            'ANO_DE_ABERTURA': 'ANO_ABERTURA',
            'ANO_ABERTURA': 'ANO_ABERTURA',
            'NATUREZA_JURIDICA': 'NATUREZA_JURIDICA',
            'TELEFONE': 'TELEFONES',
            'TELEFONES': 'TELEFONES'
        }
        
        # Renomeia colunas
        df = df.rename(columns={k:v for k,v in col_map.items() if k in df.columns})
        
        # Seleciona apenas colunas necessárias
        cols_necessarias = ['CNPJ','RAZAO_SOCIAL','ENDERECO','EMAIL',
                           'ANO_ABERTURA','NATUREZA_JURIDICA','TELEFONES']
        df = df[[c for c in cols_necessarias if c in df.columns]].copy()
        
        # Validação e limpeza
        df['CNPJ'] = df['CNPJ'].astype(str).str.zfill(14)
        df = df[df['CNPJ'].apply(validar_cnpj)]
        
        df['RAZAO_SOCIAL'] = df['RAZAO_SOCIAL'].fillna('nan').str.upper()
        ltda_mask = df['RAZAO_SOCIAL'].str.endswith('LTDA')
        
        df['ENDERECO'] = df['ENDERECO'].fillna('nan')
        df['EMAIL'] = df['EMAIL'].apply(lambda x: 'nan' if pd.isna(x) or '@' not in str(x) else x)
        df['ANO_ABERTURA'] = df['ANO_ABERTURA'].astype(str).str[:4].fillna('nan')
        df['NATUREZA_JURIDICA'] = df['NATUREZA_JURIDICA'].fillna('nan')
        df['TELEFONES'] = df['TELEFONES'].apply(limpar_telefone)
        df = df.dropna(subset=['TELEFONES'])
        df['TELEFONES'] = df['TELEFONES'].astype(str)
        
        # Salva os arquivos
        df[ltda_mask].to_csv(f"{output_dir}/LTDA.csv", sep=';', index=False, encoding='utf-8')
        df[~ltda_mask].to_csv(f"{output_dir}/MEI_ME.csv", sep=';', index=False, encoding='utf-8')
        df.to_csv(f"{output_dir}/leads_processados.csv", sep=';', index=False, encoding='utf-8')
        
        print(f"""
✅ Processamento concluído com sucesso!
• Empresas LTDA: {sum(ltda_mask)}
• Empresas MEI/ME: {sum(~ltda_mask)}
• Total de leads válidos: {len(df)}
Arquivos salvos em: {output_dir}/
        """)
        return True

    except Exception as e:
        print(f"🚨 Erro crítico: {str(e)}")
        return False

def mainCompleto():
    """Executa o processo completo de ponta a ponta"""
    mainPegarDados()                # 1. Baixa os arquivos da Receita Federal
    time.sleep(1)
    dezipar()                       # 2. Descompacta os arquivos ZIP
    time.sleep(1)
    apagarDentroPasta('arquivo receita') # 3. Limpa arquivos ZIP baixados
    time.sleep(1)
    processar_arquivos_deszipados() # 4. Divide os CSVs em 10 partes
    time.sleep(1)
    recreate_database_tables()      # 5. Apaga e recria todas as tabelas no banco
    time.sleep(1)
    apagar_arquivos_especificos()   # 6. Limpa arquivos CSV descompactados
    time.sleep(1)
    recreate_database_tables()      # 7 Recriar as tabelas do banco
    time.sleep(1)
    mainJogarNoBanco()              # 8. Insere os dados divididos no banco
    time.sleep(1)
    apagarDentroPasta10Para()       # 9. Limpa arquivos divididos em 10 partes
    time.sleep(1)
    create_and_export_empresas()    # 10. Cria tabela final e exporta para CSV
    time.sleep(1)
    fazerLeads()                    # 11. Fazer LEDS
    time.sleep(1)
    apagarDentroPasta('export')     # apagar o Export
    time.sleep(1)
    print("\n✅ PROCESSO CONCLUÍDO COM SUCESSO!")
                                  

if __name__ == "__main__":
    mainCompleto()
