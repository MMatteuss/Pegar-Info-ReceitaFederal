-- criar database
-- create database cnpj_brasil;

-- usar o database
use cnpj_brasil

-- Tabela de empresas
CREATE TABLE empresas (
    cnpj_basico VARCHAR(8),
    razao_social VARCHAR(150),
    natureza_juridica INT,
    qualificacao_do_responsavel INT,
    capital_social DECIMAL(20,2),
    porte_da_empresa INT,
    ente_federativo_responsavel VARCHAR(100)
);	

-- Tabela de estabelecimentos
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
);

-- Tabela de sócios
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
);

-- Tabela de CNAEs secundários
CREATE TABLE cnpj_brasil.cnaes (
    cnpj_basico VARCHAR(8),
    cnae_fiscal_secundaria VARCHAR(7)
);

-- Tabela de motivos de situação cadastral
CREATE TABLE cnpj_brasil.motivos (
    codigo_motivo INT,
    descricao_motivo VARCHAR(100)
);

-- Tabela de municípios
CREATE TABLE cnpj_brasil.municipios (
    codigo_municipio INT,
    nome_municipio VARCHAR(100)
);

-- Tabela de naturezas jurídicas
CREATE TABLE cnpj_brasil.natureza (
    codigo_natureza_juridica INT,
    descricao_natureza_juridica VARCHAR(200)
);

-- Tabela de qualificações
CREATE TABLE cnpj_brasil.qualificacoes (
    codigo_qualificacao INT,
    descricao_qualificacao VARCHAR(100)
);

-- Tabela de países
CREATE TABLE cnpj_brasil.paises (
    codigo_pais VARCHAR(3),
    nome_pais VARCHAR(60)
);