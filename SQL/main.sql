-- Usar o database
USE cnpj_brasil;

-- Criar a tabela empresas_202506
CREATE TABLE empresas_202506 AS
SELECT
    e.cnpj_basico,
    e.razao_social,
    e.natureza_juridica,
    e.qualificacao_do_responsavel,
    e.capital_social,
    e.porte_da_empresa,
    e.ente_federativo_responsavel,
    est.cnpj_ordem,
    est.cnpj_dv,
    est.identificador_matriz_filial,
    est.nome_fantasia,
    est.situacao_cadastral,
    est.data_situacao_cadastral,
    est.motivo_situacao_cadastral,
    est.nome_da_cidade_no_exterior,
    est.pais AS pais_est,
    est.data_de_inicio_da_atividade,
    est.cnae_fiscal_principal,
    est.cnae_fiscal_secundaria,
    est.tipo_de_logradouro,
    est.logradouro,
    est.numero,
    est.complemento,
    est.bairro,
    est.cep,
    est.uf,
    est.municipio,
    est.ddd1,
    est.telefone1,
    est.ddd2,
    est.telefone2,
    est.ddd_do_fax,
    est.fax,
    est.correio_eletronico,
    est.situacao_especial,
    est.data_da_situacao_especial,
    s.identificador_de_socio,
    s.nome_do_socio,
    s.cnpj_ou_cpf_do_socio,
    s.qualificacao_do_socio,
    s.data_de_entrada_sociedade,
    s.pais AS pais_socio,
    s.representante_legal,
    s.nome_do_representante,
    s.qualificacao_do_representante_legal,
    s.faixa_etaria,
    c.cnae_fiscal_secundaria AS cnae_secundario,
    m.descricao_motivo,
    mun.nome_municipio,
    nat.descricao_natureza_juridica,
    q.descricao_qualificacao,
    p.nome_pais,
    CONCAT(est.cnpj_basico, LPAD(est.cnpj_ordem, 4, '0'), LPAD(est.cnpj_dv, 2, '0')) AS cnpj_completo
FROM
    empresas e
INNER JOIN
    estabelecimentos est ON e.cnpj_basico = est.cnpj_basico
LEFT JOIN
    socios s ON e.cnpj_basico = s.cnpj_basico
LEFT JOIN
    cnaes c ON e.cnpj_basico = c.cnpj_basico
LEFT JOIN
    motivos m ON est.motivo_situacao_cadastral = m.codigo_motivo
LEFT JOIN
    municipios mun ON est.municipio = mun.codigo_municipio
LEFT JOIN
    natureza nat ON e.natureza_juridica = nat.codigo_natureza_juridica
LEFT JOIN
    qualificacoes q ON e.qualificacao_do_responsavel = q.codigo_qualificacao
LEFT JOIN
    paises p ON est.pais = p.codigo_pais
WHERE 
    est.situacao_cadastral = 2
    AND est.uf IN ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE');