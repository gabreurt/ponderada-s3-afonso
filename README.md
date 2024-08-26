Query utilizada para criar a view:

CREATE OR REPLACE VIEW structured_data AS
SELECT
    toDateTime(data_ingestao) AS data_ingestao,
    parseDateTimeBestEffort(JSONExtractString(dado_linha, 'data')) AS data_venda,
    JSONExtractInt(dado_linha, 'cod_vendedor') AS cod_vendedor,
    JSONExtractString(dado_linha, 'cod_loja') AS cod_loja,
    JSONExtractString(dado_linha, 'cod_transacao') AS cod_transacao,
    JSONExtractInt(dado_linha, 'quantidade') AS quantidade,
    JSONExtractUInt(dado_linha, 'cod_prod') AS cod_prod,
    JSONExtractFloat(dado_linha, 'preco') AS preco,
    toDateTime(JSONExtractUInt(dado_linha, 'data_ingestao') / 1000) AS data_ingestao_json,
    toInt16(tag) AS ano
FROM
    default.working_data;

