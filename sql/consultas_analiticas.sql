-- 1. Uso de LAG - Crescimento mensal acima de 50% considerando meses consecutivos.
WITH base AS (
  SELECT id_cliente, nome_cliente, ano_mes,
         TO_DATE(CONCAT(ano_mes, '-01')) AS mes,
         valor_total
  FROM novarota.ouro.gold_resumo_cliente_mes
),
movimentacao AS (
  SELECT *,
         LAG(valor_total) OVER (PARTITION BY id_cliente ORDER BY mes) AS valor_mes_anterior,
         LAG(mes) OVER (PARTITION BY id_cliente ORDER BY mes) AS mes_anterior
  FROM base
)
SELECT id_cliente, nome_cliente, ano_mes,
       valor_mes_anterior, valor_total,
       ROUND(100 * (valor_total - valor_mes_anterior) / valor_mes_anterior, 2) AS crescimento_percentual
FROM movimentacao
WHERE mes_anterior = ADD_MONTHS(mes, -1)
  AND valor_mes_anterior > 0
  AND valor_total > valor_mes_anterior * 1.50
ORDER BY crescimento_percentual DESC;

-- 2. Uso de NTILE - Quartis de clientes por montante movimentado (quartil 1 = maior valor).
WITH base AS (
  SELECT id_cliente, MAX(nome_cliente) AS nome_cliente,
         SUM(valor_total) AS total_movimentado
  FROM novarota.ouro.gold_resumo_cliente_mes
  GROUP BY id_cliente
), quartis AS (
  SELECT *, NTILE(4) OVER (ORDER BY total_movimentado DESC, id_cliente) AS quartil
  FROM base
)
SELECT * FROM quartis ORDER BY quartil, total_movimentado DESC;

-- 3. Uso de DENSE_RANK - Ranking por mês e cidade.
WITH resumo AS (
  SELECT ano_mes, cidade_estabelecimento, nome_estabelecimento,
         SUM(valor) AS valor_total,
         COUNT(*) AS qtd_transacoes
  FROM novarota.ouro.gold_transacoes_cartao
  GROUP BY ano_mes, cidade_estabelecimento, nome_estabelecimento
), ranking AS (
  SELECT *, DENSE_RANK() OVER (
    PARTITION BY ano_mes, cidade_estabelecimento
    ORDER BY valor_total DESC, qtd_transacoes DESC
  ) AS posicao
  FROM resumo
)
SELECT * FROM ranking ORDER BY ano_mes, cidade_estabelecimento, posicao;

-- 4. SQL de auditoria das camadas.
SELECT COUNT(*) AS transacoes_validas,
       SUM(CAST(estornada AS INT)) AS transacoes_estornadas
FROM novarota.prata.transacoes;

SELECT entidade, tipo_erro, SUM(qtd_rejeitados) AS total
FROM novarota.ouro.gold_alertas_qualidade
GROUP BY entidade, tipo_erro
ORDER BY total DESC;

-- 5. Uso de ROW_NUMBER - Selecionar a versão mais recente de cada cliente

WITH clientes_ordenados AS (
    SELECT
        id_cliente,
        nome,
        cpf,
        cidade,
        estado,
        data_atualizacao,
        ROW_NUMBER() OVER (
            PARTITION BY id_cliente
            ORDER BY
                TRY_CAST(data_atualizacao AS TIMESTAMP) DESC NULLS LAST,
                data_referencia DESC NULLS LAST,
                arquivo_origem DESC,
                hash_linha DESC
        ) AS numero_versao
    FROM novarota.bronze.clientes
)

SELECT *
FROM clientes_ordenados
WHERE numero_versao = 1;
