WITH data_corte AS (
    -- 1. Definição da data de referência (D0)
    SELECT date_parse('20260101', '%Y%m%d') AS d0
),

safra_publico_contratos AS (
    -- 2. Mapeamento de contratos elegíveis na data de corte
    SELECT DISTINCT 
        id AS id_cliente,
        id_contrato
    FROM publico
    WHERE data_ref = 20260101
      -- AND regra_elegibilidade = true 
),

safra_publico_cliente AS (
    -- 3. Universo de clientes (a granularidade final do modelo)
    SELECT DISTINCT id_cliente 
    FROM safra_publico_contratos
),

primeiro_evento_valido AS (
    -- 4. Busca do primeiro evento pós-D0 associado a um contrato elegível
    SELECT 
        p.id_cliente,
        MIN(date_parse(CAST(e.data_evento AS VARCHAR), '%Y%m%d')) AS data_primeiro_evento
    FROM eventos e
    INNER JOIN safra_publico_contratos p 
        ON e.id_contrato = p.id_contrato
    CROSS JOIN data_corte dt
    -- Filtro crucial: ignorar eventos que ocorreram ANTES da data de referência
    WHERE date_parse(CAST(e.data_evento AS VARCHAR), '%Y%m%d') >= dt.d0
    GROUP BY p.id_cliente
)

-- 5. Construção das Targets (Flags Binárias Cumulativas)
SELECT 
    c.id_cliente,
    dt.d0 AS data_referencia,
    e.data_primeiro_evento,
    
    -- Coluna de apoio: tempo exato até o evento (útil para modelos de sobrevivência no futuro)
    date_diff('day', dt.d0, e.data_primeiro_evento) AS dias_ate_conversao,

    -- Construção das targets: 1 se converteu dentro do prazo, 0 caso contrário (ou se nunca converteu)
    CASE 
        WHEN e.data_primeiro_evento IS NOT NULL 
         AND date_diff('day', dt.d0, e.data_primeiro_evento) <= (4 * 7) THEN 1 
        ELSE 0 
    END AS target_4w,

    CASE 
        WHEN e.data_primeiro_evento IS NOT NULL 
         AND date_diff('day', dt.d0, e.data_primeiro_evento) <= (8 * 7) THEN 1 
        ELSE 0 
    END AS target_8w,

    CASE 
        WHEN e.data_primeiro_evento IS NOT NULL 
         AND date_diff('day', dt.d0, e.data_primeiro_evento) <= (12 * 7) THEN 1 
        ELSE 0 
    END AS target_12w,

    CASE 
        WHEN e.data_primeiro_evento IS NOT NULL 
         AND date_diff('day', dt.d0, e.data_primeiro_evento) <= (16 * 7) THEN 1 
        ELSE 0 
    END AS target_16w,
    
    CASE 
        WHEN e.data_primeiro_evento IS NOT NULL 
         AND date_diff('day', dt.d0, e.data_primeiro_evento) <= (24 * 7) THEN 1 
        ELSE 0 
    END AS target_24w

FROM safra_publico_cliente c
CROSS JOIN data_corte dt
LEFT JOIN primeiro_evento_valido e 
    ON c.id_cliente = e.id_cliente;
