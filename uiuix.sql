WITH data_corte AS (
    -- 1. Definimos a data de referência D0
    SELECT date_parse('20260101', '%Y%m%d') AS d0
),

safra_publico_contratos AS (
    -- 2. Isolamos TODOS os contratos elegíveis por cliente na data de corte.
    -- Um cliente pode aparecer múltiplas vezes aqui se tiver vários contratos elegíveis.
    SELECT DISTINCT 
        id AS id_cliente,
        id_contrato
    FROM publico
    WHERE data_ref = 20260101 
      -- AND regra_elegibilidade = true 
),

safra_publico_cliente AS (
    -- 3. Isolamos a lista única de clientes para ser o nosso denominador (N_0).
    -- Isso garante que João conte apenas como 1 indivíduo em risco, mesmo tendo 3 contratos.
    SELECT DISTINCT id_cliente 
    FROM safra_publico_contratos
),

primeiro_evento_valido AS (
    -- 4. APLICAÇÃO DO INNER JOIN:
    -- Cruzamos a base de eventos com os contratos elegíveis. 
    -- Se o evento ocorreu em um contrato não-elegível, o INNER JOIN o descarta.
    -- Em seguida, agrupamos pelo CLIENTE para pegar a data da PRIMEIRA conversão válida.
    SELECT 
        p.id_cliente,
        MIN(date_parse(CAST(e.data_evento AS VARCHAR), '%Y%m%d')) AS data_primeiro_evento
    FROM eventos e
    INNER JOIN safra_publico_contratos p 
        ON e.id_contrato = p.id_contrato
    GROUP BY p.id_cliente
),

calculo_janela_k AS (
    -- 5. Cruzamos a safra única de clientes com o seu primeiro evento válido
    SELECT 
        c.id_cliente,
        dt.d0,
        e.data_primeiro_evento,
        CASE 
            WHEN e.data_primeiro_evento IS NOT NULL AND e.data_primeiro_evento >= dt.d0 
            THEN floor(date_diff('day', dt.d0, e.data_primeiro_evento) / 7.0) + 1
            ELSE NULL 
        END AS semana_k
    FROM safra_publico_cliente c
    CROSS JOIN data_corte dt
    LEFT JOIN primeiro_evento_valido e 
        ON c.id_cliente = e.id_cliente
),

agregacao_eventos AS (
    -- 6. Contamos os eventos (clientes convertidos legitimamente) por semana
    SELECT 
        semana_k,
        COUNT(id_cliente) AS clientes_convertidos_ek
    FROM calculo_janela_k
    WHERE semana_k IS NOT NULL 
      AND semana_k <= 24
    GROUP BY semana_k
),

total_publico AS (
    -- 7. Calculamos o N total da população (CPFs/CNPJs únicos)
    SELECT COUNT(id_cliente) AS populacao_inicial
    FROM safra_publico_cliente
)

-- 8. Cálculo final do Hazard Rate (Janela k)
SELECT 
    a.semana_k,
    a.clientes_convertidos_ek,
    
    -- R_k = População Inicial de Clientes - (Soma cumulativa de conversões anteriores)
    (t.populacao_inicial - COALESCE(SUM(a.clientes_convertidos_ek) OVER (
        ORDER BY a.semana_k 
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ), 0)) AS clientes_em_risco_rk,
    
    -- Hazard Rate = E_k / R_k
    CAST(a.clientes_convertidos_ek AS DOUBLE) / 
    (t.populacao_inicial - COALESCE(SUM(a.clientes_convertidos_ek) OVER (
        ORDER BY a.semana_k 
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ), 0)) AS hazard_rate

FROM agregacao_eventos a
CROSS JOIN total_publico t
ORDER BY a.semana_k;
