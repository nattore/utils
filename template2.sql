WITH b_normalized AS (
    SELECT
        c.internal_id AS id,
        -- Normalize ver_date: replace NULL with a default date (e.g., '1900-01-01' or '9999-12-31' depending on use case)
        b.column_1,
        b.column_2
        date_format(CAST(date_parse(feature_date, '%Y%m') AS DATE), '%Y-%m-%d') AS ref_date,
        COALESCE(b.ver_date, DATE '1900-01-01') AS ver_date,
    FROM feature_database.feature_table AS b
    JOIN mapping_database.id_mapping_table AS c
        ON b.user_id = c.external_id
),

b_latest_version AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY id, ref_date
                   ORDER BY ver_date DESC  -- no need for CASE anymore
               ) AS rn
        FROM b_normalized
    ) sub
    WHERE rn = 1
),

b_asof_filtered AS (
    -- Step 3: Prepare all candidate (a, b) joins based on "as-of" rule
    SELECT
        a.*,
        b.*,
        ROW_NUMBER() OVER (
            PARTITION BY a.id, a.ref_date
            ORDER BY b.ref_date DESC
        ) AS rn
    FROM fundamental_database.fundamental_table AS a
    JOIN b_latest_version AS b
        ON a.id = b.id
        AND b.ref_date <= a.ref_date - INTERVAL 'n days'
),

joined_final AS (
    -- Step 4: Select only the closest b.ref_date for each a row
    SELECT *
    FROM b_asof_filtered
    WHERE rn = 1
)

-- Final projection
SELECT *
FROM joined_final;