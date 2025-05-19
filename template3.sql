with normalized_feature as (
    select c.internal_id as id,
        b.column_1,
        b.column_2,
        -- (..)
        b.column_n,
        date_format(date_parse(b.feature_ref_date, '%Y%m'), '%Y-%m-%d') as ref_date,
        COALESCE(b.ver_date, DATE '9999-12-31') AS ver_date
    from feature_database.feature_table as b
    left join mapping_database.mapping_table as c
        on b.user_id = c.external_id
    where date_parse(b.feature_ref_date, '%Y%m') = date '{process_date}'
    and condition_1 = {condition_1}  -- ver_date
    and condition_2 = {condition_2}  -- produto
    -- (...)
    and condition_n = {condition_n}  -- atributo
);