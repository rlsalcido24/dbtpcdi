{{
    config(
        materialized = 'table',
        cluster_by = 'rectype'
    )
}}

SELECT
    string_field_0 AS value,
    SUBSTRING(string_field_0, 16, 3) rectype
FROM
    {{ source(var('benchmark'), 'FinWireStg') }}
