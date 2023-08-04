{{
    config(
        materialized = 'table',
        cluster_by = 'rectype'
    )
}}

SELECT
    string_field_0 AS value,
    substring(string_field_0, 16, 3) rectype
FROM
    {{ source(var('benchmark'), 'FinWireStg') }}
