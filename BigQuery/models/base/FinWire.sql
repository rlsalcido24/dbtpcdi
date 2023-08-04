{{
    config(
        materialized = 'table',
        cluster_by = 'rectype'
    )
}}

SELECT
    string_field_0 AS value, -- noqa: RF04
    substring(string_field_0, 16, 3) AS rectype
FROM
    {{ source(var('benchmark'), 'FinWireStg') }}
