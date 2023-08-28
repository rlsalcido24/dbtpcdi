{{
    config(
        materialized = 'view'
    )
}}

SELECT *
FROM
    hive_metastore.{{ var('stagingschema') }}.customermgmt
