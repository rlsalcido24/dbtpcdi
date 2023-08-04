{{
    config(
        materialized = 'view'
    )
}}
SELECT *
FROM
    fe-dev-sandbox.tpcdi_eu.CustomerMgmt_{{ var('benchmark') }}
