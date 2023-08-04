{{
    config(
        materialized = 'view'
    )
}}
SELECT *
FROM
    hive_metastore.roberto_salcido_tpcdi_stage.customermgmt1000
