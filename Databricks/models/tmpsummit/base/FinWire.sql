{{
    config(
        materialized = 'view',
        partition_by = 'rectype'
    )
}}

select * from {{ var('catalog') }}.ricardo_portilla_tpcdi_stmv_10_stage.finwire
