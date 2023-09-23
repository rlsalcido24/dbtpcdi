{{
    config(
        materialized = 'view',
        partition_by = 'rectype'
    )
}}

select * from {{ var('catalog') }}.{{ var('stagingschema') }}.finwire