{{
    config(
        materialized = 'view'
    )
}}

select
    * from {{ var('catalog') }}.{{ var('prodschema') }}.batchdate

