{{
    config(
        materialized = 'table'
    )
}}
select
    * from {{ var('catalog') }}.{{ var('prodschema') }}.dimtime