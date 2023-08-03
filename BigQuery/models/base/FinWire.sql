
{{
    config(
        materialized = 'table',
        cluster_by = 'rectype'
    )
}}

select string_field_0 as value,  substring(string_field_0, 16, 3) rectype  from 
{{source(var('benchmark'), 'FinWireStg') }}