{{
    config(
        materialized = 'view',
        partition_by = 'rectype'
    )
}}

select *, substring(value, 16, 3) rectype from 
text.`dbfs:/tmp/tpcdi/sf=10000/Batch1/FINWIRE*`

