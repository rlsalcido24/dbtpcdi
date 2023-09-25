{{
    config(
        materialized = 'table',
        partition_by = 'rectype'
    )
}}

SELECT value, substring(value, 16, 3) rectype FROM (   SELECT value FROM text.`/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/sf="{{var('scalefactor')}}"/Batch1/FINWIRE[0-9][0-9][0-9][0-9]Q[1-4]`)
