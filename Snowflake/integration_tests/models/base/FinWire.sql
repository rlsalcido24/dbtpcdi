{{
    config(
        materialized = 'view',
        partition_by = 'rectype'
    )
}}

SELECT $1 VALUE, SUBSTR($1, 16, 3) RECTYPE FROM @tpcdi_sf10000_raw/Batch1 (FILE_FORMAT => 'TXT_FIXED_WIDTH', PATTERN => '.*FINWIRE.*')


