{{
    config(
        materialized = 'view',
        partition_by = 'rectype'
    )
}}

SELECT
    $1 value,
    SUBSTR($1, 16, 3) rectype
FROM
    @{{ var('stage') }}/Batch1 (
        FILE_FORMAT => 'TXT_FIXED_WIDTH', PATTERN => '.*FINWIRE.*'
    )
