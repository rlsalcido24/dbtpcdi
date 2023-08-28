{{
    config(
        materialized = 'view',
        partition_by = 'rectype'
    )
}}

SELECT
    $1 AS value, -- noqa: RF04
    SUBSTR($1, 16, 3) AS rectype
FROM
    @{{ var('stage') }}/Batch1 (
        FILE_FORMAT => 'TXT_FIXED_WIDTH', PATTERN => '.*FINWIRE.*'
    )
