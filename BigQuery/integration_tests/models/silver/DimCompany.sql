{{
    config(
        materialized = 'table'
    )
}}
SELECT
    CAST(cmp.cik AS BIGINT) AS companyid,
    st.st_name AS status,
    cmp.companyname AS name, -- noqa:RF04
    ind.in_name AS industry,
    IF(cmp.sprating IN (
        'AAA',
        'AA',
        'AA+',
        'AA-',
        'A',
        'A+',
        'A-',
        'BBB',
        'BBB+',
        'BBB-',
        'BB',
        'BB+',
        'BB-',
        'B',
        'B+',
        'B-',
        'CCC',
        'CCC+',
        'CCC-',
        'CC',
        'C',
        'D'
    ), cmp.sprating, CAST(NULL AS STRING
    )) AS sprating,
    CASE
        WHEN
            sprating IN (
                'AAA',
                'AA',
                'A',
                'AA+',
                'A+',
                'AA-',
                'A-',
                'BBB',
                'BBB+',
                'BBB-'
            )
            THEN FALSE
        WHEN sprating IN (
            'BB',
            'B',
            'CCC',
            'CC',
            'C',
            'D',
            'BB+',
            'B+',
            'CCC+',
            'BB-',
            'B-',
            'CCC-'
        ) THEN TRUE
        ELSE
            CAST(NULL AS BOOLEAN)
    END
        AS islowgrade,
    cmp.ceoname AS ceo,
    cmp.addrline1 AS addressline1,
    cmp.addrline2 AS addressline2,
    cmp.postalcode,
    cmp.city,
    cmp.stateprovince AS stateprov,
    cmp.country,
    cmp.description,
    cmp.foundingdate,
    COALESCE(
        LEAD(cmp.pts) OVER (PARTITION BY cmp.cik ORDER BY cmp.pts) IS NOT NULL,
        FALSE
    ) AS iscurrent,
    1 AS batchid,
    DATE(cmp.pts) AS effectivedate,
    CONCAT(CAST(cmp.cik AS BIGINT), '-', DATE(cmp.pts)) AS sk_companyid,
    COALESCE(
        LEAD(DATE(cmp.pts)) OVER (PARTITION BY cmp.cik ORDER BY cmp.pts),
        CAST('9999-12-31' AS DATE)
    ) AS enddate
FROM (
    SELECT
        PARSE_TIMESTAMP('%E4Y%m%d-%H%M%S', SUBSTRING(value, 1, 15)) AS pts,
        TRIM(SUBSTRING(value, 19, 60)) AS companyname,
        TRIM(SUBSTRING(value, 79, 10)) AS cik,
        TRIM(SUBSTRING(value, 89, 4)) AS status,
        TRIM(SUBSTRING(value, 93, 2)) AS industryid,
        TRIM(SUBSTRING(value, 95, 4)) AS sprating,
        PARSE_DATE(
            '%E4Y%m%d',
            IF(
                CAST(TRIM(SUBSTRING(value, 99, 8)) AS STRING) = '',
                NULL,
                CAST(TRIM(SUBSTRING(value, 99, 8)) AS STRING)
            )
        ) AS foundingdate,
        TRIM(SUBSTRING(value, 107, 80)) AS addrline1,
        TRIM(SUBSTRING(value, 187, 80)) AS addrline2,
        TRIM(SUBSTRING(value, 267, 12)) AS postalcode,
        TRIM(SUBSTRING(value, 279, 25)) AS city,
        TRIM(SUBSTRING(value, 304, 20)) AS stateprovince,
        TRIM(SUBSTRING(value, 324, 24)) AS country,
        TRIM(SUBSTRING(value, 348, 46)) AS ceoname,
        TRIM(SUBSTRING(value, 394, 150)) AS description
    FROM
        {{ ref('FinWire') }}
    WHERE rectype = 'CMP'
) AS cmp
    INNER JOIN
        {{ source(var('benchmark'),'StatusType') }} AS st
        ON
            cmp.status = st.st_id
    INNER JOIN
        {{ source(var('benchmark'),'Industry') }} AS ind
        ON
            cmp.industryid = ind.in_id
