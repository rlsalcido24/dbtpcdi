{{
    config(
        materialized = 'table'
    )
}}
SELECT
    CAST(cik AS BIGINT) companyid,
    st.st_name status,
    companyname name,
    ind.in_name industry,
    IF(sprating IN (
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
    ), sprating, CAST(null AS STRING
    )) sprating,
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
            THEN false
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
        ) THEN true
        ELSE
            CAST(null AS BOOLEAN)
    END
        AS islowgrade,
    ceoname ceo,
    addrline1 addressline1,
    addrline2 addressline2,
    postalcode,
    city,
    stateprovince stateprov,
    country,
    description,
    foundingdate,
    COALESCE(
        LEAD(pts) OVER (PARTITION BY cik ORDER BY pts) IS NOT NULL, false
    ) AS iscurrent,
    1 batchid,
    DATE(pts) effectivedate,
    CONCAT(CAST(cik AS BIGINT), '-', DATE(pts)) sk_companyid,
    COALESCE(
        LEAD(DATE(pts)) OVER (PARTITION BY cik ORDER BY pts),
        CAST('9999-12-31' AS DATE)
    ) enddate
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
                null,
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
    WHERE rectype = "CMP"
) AS cmp
    JOIN
        {{ source(var('benchmark'),'StatusType') }} AS st
        ON
            cmp.status = st.st_id
    JOIN
        {{ source(var('benchmark'),'Industry') }} AS ind
        ON
            cmp.industryid = ind.in_id
