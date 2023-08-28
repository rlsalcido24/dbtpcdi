{{
    config(
        materialized = 'table'
    )
}}

SELECT *
FROM (
    SELECT
        CAST(cmp.cik AS BIGINT) AS companyid,
        st.st_name AS status,
        cmp.companyname AS name, -- noqa: RF04
        ind.in_name AS industry,
        CASE
            WHEN
                cmp.sprating IN (
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
                )
                THEN cmp.sprating
            ELSE CAST(NULL AS VARCHAR(4))
        END AS sprating,
        CASE
            WHEN
                cmp.cmp.sprating IN (
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
                THEN CAST(0 AS BOOLEAN)
            WHEN
                cmp.sprating IN (
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
                )
                THEN CAST(1 AS BOOLEAN)
            ELSE CAST(0 AS BOOLEAN)
        END AS islowgrade,
        cmp.ceoname AS ceo,
        cmp.addrline1 AS addressline1,
        cmp.addrline2 AS addressline2,
        cmp.postalcode,
        cmp.city,
        cmp.stateprovince AS stateprov,
        cmp.country,
        cmp.description,
        cmp.foundingdate,
        CASE
            WHEN
                LEAD(cmp.pts)
                    OVER (PARTITION BY cmp.cik ORDER BY cmp.pts)
                IS NOT NULL
                THEN 1
            ELSE 0
        END AS iscurrent,
        1 AS batchid,
        CAST(cmp.pts AS DATE) AS effectivedate,
        CONCAT(
            CONCAT(CAST(cmp.cik AS BIGINT), '-'), CAST(cmp.pts AS DATE)
        ) AS sk_companyid,
        COALESCE(
            LEAD(
                CAST(cmp.pts AS DATE))
                OVER (PARTITION BY cmp.cik ORDER BY cmp.pts),
            CAST('9999-12-31' AS DATE)
        ) AS enddate
    FROM (
        SELECT
            TO_TIMESTAMP(
                SUBSTRING(value, 1, 8)
                || ' '
                || SUBSTRING(value, 10, 2)
                || ':'
                || SUBSTRING(value, 12, 2)
                || ':'
                || SUBSTRING(value, 14, 2),
                'YYYYMMDD HH24:MI:SS'
            ) AS pts,
            TRIM(SUBSTRING(value, 19, 60)) AS companyname,
            TRIM(SUBSTRING(value, 79, 10)) AS cik,
            TRIM(SUBSTRING(value, 89, 4)) AS status,
            TRIM(SUBSTRING(value, 93, 2)) AS industryid,
            TRIM(SUBSTRING(value, 95, 4)) AS sprating,
            TO_DATE(
                NULLIF(TRIM(SUBSTRING(value, 99, 8)), ''), 'YYYYMMDD'
            ) AS foundingdate,

            TRIM(SUBSTRING(value, 107, 80)) AS addrline1,
            TRIM(SUBSTRING(value, 187, 80)) AS addrline2,
            TRIM(SUBSTRING(value, 267, 12)) AS postalcode,
            TRIM(SUBSTRING(value, 279, 25)) AS city,
            TRIM(SUBSTRING(value, 304, 20)) AS stateprovince,
            TRIM(SUBSTRING(value, 324, 24)) AS country,
            TRIM(SUBSTRING(value, 348, 46)) AS ceoname,
            TRIM(SUBSTRING(value, 394, 150)) AS description
        FROM {{ ref('finwire') }}
        WHERE rectype = 'CMP'
    ) AS cmp
        INNER JOIN {{ source('tpcdi', 'StatusType') }} AS st
            ON cmp.status = st.st_id
        INNER JOIN {{ source('tpcdi', 'Industry') }} AS ind
            ON cmp.industryid = ind.in_id
) AS t
