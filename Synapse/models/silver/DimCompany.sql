{{
    config(
        materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='REPLICATE'
    )
}}
SELECT *
FROM (
    SELECT
        CAST(cmp.cik AS BIGINT) AS companyid,
        st.st_name AS status,
        t.companyname AS name,
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
                cmp.sprating IN (
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
                THEN 0
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
                THEN 1
            ELSE CAST(NULL AS BIT)
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
        CONCAT(CAST(cmp.cik AS BIGINT), '-', CAST(cmp.pts AS DATE))
            AS sk_companyid,
        COALESCE(
            LEAD(CAST(cmp.pts AS DATE))
                OVER (PARTITION BY cmp.cik ORDER BY cmp.pts),
            CAST('9999-12-31' AS DATE)
        ) AS enddate
    FROM (
        SELECT
            CONVERT( --noqa: CV11
                DATETIME2,
                SUBSTRING(value, 1, 8)
                + ' '
                + SUBSTRING(value, 10, 2)
                + ':'
                + SUBSTRING(value, 12, 2)
                + ':'
                + SUBSTRING(value, 14, 2),
                112
            ) AS pts,
            TRIM(SUBSTRING(value, 19, 60)) AS companyname,
            TRIM(SUBSTRING(value, 79, 10)) AS cik,
            TRIM(SUBSTRING(value, 89, 4)) AS status,
            TRIM(SUBSTRING(value, 93, 2)) AS industryid,
            TRIM(SUBSTRING(value, 95, 4)) AS sprating,
            CONVERT( --noqa: CV11
                DATE,
                CASE
                    WHEN TRIM(SUBSTRING(value, 99, 8)) = '' THEN NULL ELSE
                        SUBSTRING(value, 99, 8)
                END,
                112
            ) AS foundingdate,
            TRIM(SUBSTRING(value, 107, 80)) AS addrline1,
            TRIM(SUBSTRING(value, 187, 80)) AS addrline2,
            TRIM(SUBSTRING(value, 267, 12)) AS postalcode,
            TRIM(SUBSTRING(value, 279, 25)) AS city,
            TRIM(SUBSTRING(value, 304, 20)) AS stateprovince,
            TRIM(SUBSTRING(value, 324, 24)) AS country,
            TRIM(SUBSTRING(value, 348, 46)) AS ceoname,
            TRIM(SUBSTRING(value, 394, 150)) AS description
        FROM {{ ref('FinWire_CMP') }}
    ) AS cmp
        INNER JOIN {{ ref('StatusType') }} AS st
            ON cmp.status = st.st_id
        INNER JOIN {{ ref('Industry') }} AS ind
            ON cmp.industryid = ind.in_id
) AS t
