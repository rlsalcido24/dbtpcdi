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
        CAST(cik AS BIGINT) companyid,
        st.st_name status,
        companyname name,
        ind.in_name industry,
        CASE
            WHEN
                sprating IN (
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
                THEN sprating
            ELSE CAST(null AS VARCHAR(4))
        END sprating,
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
                THEN 0
            WHEN
                sprating IN (
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
            ELSE CAST(null AS BIT)
        END AS islowgrade,
        ceoname ceo,
        addrline1 addressline1,
        addrline2 addressline2,
        postalcode,
        city,
        stateprovince stateprov,
        country,
        description,
        foundingdate,
        CASE
            WHEN
                LEAD(pts) OVER (PARTITION BY cik ORDER BY pts) IS NOT NULL
                THEN 1
            ELSE 0
        END iscurrent,
        1 batchid,
        CAST(pts AS DATE) effectivedate,
        CONCAT(CAST(cik AS BIGINT), '-', CAST(pts AS DATE)) sk_companyid,
        COALESCE(
            LEAD(CAST(pts AS DATE)) OVER (PARTITION BY cik ORDER BY pts),
            CAST('9999-12-31' AS DATE)
        ) enddate
    FROM (
        SELECT
            CONVERT(
                DATETIME2,
                SUBSTRING([value], 1, 8)
                + ' '
                + SUBSTRING([value], 10, 2)
                + ':'
                + SUBSTRING([value], 12, 2)
                + ':'
                + SUBSTRING([value], 14, 2),
                112
            ) AS pts,
            TRIM(SUBSTRING(value, 19, 60)) AS companyname,
            TRIM(SUBSTRING(value, 79, 10)) AS cik,
            TRIM(SUBSTRING(value, 89, 4)) AS status,
            TRIM(SUBSTRING(value, 93, 2)) AS industryid,
            TRIM(SUBSTRING(value, 95, 4)) AS sprating,
            CONVERT(
                DATE,
                CASE
                    WHEN TRIM(SUBSTRING(value, 99, 8)) = '' THEN null ELSE
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
    ) cmp
        JOIN {{ ref('StatusType') }} st ON cmp.status = st.st_id
        JOIN {{ ref('Industry') }} ind ON cmp.industryid = ind.in_id
) t
