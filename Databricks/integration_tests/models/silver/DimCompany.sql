{{
    config(
        materialized = 'table'
    )
}}
SELECT *
FROM (
    SELECT

        CAST(cik AS BIGINT) companyid,
        st.st_name status,
        companyname name,
        ind.in_name industry,
        IF(
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
            ),
            sprating,
            CAST(null AS STRING)
        ) sprating,
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
                THEN true
            ELSE CAST(null AS BOOLEAN)
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
        NVL2(
            LEAD(pts) OVER (PARTITION BY cik ORDER BY pts), true, false
        ) iscurrent,
        1 batchid,
        DATE(pts) effectivedate,
        BIGINT(
            CONCAT(
                DATE_FORMAT(effectivedate, 'yyyyMMdd'),
                CAST(companyid AS STRING)
            )
        ) AS sk_companyid,
        COALESCE(
            LEAD(DATE(pts)) OVER (PARTITION BY cik ORDER BY pts),
            CAST('9999-12-31' AS DATE)
        ) enddate
    FROM (
        SELECT
            TO_TIMESTAMP(SUBSTRING(value, 1, 15), 'yyyyMMdd-HHmmss') AS pts,
            TRIM(SUBSTRING(value, 19, 60)) AS companyname,
            TRIM(SUBSTRING(value, 79, 10)) AS cik,
            TRIM(SUBSTRING(value, 89, 4)) AS status,
            TRIM(SUBSTRING(value, 93, 2)) AS industryid,
            TRIM(SUBSTRING(value, 95, 4)) AS sprating,
            TO_DATE(
                IFF(
                    TRIM(SUBSTRING(value, 99, 8)) = '',
                    null,
                    SUBSTRING(value, 99, 8)
                ),
                'yyyyMMdd'
            ) AS foundingdate,
            TRIM(SUBSTRING(value, 107, 80)) AS addrline1,
            TRIM(SUBSTRING(value, 187, 80)) AS addrline2,
            TRIM(SUBSTRING(value, 267, 12)) AS postalcode,
            TRIM(SUBSTRING(value, 279, 25)) AS city,
            TRIM(SUBSTRING(value, 304, 20)) AS stateprovince,
            TRIM(SUBSTRING(value, 324, 24)) AS country,
            TRIM(SUBSTRING(value, 348, 46)) AS ceoname,
            TRIM(SUBSTRING(value, 394, 150)) AS description
        FROM {{ ref('FinWire') }}
        WHERE rectype = 'CMP'
    ) cmp
        JOIN {{ source('tpcdi', 'StatusType') }} st ON cmp.status = st.st_id
        JOIN {{ source('tpcdi', 'Industry') }} ind ON cmp.industryid = ind.in_id
)
