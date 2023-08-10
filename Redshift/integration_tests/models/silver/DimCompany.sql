{{
    config(
        materialized = 'table'
    )
}}
--,index='CLUSTERED COLUMNSTORE INDEX'
--,dist='REPLICATE'

SELECT *
FROM (
    SELECT
        CAST(cik AS BIGINT) companyid,
        st.st_name status,
        companyname name,
        ind.in_name industry,
        --    iff(
        --      SPrating IN ('AAA','AA','AA+','AA-','A','A+','A-','BBB','BBB+','BBB-','BB','BB+','BB-','B','B+','B-','CCC','CCC+','CCC-','CC','C','D'), 
        --      SPrating, 
        --      cast(null as string)
        --    ) sprating, 
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
            ELSE CAST(NULL AS VARCHAR(4))
        END sprating,
        --    CASE
        --      WHEN SPrating IN ('AAA','AA','A','AA+','A+','AA-','A-','BBB','BBB+','BBB-') THEN false
        --      WHEN SPrating IN ('BB','B','CCC','CC','C','D','BB+','B+','CCC+','BB-','B-','CCC-') THEN true
        --      ELSE cast(null as boolean)
        --    END as islowgrade, 
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
                THEN CAST(0 AS BOOLEAN)
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
                THEN CAST(1 AS BOOLEAN)
            ELSE CAST(0 AS BOOLEAN)
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
        --    nvl2(lead(pts) OVER (PARTITION BY cik ORDER BY pts), true, false) iscurrent,
        --    CASE WHEN lead(pts) OVER (PARTITION BY cik ORDER BY pts) IS NOT NULL THEN 1 ELSE 0 END iscurrent,
        --    CASE WHEN LEAD(pts) OVER (PARTITION BY cik ORDER BY pts) IS NOT NULL THEN CAST(1 AS INTEGER) ELSE CAST(0 AS INTEGER) END AS iscurrent,
        CASE
            WHEN
                LEAD(pts) OVER (PARTITION BY cik ORDER BY pts) IS NOT NULL
                THEN 1
            ELSE 0
        END iscurrent,

        1 batchid,
        --date(pts) effectivedate,
        CAST(pts AS DATE) effectivedate,
        --concat(companyid, '-', effectivedate) sk_companyid,
        CONCAT(
            CONCAT(CAST(cik AS BIGINT), '-'), CAST(pts AS DATE)
        ) sk_companyid,
        --    coalesce(
        --      lead(date(pts)) OVER (PARTITION BY cik ORDER BY pts),
        --      cast('9999-12-31' as date)) enddate
        COALESCE(
            LEAD(CAST(pts AS DATE)) OVER (PARTITION BY cik ORDER BY pts),
            CAST('9999-12-31' AS DATE)
        ) enddate
    FROM (
        SELECT
            --      to_timestamp(substring(value, 1, 15), 'yyyyMMdd-HHmmss') AS PTS,
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
            --      to_date(iff(trim(substring(value, 99, 8))='',NULL,substring(value, 99, 8)), 'yyyyMMdd') AS FoundingDate,
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
        --FROM stg.FinWire
        WHERE rectype = 'CMP'
    ) cmp
        JOIN {{ source('tpcdi', 'StatusType') }} st ON cmp.status = st.st_id
        --JOIN prd.StatusType st ON cmp.status = st.st_id
        JOIN {{ source('tpcdi', 'Industry') }} ind ON cmp.industryid = ind.in_id
--JOIN prd.Industry ind ON cmp.industryid = ind.in_id
) t
