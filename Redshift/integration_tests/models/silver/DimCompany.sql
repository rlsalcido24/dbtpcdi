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
        CAST(cmp.cik AS BIGINT) AS companyid,
        st.st_nameas AS status,
        cmp.companyname AS name, -- noqa: RF04
        ind.in_name AS industry,
        --    iff(
        --      SPrating IN ('AAA','AA','AA+','AA-','A','A+','A-','BBB','BBB+','BBB-','BB','BB+','BB-','B','B+','B-','CCC','CCC+','CCC-','CC','C','D'), -- noqa: LT05
        --      SPrating,
        --      cast(null as string)
        --    ) sprating,
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
        --    CASE -- noqa: LT05
        --      WHEN SPrating IN ('AAA','AA','A','AA+','A+','AA-','A-','BBB','BBB+','BBB-') THEN false -- noqa: LT05
        --      WHEN SPrating IN ('BB','B','CCC','CC','C','D','BB+','B+','CCC+','BB-','B-','CCC-') THEN true -- noqa: LT05
        --      ELSE cast(null as boolean) -- noqa: LT05
        --    END as islowgrade, -- noqa: LT05
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
        cmp.ceoname AS ceo,
        cmp.addrline1 AS addressline1,
        cmp.addrline2 AS addressline2,
        cmp.postalcode,
        cmp.city,
        cmp.stateprovince AS stateprov,
        cmp.country,
        cmp.description,
        cmp.foundingdate,
        --    nvl2(lead(pts) OVER (PARTITION BY cik ORDER BY pts), true, false) iscurrent, -- noqa: LT05
        --    CASE WHEN lead(pts) OVER (PARTITION BY cik ORDER BY pts) IS NOT NULL THEN 1 ELSE 0 END iscurrent, -- noqa: LT05
        --    CASE WHEN LEAD(pts) OVER (PARTITION BY cik ORDER BY pts) IS NOT NULL THEN CAST(1 AS INTEGER) ELSE CAST(0 AS INTEGER) END AS iscurrent, -- noqa: LT05
        CASE
            WHEN
                LEAD(cmp.pts)
                    OVER (PARTITION BY cmp.cik ORDER BY cmp.pts)
                IS NOT NULL
                THEN 1
            ELSE 0
        END AS iscurrent,
        1 AS batchid,
        --date(pts) effectivedate,
        CAST(cmp.pts AS DATE) AS effectivedate,
        --concat(companyid, '-', effectivedate) sk_companyid,
        CONCAT(
            CONCAT(CAST(cmp.cik AS BIGINT), '-'), CAST(cmp.pts AS DATE)
        ) AS sk_companyid,
        --    coalesce(
        --      lead(date(pts)) OVER (PARTITION BY cik ORDER BY pts),
        --      cast('9999-12-31' as date)) enddate
        COALESCE(
            LEAD(CAST(cmp.pts AS DATE))
                OVER (PARTITION BY cmp.cik ORDER BY cmp.pts),
            CAST('9999-12-31' AS DATE)
        ) AS enddate
    FROM (
        SELECT
            --      to_timestamp(substring(value, 1, 15), 'yyyyMMdd-HHmmss') AS PTS, -- noqa: LT05
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
            --      to_date(iff(trim(substring(value, 99, 8))='',NULL,substring(value, 99, 8)), 'yyyyMMdd') AS FoundingDate, -- noqa: LT05
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
    ) AS cmp
        INNER JOIN
            {{ source('tpcdi', 'StatusType') }} AS st
            ON cmp.status = st.st_id
        --JOIN prd.StatusType st ON cmp.status = st.st_id
        INNER JOIN
            {{ source('tpcdi', 'Industry') }} AS ind
            ON cmp.industryid = ind.in_id
--JOIN prd.Industry ind ON cmp.industryid = ind.in_id
) AS t
