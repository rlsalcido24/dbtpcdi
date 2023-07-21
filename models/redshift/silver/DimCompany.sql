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
    cast(cik as BIGINT) companyid,
    st.st_name status,
    companyname name,
    ind.in_name industry,
--    iff(
--      SPrating IN ('AAA','AA','AA+','AA-','A','A+','A-','BBB','BBB+','BBB-','BB','BB+','BB-','B','B+','B-','CCC','CCC+','CCC-','CC','C','D'), 
--      SPrating, 
--      cast(null as string)
--    ) sprating, 
    CASE 
        WHEN SPrating IN ('AAA','AA','AA+','AA-','A','A+','A-','BBB','BBB+','BBB-','BB','BB+','BB-','B','B+','B-','CCC','CCC+','CCC-','CC','C','D')
        THEN SPrating
        ELSE cast(null as varchar(4))
    END sprating,
--    CASE
--      WHEN SPrating IN ('AAA','AA','A','AA+','A+','AA-','A-','BBB','BBB+','BBB-') THEN false
--      WHEN SPrating IN ('BB','B','CCC','CC','C','D','BB+','B+','CCC+','BB-','B-','CCC-') THEN true
--      ELSE cast(null as boolean)
--    END as islowgrade, 
    CASE
      WHEN SPrating IN ('AAA','AA','A','AA+','A+','AA-','A-','BBB','BBB+','BBB-') THEN CAST(0 AS boolean)
      WHEN SPrating IN ('BB','B','CCC','CC','C','D','BB+','B+','CCC+','BB-','B-','CCC-') THEN CAST(1 AS boolean)
      ELSE cast(0 as boolean)
    END as islowgrade, 
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
    CASE WHEN lead(pts) OVER (PARTITION BY cik ORDER BY pts) IS NOT NULL THEN 1 ELSE 0 END iscurrent,

    1 batchid,
    --date(pts) effectivedate,
    cast(pts as date) effectivedate,
    --concat(companyid, '-', effectivedate) sk_companyid,
    concat(concat(cast(cik as BIGINT), '-'), cast(pts as date)) sk_companyid,
--    coalesce(
--      lead(date(pts)) OVER (PARTITION BY cik ORDER BY pts),
--      cast('9999-12-31' as date)) enddate
    coalesce(
      lead(cast(pts as date)) OVER (PARTITION BY cik ORDER BY pts),
      cast('9999-12-31' as date)) enddate
  FROM (
    SELECT
--      to_timestamp(substring(value, 1, 15), 'yyyyMMdd-HHmmss') AS PTS,
      TO_TIMESTAMP(SUBSTRING(value, 1, 8) || ' ' || SUBSTRING(value, 10, 2) || ':' || SUBSTRING(value, 12, 2) || ':' || SUBSTRING(value, 14, 2), 'YYYYMMDD HH24:MI:SS') AS PTS,
      trim(substring(value, 19, 60)) AS CompanyName,
      trim(substring(value, 79, 10)) AS CIK,
      trim(substring(value, 89, 4)) AS Status,
      trim(substring(value, 93, 2)) AS IndustryID,
      trim(substring(value, 95, 4)) AS SPrating,
--      to_date(iff(trim(substring(value, 99, 8))='',NULL,substring(value, 99, 8)), 'yyyyMMdd') AS FoundingDate,
      TO_DATE(NULLIF(TRIM(SUBSTRING(value, 99, 8)), ''), 'YYYYMMDD') AS FoundingDate,

      trim(substring(value, 107, 80)) AS AddrLine1,
      trim(substring(value, 187, 80)) AS AddrLine2,
      trim(substring(value, 267, 12)) AS PostalCode,
      trim(substring(value, 279, 25)) AS City,
      trim(substring(value, 304, 20)) AS StateProvince,
      trim(substring(value, 324, 24)) AS Country,
      trim(substring(value, 348, 46)) AS CEOname,
      trim(substring(value, 394, 150)) AS Description
    FROM {{ ref('finwire') }}
    --FROM stg.FinWire
    WHERE rectype = 'CMP'
       ) cmp
  JOIN {{ source('tpcdi', 'StatusType') }} st ON cmp.status = st.st_id
  --JOIN prd.StatusType st ON cmp.status = st.st_id
  JOIN {{ source('tpcdi', 'Industry') }} ind ON cmp.industryid = ind.in_id
  --JOIN prd.Industry ind ON cmp.industryid = ind.in_id
) T
