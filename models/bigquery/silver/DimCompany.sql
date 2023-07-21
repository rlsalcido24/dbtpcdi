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
    IF
      ( SPrating IN ('AAA',
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
          'D'), SPrating, CAST(NULL AS string)) sprating,
      CASE
        WHEN SPrating IN ('AAA', 'AA', 'A', 'AA+', 'A+', 'AA-', 'A-', 'BBB', 'BBB+', 'BBB-') THEN FALSE
        WHEN SPrating IN ('BB',
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
        'CCC-') THEN TRUE
      ELSE
      CAST(NULL AS boolean)
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
      IFNULL(LEAD(pts) OVER (PARTITION BY cik ORDER BY pts) IS NOT NULL, FALSE) AS iscurrent,
      1 batchid,
      DATE(pts) effectivedate,
      CONCAT( CAST(cik AS BIGINT), '-', DATE(pts)) sk_companyid,
      COALESCE( LEAD(DATE(pts)) OVER (PARTITION BY cik ORDER BY pts), CAST('9999-12-31' AS date)) enddate
    FROM (
      SELECT
        PARSE_TIMESTAMP('%E4Y%m%d-%H%M%S',substring(value, 1, 15)) AS PTS,
        trim(substring(value, 19, 60)) AS CompanyName,
        trim(substring(value, 79, 10)) AS CIK,
        trim(substring(value, 89, 4)) AS Status,
        trim(substring(value, 93, 2)) AS IndustryID,
        trim(substring(value, 95, 4)) AS SPrating,
        PARSE_DATE('%E4Y%m%d',IF (CAST(trim(substring(value, 99, 8)) AS STRING)='', NULL, CAST(trim(substring(value, 99, 8)) AS STRING))) AS FoundingDate,
        trim(substring(value, 107, 80)) AS AddrLine1,
        trim(substring(value, 187, 80)) AS AddrLine2,
        trim(substring(value, 267, 12)) AS PostalCode,
        trim(substring(value, 279, 25)) AS City,
        trim(substring(value, 304, 20)) AS StateProvince,
        trim(substring(value, 324, 24)) AS Country,
        trim(substring(value, 348, 46)) AS CEOname,
        trim(substring(value, 394, 150)) AS Description
      FROM
        {{ ref('FinWire') }} WHERE rectype="CMP") as cmp
      JOIN
         {{source(var('benchmark'),'StatusType') }} as  st
      ON
        cmp.Status = st.st_id
      JOIN
        {{source(var('benchmark'),'Industry') }} as ind
      ON
        cmp.IndustryID = ind.in_id