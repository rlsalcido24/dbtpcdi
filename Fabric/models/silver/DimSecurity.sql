{{
    config(
        materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='HASH(symbol)'
    )
}}

        
SELECT 
  Symbol,
  issue,
  status,
  Name,
  exchangeid,
  sk_companyid,
  sharesoutstanding,
  firsttrade,
  firsttradeonexchange,
  Dividend,
  CASE WHEN enddate = convert(date, '9999-12-31') THEN 1 ELSE 0 END iscurrent,
  1 batchid,
  effectivedate,
  concat(exchangeid, '-', effectivedate) as sk_securityid, 
  enddate
FROM (
  SELECT 
    fws.Symbol,
    fws.issue,
    fws.status,
    fws.Name,
    fws.exchangeid,
    dc.sk_companyid,
    fws.sharesoutstanding,
    fws.firsttrade,
    fws.firsttradeonexchange,
    fws.Dividend,
    CASE WHEN fws.effectivedate < dc.effectivedate THEN dc.effectivedate ELSE fws.effectivedate END effectivedate,
    CASE WHEN fws.enddate > dc.enddate THEN dc.enddate ELSE fws.enddate END enddate
  FROM (
    SELECT 
      fws.effectivedate,
      fws.Symbol,
      fws.issue,
      fws.Name,
      fws.exchangeid,
      fws.sharesoutstanding,
      fws.firsttrade,
      fws.firsttradeonexchange,
      fws.Dividend,
      ISNULL( convert(varchar,try_cast(conameorcik as bigint)), conameorcik) conameorcik,
      s.st_name as status,
      coalesce(
        lead(effectivedate) OVER (
          PARTITION BY Symbol
          ORDER BY effectivedate),
        convert(date,'9999-12-31')
      ) enddate
    FROM (
      SELECT
        convert(date,convert(datetime2, substring([value],1,8)+' '+substring([value],10,2)+':'+substring([value],12,2)+':'+substring([value],14,2), 112)) AS effectivedate,
        trim(substring(value, 19, 15)) AS Symbol,
        trim(substring(value, 34, 6)) AS issue,
        trim(substring(value, 40, 4)) AS Status,
        trim(substring(value, 44, 70)) AS Name,
        trim(substring(value, 114, 6)) AS exchangeid,
        cast(substring(value, 120, 13) as BIGINT) AS sharesoutstanding,
        convert(date,substring(value, 133, 8), 112) AS firsttrade,
        convert(date,substring(value, 141, 8), 112) AS firsttradeonexchange,
        cast(substring(value, 149, 12) AS decimal(10,2)) AS Dividend,
        trim(substring(value, 161, 60)) AS conameorcik
      FROM {{ ref('FinWire_SEC') }}
      ) fws
    JOIN {{ ref('StatusType') }} s
      ON s.st_id = fws.Status
    ) fws
  JOIN (
    SELECT 
      sk_companyid,
      name conameorcik,
      effectivedate,
      enddate
    FROM {{ ref('DimCompany') }}
    UNION ALL
    SELECT 
      sk_companyid,
      cast(companyid as varchar) conameorcik,
      effectivedate,
      enddate
    FROM {{ ref('DimCompany') }}
  ) dc 
  ON
    fws.conameorcik = dc.conameorcik 
    AND fws.effectivedate < dc.enddate
    AND fws.enddate > dc.effectivedate
) fws
WHERE effectivedate <> enddate