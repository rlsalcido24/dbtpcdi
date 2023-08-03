{{
    config(
        materialized = 'table'
    )
}}
        --,index='CLUSTERED COLUMNSTORE INDEX'
        --,dist='REPLICATE'
        
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
  --if(enddate = date('9999-12-31'), True, False) iscurrent,
  CASE WHEN enddate = '9999-12-31'::DATE  THEN 1 ELSE 0 END iscurrent,
  1 batchid,
  effectivedate,
  concat(concat(exchangeid, '-'), effectivedate) as sk_securityid, 
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
    --if(fws.effectivedate < dc.effectivedate, dc.effectivedate, fws.effectivedate) effectivedate,
    CASE WHEN fws.effectivedate < dc.effectivedate THEN dc.effectivedate ELSE fws.effectivedate END effectivedate,
    --if(fws.enddate > dc.enddate, dc.enddate, fws.enddate) enddate
    CASE WHEN fws.enddate > dc.enddate THEN dc.enddate ELSE fws.enddate END enddate
  FROM (
    SELECT 
      --fws.* except(Status, conameorcik),
      fws.effectivedate,
      fws.Symbol,
      fws.issue,
      fws.Name,
      fws.exchangeid,
      fws.sharesoutstanding,
      fws.firsttrade,
      fws.firsttradeonexchange,
      fws.Dividend,
      
      COALESCE(CAST(
        case
when conameorcik SIMILAR TO '[0-9]+(.[0-9][0-9])?' then conameorcik::integer
else null
end as VARCHAR)
, conameorcik) conameorcik,
      --nvl(string(cast(conameorcik as bigint)), conameorcik) conameorcik,
      --ISNULL( convert(varchar,try_cast(conameorcik as bigint)), conameorcik) conameorcik,
      s.ST_NAME as status,
      coalesce(
        lead(effectivedate) OVER (
          PARTITION BY symbol
          ORDER BY effectivedate),
        --date('9999-12-31')
        TO_DATE('9999-12-31','yyyyMMdd')
      ) enddate
    FROM (
      SELECT
        to_timestamp(substring(value, 1, 15),'YYYYMMDD-HH24MISS') AS effectivedate,
--        convert(date,convert(datetime2, substring([value],1,8)+' '+substring([value],10,2)+':'+substring([value],12,2)+':'+substring([value],14,2), 112)) AS effectivedate,
        trim(substring(value, 19, 15)) AS Symbol,
        trim(substring(value, 34, 6)) AS issue,
        trim(substring(value, 40, 4)) AS Status,
        trim(substring(value, 44, 70)) AS Name,
        trim(substring(value, 114, 6)) AS exchangeid,
        cast(substring(value, 120, 13) as BIGINT) AS sharesoutstanding,
        to_date(substring(value, 133, 8), 'yyyyMMdd') AS firsttrade,
--        convert(date,substring(value, 133, 8), 112) AS firsttrade,
        to_date(substring(value, 141, 8), 'yyyyMMdd') AS firsttradeonexchange,
--        convert(date,substring(value, 141, 8), 112) AS firsttradeonexchange,
        cast(substring(value, 149, 12) AS FLOAT) AS Dividend,
        trim(substring(value, 161, 60)) AS conameorcik
      FROM {{ ref('finwire') }}
      --FROM stg.FinWire
      WHERE rectype = 'SEC'
      ) fws
    JOIN {{ source('tpcdi', 'StatusType') }} s
      ON s.ST_ID = fws.status
    ) fws
  JOIN (
    SELECT 
      sk_companyid,
      name conameorcik,
      EffectiveDate,
      EndDate
    FROM {{ ref('dimcompany') }}
    UNION ALL
    SELECT 
      sk_companyid,
      --cast(companyid as string) conameorcik,
      cast(companyid as varchar) conameorcik,
      EffectiveDate,
      EndDate
    FROM {{ ref('dimcompany') }}
  ) dc 
  ON
    fws.conameorcik = dc.conameorcik 
    AND fws.EffectiveDate < dc.EndDate
    AND fws.EndDate > dc.EffectiveDate
) fws
--WHERE effectivedate != enddate
WHERE effectivedate <> enddate