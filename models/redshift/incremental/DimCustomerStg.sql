{{
    config(
        materialized = 'table'
    )
}}
        --,index='HEAP'
        --,dist='ROUND_ROBIN'
SELECT * 
FROM (
    SELECT
    sk_customerid,
    customerid,
    --coalesce(taxid, last_value(taxid) IGNORE NULLS OVER (
    coalesce(taxid, last_value(taxid IGNORE NULLS) OVER (
        PARTITION BY customerid
        ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) taxid,
    status,
    --coalesce(lastname, last_value(lastname) IGNORE NULLS OVER (
    coalesce(lastname, last_value(lastname IGNORE NULLS) OVER (
        PARTITION BY customerid
        ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) lastname,
    --coalesce(firstname, last_value(firstname) IGNORE NULLS OVER (
    coalesce(firstname, last_value(firstname IGNORE NULLS) OVER (
        PARTITION BY customerid
        ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) firstname,
    --coalesce(middleinitial, last_value(middleinitial) IGNORE NULLS OVER (
    coalesce(middleinitial, last_value(middleinitial IGNORE NULLS) OVER (
        PARTITION BY customerid
        ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) middleinitial,
    --coalesce(gender, last_value(gender) IGNORE NULLS OVER (
    coalesce(gender, last_value(gender IGNORE NULLS) OVER (
        PARTITION BY customerid
        ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) gender,
    --coalesce(tier, last_value(tier) IGNORE NULLS OVER (
    coalesce(tier, last_value(tier IGNORE NULLS) OVER (
        PARTITION BY customerid
        ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) tier,
    --coalesce(dob, last_value(dob) IGNORE NULLS OVER (
    coalesce(dob, last_value(dob IGNORE NULLS) OVER (
        PARTITION BY customerid
        ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) dob,
    --coalesce(addressline1, last_value(addressline1) IGNORE NULLS OVER (
    coalesce(addressline1, last_value(addressline1 IGNORE NULLS) OVER (
        PARTITION BY customerid
        ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) addressline1,
    --coalesce(addressline2, last_value(addressline2) IGNORE NULLS OVER (
    coalesce(addressline2, last_value(addressline2 IGNORE NULLS) OVER (
        PARTITION BY customerid
        ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) addressline2,
    --coalesce(postalcode, last_value(postalcode) IGNORE NULLS OVER (
    coalesce(postalcode, last_value(postalcode IGNORE NULLS) OVER (
        PARTITION BY customerid
        ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) postalcode,
    --coalesce(CITY, last_value(CITY) IGNORE NULLS OVER (
    coalesce(CITY, last_value(CITY IGNORE NULLS) OVER (
        PARTITION BY customerid
        ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) CITY,
    --coalesce(stateprov, last_value(stateprov) IGNORE NULLS OVER (
    coalesce(stateprov, last_value(stateprov IGNORE NULLS) OVER (
        PARTITION BY customerid
        ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) stateprov,
    --coalesce(country, last_value(country) IGNORE NULLS OVER (
    coalesce(country, last_value(country IGNORE NULLS) OVER (
        PARTITION BY customerid
        ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) country,
    --coalesce(phone1, last_value(phone1) IGNORE NULLS OVER (
    coalesce(phone1, last_value(phone1 IGNORE NULLS) OVER (
        PARTITION BY customerid
        ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) phone1,
    --coalesce(phone2, last_value(phone2) IGNORE NULLS OVER (
    coalesce(phone2, last_value(phone2 IGNORE NULLS) OVER (
        PARTITION BY customerid
        ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) phone2,
    --coalesce(phone3, last_value(phone3) IGNORE NULLS OVER (
    coalesce(phone3, last_value(phone3 IGNORE NULLS) OVER (
        PARTITION BY customerid
        ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) phone3,
    --coalesce(email1, last_value(email1) IGNORE NULLS OVER (
    coalesce(email1, last_value(email1 IGNORE NULLS) OVER (
        PARTITION BY customerid
        ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) email1,
    --coalesce(email2, last_value(email2) IGNORE NULLS OVER (
    coalesce(email2, last_value(email2 IGNORE NULLS) OVER (
        PARTITION BY customerid
        ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) email2,
    --coalesce(LCL_TX_ID, last_value(LCL_TX_ID) IGNORE NULLS OVER (
    coalesce(LCL_TX_ID, last_value(LCL_TX_ID IGNORE NULLS) OVER (
        PARTITION BY customerid
        ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) LCL_TX_ID,
    --coalesce(NAT_TX_ID, last_value(NAT_TX_ID) IGNORE NULLS OVER (
    coalesce(NAT_TX_ID, last_value(NAT_TX_ID IGNORE NULLS) OVER (
        PARTITION BY customerid
        ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) NAT_TX_ID,
    batchid,
    --nvl2(lead(update_ts) OVER (PARTITION BY customerid ORDER BY update_ts), false, true) iscurrent,
    CASE WHEN lead(update_ts) OVER (PARTITION BY customerid ORDER BY update_ts) IS NOT NULL THEN 0 ELSE 1 END iscurrent,
    --date(update_ts) effectivedate,
     CAST(update_ts AS timestamp) effectivedate,
    --coalesce(lead(date(update_ts)) OVER (PARTITION BY customerid ORDER BY update_ts), date('9999-12-31')) enddate
    coalesce(lead(CAST(update_ts AS timestamp)) OVER (PARTITION BY customerid ORDER BY update_ts), CAST('9999-12-31' AS date)) enddate
  FROM (
    SELECT

      customerid,
      taxid,
      status,
      lastname,
      firstname,
      middleinitial,
      gender,
      tier,
      dob,
      addressline1,
      addressline2,
      postalcode,
      city,
      stateprov,
      country,
      phone1,
      phone2,
      phone3,
      email1,
      email2,
      lcl_tx_id,
      nat_tx_id,
      1 batchid,
      to_timestamp(update_ts, 'YYYY-MM-DD HH24:MI:SS') AS update_ts,
      concat(concat(customerid, '-'), update_ts) as sk_customerid
    FROM {{ ref('customermgmtview') }} c
--    FROM stg.CustomerMgmt c
    WHERE ActionType in ('NEW', 'INACT', 'UPDCUST')
    UNION ALL
    SELECT

      c.customerid,
      nullif(c.taxid, '') taxid,
      nullif(s.st_name, '') as status,
      nullif(c.lastname, '') lastname,
      nullif(c.firstname, '') firstname,
      nullif(c.middleinitial, '') middleinitial,
      gender,
      c.tier,
      c.dob,
      nullif(c.addressline1, '') addressline1,
      nullif(c.addressline2, '') addressline2,
      nullif(c.postalcode, '') postalcode,
      nullif(c.city, '') city,
      nullif(c.stateprov, '') stateprov,
      nullif(c.country, '') country,
      CASE
        --WHEN isnull(c_local_1) then c_local_1
        WHEN c_local_1 IS NULL THEN c_local_1
        ELSE 
                concat(concat(concat(
          case when c_ctry_1 is not null then concat(concat('+' , c_ctry_1) , ' ') else '' end,
          case when c_area_1 is not null then concat(concat('(' , c_area_1) , ') ') else '' end),
          c_local_1),
          coalesce(c_ext_1,'')
        ) 

        END as phone1,
      CASE
        --WHEN isnull(c_local_2) then c_local_2
        WHEN c_local_2 IS NULL THEN c_local_2
        ELSE 
                concat(concat(concat(
          case when c_ctry_2 is not null then concat(concat('+' , c_ctry_2) , ' ') else '' end,
          case when c_area_2 is not null then concat(concat('(' , c_area_2) , ') ') else '' end),
          c_local_2),
          coalesce(c_ext_2,'')
        ) 
        END as phone2,
      CASE
        --WHEN isnull(c_local_3) then c_local_3
         WHEN c_local_3 IS NULL THEN c_local_3
        ELSE 
            concat(concat(concat(
          case when c_ctry_3 is not null then concat(concat('+' , c_ctry_3) , ' ') else '' end,
          case when c_area_3 is not null then concat(concat('(' , c_area_3) , ') ') else '' end),
          c_local_3),
          coalesce(c_ext_3,'')
        ) 


        END as phone3,
      nullif(c.email1, '') email1,
      nullif(c.email2, '') email2,
      c.LCL_TX_ID, 
      c.NAT_TX_ID,
      c.batchid,
      --timestamp(bd.batchdate) update_ts,
      to_timestamp(bd.batchdate, 'YYYY-MM-DD HH24:MI:SS') AS update_ts,
      --concat(c.customerid, '-', update_ts) as sk_customerid
      concat(concat(c.customerid, '-'), bd.batchdate) as sk_customerid
    FROM {{ ref('customerincremental') }} c
--    FROM stg.CustomerIncremental c
    JOIN {{ ref('batchdate') }} bd
--    JOIN prd.BatchDate bd
      ON c.batchid = bd.batchid
    JOIN {{ source('tpcdi', 'StatusType') }} s 
--    JOIN sf10.StatusType s 
      ON c.status = s.st_id
  ) c
) T

