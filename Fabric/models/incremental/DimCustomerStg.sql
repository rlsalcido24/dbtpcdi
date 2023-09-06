{{
    config(
        materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='HASH(customerid)'
    )
}}

-- !!!!! IGNORE NULLS is not supported in Synapse !!!!!

SELECT * 
FROM (
  SELECT
    sk_customerid,
    customerid,
    --coalesce(taxid, last_value(taxid) IGNORE NULLS OVER (
    coalesce(taxid, last_value(taxid) OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) taxid,
    status,
    --coalesce(lastname, last_value(lastname) IGNORE NULLS OVER (
    coalesce(lastname, last_value(lastname) OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) lastname,
    --coalesce(firstname, last_value(firstname) IGNORE NULLS OVER (
    coalesce(firstname, last_value(firstname) OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) firstname,
    --coalesce(middleinitial, last_value(middleinitial) IGNORE NULLS OVER (
    coalesce(middleinitial, last_value(middleinitial) OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) middleinitial,
    --coalesce(gender, last_value(gender) IGNORE NULLS OVER (
    coalesce(gender, last_value(gender) OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) gender,
    --coalesce(tier, last_value(tier) IGNORE NULLS OVER (
    coalesce(tier, last_value(tier) OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) tier,
    --coalesce(dob, last_value(dob) IGNORE NULLS OVER (
    coalesce(dob, last_value(dob) OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) dob,
    --coalesce(addressline1, last_value(addressline1) IGNORE NULLS OVER (
    coalesce(addressline1, last_value(addressline1) OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) addressline1,
    --coalesce(addressline2, last_value(addressline2) IGNORE NULLS OVER (
    coalesce(addressline2, last_value(addressline2) OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) addressline2,
    --coalesce(postalcode, last_value(postalcode) IGNORE NULLS OVER (
    coalesce(postalcode, last_value(postalcode) OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) postalcode,
    --coalesce(CITY, last_value(CITY) IGNORE NULLS OVER (
    coalesce(city, last_value(city) OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) city,
    --coalesce(stateprov, last_value(stateprov) IGNORE NULLS OVER (
    coalesce(stateprov, last_value(stateprov) OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) stateprov,
    --coalesce(country, last_value(country) IGNORE NULLS OVER (
    coalesce(country, last_value(country) OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) country,
    --coalesce(phone1, last_value(phone1) IGNORE NULLS OVER (
    coalesce(phone1, last_value(phone1) OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) phone1,
    --coalesce(phone2, last_value(phone2) IGNORE NULLS OVER (
    coalesce(phone2, last_value(phone2) OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) phone2,
    --coalesce(phone3, last_value(phone3) IGNORE NULLS OVER (
    coalesce(phone3, last_value(phone3) OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) phone3,
    --coalesce(email1, last_value(email1) IGNORE NULLS OVER (
    coalesce(email1, last_value(email1) OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) email1,
    --coalesce(email2, last_value(email2) IGNORE NULLS OVER (
    coalesce(email2, last_value(email2) OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) email2,
    --coalesce(lcl_tx_id, last_value(lcl_tx_id) IGNORE NULLS OVER (
    coalesce(lcl_tx_id, last_value(lcl_tx_id) OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) LCL_TX_ID,
    --coalesce(nat_tx_id, last_value(nat_tx_id) IGNORE NULLS OVER (
    coalesce(nat_tx_id, last_value(nat_tx_id) OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) nat_tx_id,
    batchid,
    CASE WHEN lead(update_ts) OVER (PARTITION BY customerid ORDER BY update_ts) IS NOT NULL THEN 0 ELSE 1 END iscurrent,
    convert(date,update_ts) effectivedate,
    coalesce(lead(convert(date,update_ts)) OVER (PARTITION BY customerid ORDER BY update_ts), convert(date,'9999-12-31')) enddate
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
      update_ts,
      concat(customerid, '-', update_ts) as sk_customerid
    FROM {{ ref('CustomerMgmt') }} c
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
        WHEN c_local_1 IS NULL THEN c_local_1
        ELSE concat(
          case when c_ctry_1 is not null then '+' + c_ctry_1 + ' ' else '' end,
          case when c_area_1 is not null then '(' + c_area_1 + ') ' else '' end,
          c_local_1,
          isnull(c_ext_1,'')
        ) 
        END as phone1,
      CASE
        WHEN c_local_2 IS NULL THEN c_local_2
        ELSE concat(
          case when c_ctry_2 is not null then '+' + c_ctry_2 + ' ' else '' end,
          case when c_area_2 is not null then '(' + c_area_2 + ') ' else '' end,
          c_local_2,
          isnull(c_ext_2,'')
        ) 
        END as phone2,
      CASE
         WHEN c_local_3 IS NULL THEN c_local_3
        ELSE concat(
          case when c_ctry_3 is not null then '+' + c_ctry_3 + ' ' else '' end,
          case when c_area_3 is not null then '(' + c_area_3 + ') ' else '' end,
          c_local_3,
          isnull(c_ext_3,'')
        ) 
        END as phone3,
      nullif(c.email1, '') email1,
      nullif(c.email2, '') email2,
      c.lcl_tx_id, 
      c.nat_tx_id,
      c.batchid,
      convert(datetime2,bd.batchdate) update_ts,
      concat(c.customerid, '-', convert(datetime2,bd.batchdate)) as sk_customerid
    FROM {{ ref('CustomerIncremental') }} c
    JOIN {{ ref('BatchDate') }} bd
      ON c.batchid = bd.batchid
    JOIN {{ ref('StatusType') }} s 
      ON c.status = s.st_id
  ) c
) T

