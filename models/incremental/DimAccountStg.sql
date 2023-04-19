{{
    config(
        materialized = 'incremental'
    )
}}

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
    update_ts
  FROM STREAM(${staging_db}.CustomerMgmt) c
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
      WHEN isnull(c_local_1) then c_local_1
      ELSE concat(
        nvl2(c_ctry_1, '+' || c_ctry_1 || ' ', ''),
        nvl2(c_area_1, '(' || c_area_1 || ') ', ''),
        c_local_1,
        nvl(c_ext_1, '')) END as phone1,
    CASE
      WHEN isnull(c_local_2) then c_local_2
      ELSE concat(
        nvl2(c_ctry_2, '+' || c_ctry_2 || ' ', ''),
        nvl2(c_area_2, '(' || c_area_2 || ') ', ''),
        c_local_2,
        nvl(c_ext_2, '')) END as phone2,
    CASE
      WHEN isnull(c_local_3) then c_local_3
      ELSE concat(
        nvl2(c_ctry_3, '+' || c_ctry_3 || ' ', ''),
        nvl2(c_area_3, '(' || c_area_3 || ') ', ''),
        c_local_3,
        nvl(c_ext_3, '')) END as phone3,
    nullif(c.email1, '') email1,
    nullif(c.email2, '') email2,
    c.LCL_TX_ID, 
    c.NAT_TX_ID,
    c.batchid,
    timestamp(bd.batchdate) update_ts
  FROM STREAM(LIVE.CustomerIncremental) c
  JOIN LIVE.BatchDate bd
    ON c.batchid = bd.batchid
  JOIN LIVE.StatusType s 
    ON c.status = s.st_id
)

KEYS (customerid)
IGNORE NULL UPDATES
SEQUENCE BY update_ts
COLUMNS * EXCEPT (update_ts)
STORED AS SCD TYPE 2;
