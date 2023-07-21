{{
    config(
        materialized = 'table'
    )
}}
  SELECT
    sk_customerid,
    customerid,
    LAST_VALUE(taxid) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS taxid,
    status,
    LAST_VALUE(lastname) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS lastname,
    LAST_VALUE(firstname) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS firstname,
    LAST_VALUE(middleinitial) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS middleinitial,
    LAST_VALUE(gender) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS gender,
    LAST_VALUE(tier) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS tier,
    LAST_VALUE(dob) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS dob,
    LAST_VALUE(addressline1) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS addressline1,
    LAST_VALUE(addressline2) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS addressline2,
    LAST_VALUE(postalcode) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS postalcode,
    LAST_VALUE(CITY) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS CITY,
    LAST_VALUE(stateprov) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS stateprov,
    LAST_VALUE(country) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS country,
    LAST_VALUE(phone1) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS phone1,
    LAST_VALUE(phone2) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS phone2,
    LAST_VALUE(phone3) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS phone3,
    LAST_VALUE(email1) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS email1,
    LAST_VALUE(email2) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS email2,
    LAST_VALUE(LCL_TX_ID) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS LCL_TX_ID,
    LAST_VALUE(NAT_TX_ID) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS NAT_TX_ID,
    batchid,
    IF(LEAD(update_ts) OVER (PARTITION BY customerid ORDER BY update_ts) IS NOT NULL, FALSE, TRUE) AS iscurrent,
    DATE(update_ts) AS effectivedate,
    COALESCE(LEAD(DATE(update_ts)) OVER (PARTITION BY customerid ORDER BY update_ts), DATE('9999-12-31')) AS enddate
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
      CITY,
      stateprov,
      country,
      phone1,
      phone2,
      phone3,
      email1,
      email2,
      LCL_TX_ID,
      NAT_TX_ID,
      1 batchid,
      update_ts,
      CONCAT(customerid, '-', update_ts) AS sk_customerid
    FROM
      {{ ref('CustomerMgmtView') }} c
    WHERE
      ActionType IN ('NEW', 'INACT', 'UPDCUST')
    UNION ALL
   SELECT
    c.customerid,
    nullif(c.taxid, '') AS taxid,
    nullif(s.st_name, '') AS status,
    nullif(c.lastname, '') AS lastname,
    nullif(c.firstname, '') AS firstname,
    nullif(c.middleinitial, '') AS middleinitial,
    gender,
    c.tier,
    c.dob,
    nullif(c.addressline1, '') AS addressline1,
    nullif(c.addressline2, '') AS addressline2,
    nullif(c.postalcode, '') AS postalcode,
    nullif(c.city, '') AS city,
    nullif(c.stateprov, '') AS stateprov,
    nullif(c.country, '') AS country,
    CONCAT(
        IF(c_local_1 IS NULL, c_local_1,
        CONCAT(
            IF(c_ctry_1 IS NOT NULL, CONCAT('+', c_ctry_1, ' '), ''),
            IF(c_area_1 IS NOT NULL, CONCAT('(', c_area_1, ') '), ''),
            c_local_1,
            IF(c_ext_1 IS NOT NULL, c_ext_1, '')
        ))
    ) AS phone1,
    CONCAT(
        IF(c_local_2 IS NULL, c_local_2,
        CONCAT(
            IF(c_ctry_2 IS NOT NULL, CONCAT('+', c_ctry_2, ' '), ''),
            IF(c_area_2 IS NOT NULL, CONCAT('(', c_area_2, ') '), ''),
            c_local_2,
            IF(c_ext_2 IS NOT NULL, c_ext_2, '')
        ))
    ) AS phone2,
    CONCAT(
        IF(c_local_3 IS NULL, c_local_3,
        CONCAT(
            IF(c_ctry_3 IS NOT NULL, CONCAT('+', c_ctry_3, ' '), ''),
            IF(c_area_3 IS NOT NULL, CONCAT('(', c_area_3, ') '), ''),
            c_local_3,
            IF(c_ext_3 IS NOT NULL, c_ext_3, '')
        ))
    ) AS phone3,
    nullif(c.email1, '') AS email1,
    nullif(c.email2, '') AS email2,
    c.LCL_TX_ID,
    c.NAT_TX_ID,
    c.batchid,
    TIMESTAMP(bd.batchdate) AS update_ts,
    CONCAT(c.customerid, '-', CAST(bd.batchdate AS STRING)) AS sk_customerid
    FROM
    {{ ref('CustomerIncremental') }} AS c
    JOIN
     {{ ref('BatchDate') }}  AS bd
    ON c.batchid = bd.batchid
    JOIN
    {{source(var('benchmark'),'StatusType') }} AS s
    ON c.status = s.st_id
  ) c