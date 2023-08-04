{{
    config(
        materialized = 'table'
    )
}}
SELECT
    agencyid,
    recdate.sk_dateid AS sk_recorddateid,
    origdate.sk_dateid AS sk_updatedateid,
    p.batchid,
    IF(c.customerid IS NOT NULL, TRUE, FALSE) AS iscustomer,
    p.lastname,
    p.firstname,
    p.middleinitial,
    p.gender,
    p.addressline1,
    p.addressline2,
    p.postalcode,
    city,
    state,
    country,
    phone,
    income,
    numbercars,
    numberchildren,
    maritalstatus,
    age,
    creditrating,
    ownorrentflag,
    employer,
    numbercreditcards,
    networth,
    IF(
      CONCAT(
        IF(networth > 1000000 OR income > 200000, 'HighValue+', ''),
        IF(numberchildren > 3 OR numbercreditcards > 5, 'Expenses+', ''),
        IF(age > 45, 'Boomer+', ''),
        IF(income < 50000 OR creditrating < 600 OR networth < 100000, 'MoneyAlert+', ''),
        IF(numbercars > 3 OR numbercreditcards > 7, 'Spender+', ''),
        IF(age < 25 AND networth > 1000000, 'Inherited+', '')
      ) != '', -- IS NOT NULL
      SUBSTR(
        CONCAT(
          IF(networth > 1000000 OR income > 200000, 'HighValue+', ''),
          IF(numberchildren > 3 OR numbercreditcards > 5, 'Expenses+', ''),
          IF(age > 45, 'Boomer+', ''),
          IF(income < 50000 OR creditrating < 600 OR networth < 100000, 'MoneyAlert+', ''),
          IF(numbercars > 3 OR numbercreditcards > 7, 'Spender+', ''),
          IF(age < 25 AND networth > 1000000, 'Inherited+', '')
        ),
        1,
        LENGTH(
          CONCAT(
            IF(networth > 1000000 OR income > 200000, 'HighValue+', ''),
            IF(numberchildren > 3 OR numbercreditcards > 5, 'Expenses+', ''),
            IF(age > 45, 'Boomer+', ''),
            IF(income < 50000 OR creditrating < 600 OR networth < 100000, 'MoneyAlert+', ''),
            IF(numbercars > 3 OR numbercreditcards > 7, 'Spender+', ''),
            IF(age < 25 AND networth > 1000000, 'Inherited+', '')
          )
        ) - 1
      ),
      NULL
    ) AS marketingnameplate
  FROM (
    SELECT
      agencyid,
      MAX(batchid) AS recordbatchid,
      lastname,
      firstname,
      middleinitial,
      gender,
      addressline1,
      addressline2,
      postalcode,
      city,
      state,
      country,
      phone,
      CAST(income AS INT64) AS income,
      numbercars,
      numberchildren,
      maritalstatus,
      age,
      creditrating,
      ownorrentflag,
      employer,
      numbercreditcards,
      CAST(networth AS INT64) AS networth,
      MIN(batchid) AS batchid
    FROM
     {{ref('ProspectRaw')}} p
    GROUP BY
      agencyid,
      lastname,
      firstname,
      middleinitial,
      gender,
      addressline1,
      addressline2,
      postalcode,
      city,
      state,
      country,
      phone,
      income,
      numbercars,
      numberchildren,
      maritalstatus,
      age,
      creditrating,
      ownorrentflag,
      employer,
      numbercreditcards,
      networth
  ) p
  JOIN (
    SELECT
      sk_dateid,
      batchid
    FROM
      {{ref('BatchDate')}} b
    JOIN
     {{source(var('benchmark'),'DimDate')}} d
    ON
      b.batchdate = d.datevalue
  ) recdate
  ON
    p.recordbatchid = recdate.batchid
  JOIN (
    SELECT
      sk_dateid,
      batchid
    FROM
       {{ref('BatchDate')}} b
    JOIN
      {{source(var('benchmark'),'DimDate')}} d
    ON
      b.batchdate = d.datevalue
  ) origdate
  ON
    p.batchid = origdate.batchid
  LEFT JOIN (
    SELECT
      customerid,
      lastname,
      firstname,
      addressline1,
      addressline2,
      postalcode
    FROM
      {{ref('DimCustomerStg')}}
    WHERE
      iscurrent
  ) c
  ON
    UPPER(p.lastname) = UPPER(c.lastname)
    AND UPPER(p.firstname) = UPPER(c.firstname)
    AND UPPER(p.addressline1) = UPPER(c.addressline1)
    AND UPPER(COALESCE(p.addressline2, '')) = UPPER(COALESCE(c.addressline2, ''))
    AND UPPER(p.postalcode) = UPPER(c.postalcode)

