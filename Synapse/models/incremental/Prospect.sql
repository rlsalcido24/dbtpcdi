{{
    config(
        materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='HASH(agencyid)'
    )
}}


SELECT 
    agencyid,
    recdate.sk_dateid sk_recorddateid,
    origdate.sk_dateid sk_updatedateid,
    p.batchid,
    CASE WHEN c.customerid IS NOT NULL THEN 1 ELSE 0 END iscustomer,
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
    RTRIM(
        CASE WHEN networth > 1000000 or income > 200000 THEN 'HighValue+' ELSE '' END +
        CASE WHEN numberchildren > 3 or numbercreditcards > 5 THEN 'Expenses+' ELSE '' END +
        CASE WHEN age > 45 THEN 'Boomer+' ELSE '' END +
        CASE WHEN income < 50000 or creditrating < 600 or networth < 100000 THEN 'MoneyAlert+' ELSE '' END +
        CASE WHEN numbercars > 3 or numbercreditcards > 7 THEN 'Spender+' ELSE '' END +
        CASE WHEN age < 25 and networth > 1000000 THEN 'Inherited+' ELSE '' END,
        '+'
    ) marketingnameplate    
FROM 
(
    SELECT *
    FROM
    (
        SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY agencyid ORDER BY batchid DESC) as rownum
        FROM 
        (
            SELECT
                agencyid,
                max(batchid) recordbatchid,
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
                networth,
                min(batchid) batchid
            FROM {{ ref('ProspectRaw') }} p
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
        ) T0
        --QUALIFY ROW_NUMBER() OVER (PARTITION BY agencyid ORDER BY batchid DESC) = 1
    ) T1 
    WHERE T1.rownum=1
) p
JOIN (
  SELECT 
    sk_dateid,
    batchid
  FROM {{ ref('BatchDate') }} b 
  JOIN {{ref('DimDate') }} d 
    ON b.batchdate = d.datevalue) recdate
  ON p.recordbatchid = recdate.batchid
JOIN (
  SELECT 
    sk_dateid,
    batchid
  FROM {{ ref('BatchDate') }} b 
  JOIN {{ ref('DimDate') }} d 
    ON b.batchdate = d.datevalue) origdate
  ON p.batchid = origdate.batchid
LEFT JOIN (
  SELECT 
    customerid,
    lastname,
    firstname,
    addressline1,
    addressline2,
    postalcode
  FROM {{ ref('DimCustomerStg') }}
  --WHERE iscurrent) c
  WHERE iscurrent=1) c
  ON 
    upper(p.LastName) = upper(c.lastname)
    and upper(p.FirstName) = upper(c.firstname)
    and upper(p.AddressLine1) = upper(c.addressline1)
    and upper(isnull(p.addressline2, '')) = upper(isnull(c.addressline2, ''))
    and upper(p.PostalCode) = upper(c.postalcode)