{{
    config(
        materialized = 'table'
    )
}}
SELECT
    p.agencyid,
    recdate.sk_dateid AS sk_recorddateid,
    origdate.sk_dateid AS sk_updatedateid,
    p.batchid,
    CASE WHEN c.customerid IS NOT NULL THEN 1 ELSE 0 END AS iscustomer,
    p.lastname,
    p.firstname,
    p.middleinitial,
    p.gender,
    p.addressline1,
    p.addressline2,
    p.postalcode,
    p.city,
    p.state,
    p.country,
    p.phone,
    p.income,
    p.numbercars,
    p.numberchildren,
    p.maritalstatus,
    p.age,
    p.creditrating,
    p.ownorrentflag,
    p.employer,
    p.numbercreditcards,
    p.networth,
    RTRIM(
        CASE
            WHEN
                p.networth > 1000000 OR p.income > 200000
                THEN 'HighValue+'
            ELSE ''
        END
        + CASE
            WHEN
                p.numberchildren > 3 OR p.numbercreditcards > 5
                THEN 'Expenses+'
            ELSE ''
        END
        + CASE WHEN p.age > 45 THEN 'Boomer+' ELSE '' END
        + CASE
            WHEN
                p.income < 50000 OR p.creditrating < 600 OR p.networth < 100000
                THEN 'MoneyAlert+'
            ELSE ''
        END
        + CASE
            WHEN
                p.numbercars > 3 OR p.numbercreditcards > 7
                THEN 'Spender+'
            ELSE ''
        END
        + CASE
            WHEN p.age < 25 AND p.networth > 1000000 THEN 'Inherited+' ELSE ''
        END,
        '+'
    ) AS marketingnameplate
FROM
    (
        SELECT *
        FROM
            (
                SELECT
                    *,
                    ROW_NUMBER()
                        OVER (PARTITION BY agencyid ORDER BY batchid DESC)
                    AS rownum
                FROM
                    (
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
                            MIN(batchid) AS batchid
                        FROM {{ ref('prospectraw') }}
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
                    ) AS t0
            ) AS t1
        WHERE t1.rownum = 1
    ) AS p
    INNER JOIN (
        SELECT
            d.sk_dateid,
            b.batchid
        FROM {{ ref('batchdate') }} AS b
            INNER JOIN {{ source('tpcdi', 'DimDate') }} AS d
                ON b.batchdate = d.datevalue
    ) AS recdate
        ON p.recordbatchid = recdate.batchid
    INNER JOIN (
        SELECT
            d.sk_dateid,
            b.batchid
        FROM {{ ref('batchdate') }} AS b
            INNER JOIN {{ source('tpcdi', 'DimDate') }} AS d
                ON b.batchdate = d.datevalue
    ) AS origdate
        ON p.batchid = origdate.batchid
    LEFT JOIN (
        SELECT
            customerid,
            lastname,
            firstname,
            addressline1,
            addressline2,
            postalcode
        FROM {{ ref('dimcustomerstg') }}
        WHERE iscurrent = 1
    ) AS c
        ON
            UPPER(p.lastname) = UPPER(c.lastname)
            AND UPPER(p.firstname) = UPPER(c.firstname)
            AND UPPER(p.addressline1) = UPPER(c.addressline1)
            AND UPPER(ISNULL(p.addressline2, ''))
            = UPPER(ISNULL(c.addressline2, ''))
            AND UPPER(p.postalcode) = UPPER(c.postalcode)
