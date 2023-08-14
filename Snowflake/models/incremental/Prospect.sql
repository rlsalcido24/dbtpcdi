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
    NVL2(c.customerid, TRUE, FALSE) AS iscustomer,
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
    IFF(
        IFF(p.networth > 1000000 OR p.income > 200000, 'HighValue+', '')
        || IFF(p.numberchildren > 3 OR p.numbercreditcards > 5, 'Expenses+', '')
        || IFF(p.age > 45, 'Boomer+', '')
        || IFF(
            p.income < 50000 OR p.creditrating < 600 OR p.networth < 100000,
            'MoneyAlert+',
            ''
        )
        || IFF(p.numbercars > 3 OR p.numbercreditcards > 7, 'Spender+', '')
        || IFF(
            p.age < 25 AND p.networth > 1000000, 'Inherited+', ''
        ) IS NOT NULL,
        LEFT(
            IFF(p.networth > 1000000 OR p.income > 200000, 'HighValue+', '')
            || IFF(
                p.numberchildren > 3 OR p.numbercreditcards > 5, 'Expenses+', ''
            )
            || IFF(p.age > 45, 'Boomer+', '')
            || IFF(
                p.income < 50000 OR p.creditrating < 600 OR p.networth < 100000,
                'MoneyAlert+',
                ''
            )
            || IFF(p.numbercars > 3 OR p.numbercreditcards > 7, 'Spender+', '')
            || IFF(p.age < 25 AND p.networth > 1000000, 'Inherited+', ''),
            LENGTH(
                IFF(p.networth > 1000000 OR p.income > 200000, 'HighValue+', '')
                || IFF(
                    p.numberchildren > 3 OR p.numbercreditcards > 5,
                    'Expenses+',
                    ''
                )
                || IFF(p.age > 45, 'Boomer+', '')
                || IFF(
                    p.income < 50000
                    OR p.creditrating < 600
                    OR p.networth < 100000,
                    'MoneyAlert+',
                    ''
                )
                || IFF(
                    p.numbercars > 3 OR p.numbercreditcards > 7, 'Spender+', ''
                )
                || IFF(p.age < 25 AND p.networth > 1000000, 'Inherited+', '')
            )
            - 1
        ),
        NULL
    ) AS marketingnameplate
FROM (
    SELECT *
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
        FROM {{ ref('ProspectRaw') }}
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
    )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY agencyid ORDER BY batchid DESC) = 1
) AS p
    INNER JOIN (
        SELECT
            d.sk_dateid,
            b.batchid
        FROM {{ ref('BatchDate') }} AS b
            INNER JOIN {{ source('tpcdi', 'DimDate') }} AS d
                ON b.batchdate = d.datevalue
    ) AS recdate
        ON p.recordbatchid = recdate.batchid
    INNER JOIN (
        SELECT
            d.sk_dateid,
            b.batchid
        FROM {{ ref('BatchDate') }} AS b
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
        FROM {{ ref('DimCustomerStg') }}
        WHERE iscurrent
    ) AS c
        ON
            UPPER(p.lastname) = UPPER(c.lastname)
            AND UPPER(p.firstname) = UPPER(c.firstname)
            AND UPPER(p.addressline1) = UPPER(c.addressline1)
            AND UPPER(COALESCE(p.addressline2, ''))
            = UPPER(COALESCE(c.addressline2, ''))
            AND UPPER(p.postalcode) = UPPER(c.postalcode)
