{{
    config(
        materialized = 'table'
    )
}}
SELECT
    agencyid,
    recdate.sk_dateid sk_recorddateid,
    origdate.sk_dateid sk_updatedateid,
    p.batchid,
    NVL2(c.customerid, TRUE, FALSE) iscustomer,
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
    IFF(
        IFF(networth > 1000000 OR income > 200000, 'HighValue+', '')
        || IFF(numberchildren > 3 OR numbercreditcards > 5, 'Expenses+', '')
        || IFF(age > 45, 'Boomer+', '')
        || IFF(
            income < 50000 OR creditrating < 600 OR networth < 100000,
            'MoneyAlert+',
            ''
        )
        || IFF(numbercars > 3 OR numbercreditcards > 7, 'Spender+', '')
        || IFF(age < 25 AND networth > 1000000, 'Inherited+', '') IS NOT NULL,
        LEFT(
            IFF(networth > 1000000 OR income > 200000, 'HighValue+', '')
            || IFF(numberchildren > 3 OR numbercreditcards > 5, 'Expenses+', '')
            || IFF(age > 45, 'Boomer+', '')
            || IFF(
                income < 50000 OR creditrating < 600 OR networth < 100000,
                'MoneyAlert+',
                ''
            )
            || IFF(numbercars > 3 OR numbercreditcards > 7, 'Spender+', '')
            || IFF(age < 25 AND networth > 1000000, 'Inherited+', ''),
            LENGTH(
                IFF(networth > 1000000 OR income > 200000, 'HighValue+', '')
                || IFF(
                    numberchildren > 3 OR numbercreditcards > 5, 'Expenses+', ''
                )
                || IFF(age > 45, 'Boomer+', '')
                || IFF(
                    income < 50000 OR creditrating < 600 OR networth < 100000,
                    'MoneyAlert+',
                    ''
                )
                || IFF(numbercars > 3 OR numbercreditcards > 7, 'Spender+', '')
                || IFF(age < 25 AND networth > 1000000, 'Inherited+', '')
            )
            - 1
        ),
        NULL
    ) marketingnameplate
FROM (
    SELECT *
    FROM (
        SELECT
            agencyid,
            MAX(batchid) recordbatchid,
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
            MIN(batchid) batchid
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
    )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY agencyid ORDER BY batchid DESC) = 1
) p
    JOIN (
        SELECT
            sk_dateid,
            batchid
        FROM {{ ref('BatchDate') }} b
            JOIN {{ source('tpcdi', 'DimDate') }} d
                ON b.batchdate = d.datevalue
    ) recdate
        ON p.recordbatchid = recdate.batchid
    JOIN (
        SELECT
            sk_dateid,
            batchid
        FROM {{ ref('BatchDate') }} b
            JOIN {{ source('tpcdi', 'DimDate') }} d
                ON b.batchdate = d.datevalue
    ) origdate
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
    ) c
        ON
            UPPER(p.lastname) = UPPER(c.lastname)
            AND UPPER(p.firstname) = UPPER(c.firstname)
            AND UPPER(p.addressline1) = UPPER(c.addressline1)
            AND UPPER(COALESCE(p.addressline2, ''))
            = UPPER(COALESCE(c.addressline2, ''))
            AND UPPER(p.postalcode) = UPPER(c.postalcode)
