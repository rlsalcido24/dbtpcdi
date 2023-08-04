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
        CASE
            WHEN networth > 1000000 OR income > 200000 THEN 'HighValue+' ELSE ''
        END
        + CASE WHEN numberchildren > 3 OR numbercreditcards > 5 THEN 'Expenses+' ELSE '' END
        + CASE WHEN age > 45 THEN 'Boomer+' ELSE '' END
        + CASE WHEN income < 50000 OR creditrating < 600 OR networth < 100000 THEN 'MoneyAlert+' ELSE '' END
        + CASE WHEN numbercars > 3 OR numbercreditcards > 7 THEN 'Spender+' ELSE '' END
        + CASE WHEN age < 25 AND networth > 1000000 THEN 'Inherited+' ELSE '' END,
        '+'
    ) marketingnameplate
FROM
    (
        SELECT *
        FROM
            (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY agencyid ORDER BY batchid DESC
                    ) AS rownum
                FROM
                    (
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
                    ) t0
            --QUALIFY ROW_NUMBER() OVER (PARTITION BY agencyid ORDER BY batchid DESC) = 1
            ) t1
        WHERE t1.rownum = 1
    ) p
    JOIN (
        SELECT
            sk_dateid,
            batchid
        FROM {{ ref('BatchDate') }} b
            JOIN {{ ref('DimDate') }} d
                ON b.batchdate = d.datevalue
    ) recdate
        ON p.recordbatchid = recdate.batchid
    JOIN (
        SELECT
            sk_dateid,
            batchid
        FROM {{ ref('BatchDate') }} b
            JOIN {{ ref('DimDate') }} d
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
        --WHERE iscurrent) c
        WHERE iscurrent = 1
    ) c
        ON
            UPPER(p.lastname) = UPPER(c.lastname)
            AND UPPER(p.firstname) = UPPER(c.firstname)
            AND UPPER(p.addressline1) = UPPER(c.addressline1)
            AND UPPER(ISNULL(p.addressline2, ''))
            = UPPER(ISNULL(c.addressline2, ''))
            AND UPPER(p.postalcode) = UPPER(c.postalcode)
