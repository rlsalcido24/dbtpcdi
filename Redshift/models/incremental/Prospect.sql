{{
    config(
        materialized = 'table'
    )
}}
--,index='CLUSTERED COLUMNSTORE INDEX'
--,dist='HASH(agencyid)'
SELECT
    p.agencyid,
    recdate.sk_dateid AS sk_recorddateid,
    origdate.sk_dateid AS sk_updatedateid,
    p.batchid,
    --nvl2(c.customerid, True, False) iscustomer,
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
    /*iff(
        iff(networth > 1000000 or income > 200000, 'HighValue+','') ||
        iff(numberchildren > 3 or numbercreditcards > 5,'Expenses+','') ||
        iff(age > 45, 'Boomer+', '') ||
        iff(income < 50000 or creditrating < 600 or networth < 100000, 'MoneyAlert+','') || -- noqa: LT05
        iff(numbercars > 3 or numbercreditcards > 7, 'Spender+','') ||
        iff(age < 25 and networth > 1000000, 'Inherited+','') IS NOT NULL,
    left(
        iff(networth > 1000000 or income > 200000,'HighValue+','') ||
        iff(numberchildren > 3 or numbercreditcards > 5,'Expenses+','') ||
        iff(age > 45, 'Boomer+', '') ||
        iff(income < 50000 or creditrating < 600 or networth < 100000, 'MoneyAlert+','') || -- noqa: LT05
        iff(numbercars > 3 or numbercreditcards > 7, 'Spender+','') ||
        iff(age < 25 and networth > 1000000, 'Inherited+',''),
        length(
        iff(networth > 1000000 or income > 200000,'HighValue+','') ||
        iff(numberchildren > 3 or numbercreditcards > 5,'Expenses+','') || -- noqa: LT05
        iff(age > 45, 'Boomer+', '') ||
        iff(income < 50000 or creditrating < 600 or networth < 100000, 'MoneyAlert+','') || -- noqa: LT05
        iff(numbercars > 3 or numbercreditcards > 7, 'Spender+','') ||
        iff(age < 25 and networth > 1000000, 'Inherited+',''))
        -1),
    NULL) marketingnameplate,*/
    /*CASE
        WHEN
            (
                CASE WHEN networth > 1000000 or income > 200000 THEN 'HighValue+' ELSE '' END + -- noqa: LT05
                CASE WHEN numberchildren > 3 or numbercreditcards > 5 THEN 'Expenses+' ELSE '' END + -- noqa: LT05
                CASE WHEN age > 45 THEN 'Boomer+' ELSE '' END +
                CASE WHEN income < 50000 or creditrating < 600 or networth < 100000 THEN 'MoneyAlert+' ELSE '' END + -- noqa: LT05
                CASE WHEN numbercars > 3 or numbercreditcards > 7 THEN 'Spender+' ELSE '' END + -- noqa: LT05
                CASE WHEN age < 25 and networth > 1000000 THEN 'Inherited+' ELSE '' END -- noqa: LT05
            ) IS NOT NULL
        THEN
            LEFT (
                CASE WHEN networth > 1000000 or income > 200000 THEN 'HighValue+' ELSE '' END + -- noqa: LT05
                CASE WHEN numberchildren > 3 or numbercreditcards > 5 THEN 'Expenses+' ELSE '' END + -- noqa: LT05
                CASE WHEN age > 45 THEN 'Boomer+' ELSE '' END +
                CASE WHEN income < 50000 or creditrating < 600 or networth < 100000 THEN 'MoneyAlert+' ELSE '' END + -- noqa: LT05
                CASE WHEN numbercars > 3 or numbercreditcards > 7 THEN 'Spender+' ELSE '' END + -- noqa: LT05
                CASE WHEN age < 25 and networth > 1000000 THEN 'Inherited+' ELSE '' END, -- noqa: LT05
                LEN(
                    CASE WHEN networth > 1000000 or income > 200000 THEN 'HighValue+' ELSE '' END + -- noqa: LT05
                    CASE WHEN numberchildren > 3 or numbercreditcards > 5 THEN 'Expenses+' ELSE '' END + -- noqa: LT05
                    CASE WHEN age > 45 THEN 'Boomer+' ELSE '' END +
                    CASE WHEN income < 50000 or creditrating < 600 or networth < 100000 THEN 'MoneyAlert+' ELSE '' END + -- noqa: LT05
                    CASE WHEN numbercars > 3 or numbercreditcards > 7 THEN 'Spender+' ELSE '' END + -- noqa: LT05
                    CASE WHEN age < 25 and networth > 1000000 THEN 'Inherited+' ELSE '' END -- noqa: LT05
                ) - 1
            )
        ELSE NULL
    END marketingnameplate*/
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
                        FROM {{ ref('prospectraw') }} AS p
                        --FROM stg.ProspectRaw p
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
            --QUALIFY ROW_NUMBER() OVER (PARTITION BY agencyid ORDER BY batchid DESC) = 1 -- noqa: LT05
            ) AS t1
        WHERE t1.rownum = 1
    ) AS p
    INNER JOIN (
        SELECT
            sk_dateid,
            b.batchid
        FROM {{ ref('batchdate') }} AS b
            --FROM prd.BatchDate b
            INNER JOIN {{ source('tpcdi', 'DimDate') }} AS d
                --JOIN prd.DimDate d
                ON b.batchdate = d.datevalue
    ) AS recdate
        ON p.recordbatchid = recdate.batchid
    INNER JOIN (
        SELECT
            sk_dateid,
            b.batchid
        FROM {{ ref('batchdate') }} AS b
            --FROM prd.BatchDate b
            INNER JOIN {{ source('tpcdi', 'DimDate') }} AS d
                --JOIN prd.DimDate d
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
        --FROM dbo.DimCustomerStg
        --WHERE iscurrent) c
        WHERE iscurrent = 1
    ) AS c
        ON
            UPPER(p.lastname) = UPPER(c.lastname)
            AND UPPER(p.firstname) = UPPER(c.firstname)
            AND UPPER(p.addressline1) = UPPER(c.addressline1)
            --and upper(nvl(p.addressline2, '')) = upper(nvl(c.addressline2, '')) --noqa: LT05
            AND UPPER(ISNULL(p.addressline2, ''))
            = UPPER(ISNULL(c.addressline2, ''))
            AND UPPER(p.postalcode) = UPPER(c.postalcode)
