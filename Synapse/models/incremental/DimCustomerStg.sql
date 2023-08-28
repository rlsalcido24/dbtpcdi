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
        COALESCE(taxid, LAST_VALUE(taxid) /*IGNORE NULLS*/ OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS taxid,
        status,
        COALESCE(lastname, LAST_VALUE(lastname) /*IGNORE NULLS*/ OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS lastname,
        COALESCE(firstname, LAST_VALUE(firstname) /*IGNORE NULLS*/ OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS firstname,
        COALESCE(
            middleinitial, LAST_VALUE(middleinitial) /*IGNORE NULLS*/ OVER (
                PARTITION BY customerid
                ORDER BY update_ts
            )
        ) AS middleinitial,
        COALESCE(gender, LAST_VALUE(gender) /*IGNORE NULLS*/ OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS gender,
        COALESCE(tier, LAST_VALUE(tier) /*IGNORE NULLS*/ OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS tier,
        COALESCE(dob, LAST_VALUE(dob) /*IGNORE NULLS*/ OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS dob,
        COALESCE(addressline1, LAST_VALUE(addressline1) /*IGNORE NULLS*/ OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS addressline1,
        COALESCE(addressline2, LAST_VALUE(addressline2) /*IGNORE NULLS*/ OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS addressline2,
        COALESCE(postalcode, LAST_VALUE(postalcode) /*IGNORE NULLS*/ OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS postalcode,
        COALESCE(city, LAST_VALUE(city) /*IGNORE NULLS*/ OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS city,
        COALESCE(stateprov, LAST_VALUE(stateprov) /*IGNORE NULLS*/ OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS stateprov,
        COALESCE(country, LAST_VALUE(country) /*IGNORE NULLS*/ OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS country,
        COALESCE(phone1, LAST_VALUE(phone1) /*IGNORE NULLS*/ OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS phone1,
        COALESCE(phone2, LAST_VALUE(phone2) /*IGNORE NULLS*/ OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS phone2,
        COALESCE(phone3, LAST_VALUE(phone3) /*IGNORE NULLS*/ OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS phone3,
        COALESCE(email1, LAST_VALUE(email1) /*IGNORE NULLS*/ OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS email1,
        COALESCE(email2, LAST_VALUE(email2) /*IGNORE NULLS*/ OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS email2,
        COALESCE(lcl_tx_id, LAST_VALUE(lcl_tx_id) /*IGNORE NULLS*/ OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS lcl_tx_id,
        COALESCE(nat_tx_id, LAST_VALUE(nat_tx_id) /*IGNORE NULLS*/ OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS nat_tx_id,
        batchid,
        CASE
            WHEN
                LEAD(update_ts)
                    OVER (PARTITION BY customerid ORDER BY update_ts)
                IS NOT NULL
                THEN 0
            ELSE 1
        END AS iscurrent,
        CAST(update_ts AS DATE) AS effectivedate,
        COALESCE(
            LEAD(CAST(update_ts AS DATE))
                OVER (PARTITION BY customerid ORDER BY update_ts),
            CAST('9999-12-31' AS DATE)
        ) AS enddate
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
            1 AS batchid,
            update_ts,
            CONCAT(customerid, '-', update_ts) AS sk_customerid
        FROM {{ ref('CustomerMgmt') }}
        WHERE actiontype IN ('NEW', 'INACT', 'UPDCUST')

        UNION ALL

        SELECT
            c.customerid,
            NULLIF(c.taxid, '') AS taxid,
            NULLIF(s.st_name, '') AS status,
            NULLIF(c.lastname, '') AS lastname,
            NULLIF(c.firstname, '') AS firstname,
            NULLIF(c.middleinitial, '') AS middleinitial,
            c.gender,
            c.tier,
            c.dob,
            NULLIF(c.addressline1, '') AS addressline1,
            NULLIF(c.addressline2, '') AS addressline2,
            NULLIF(c.postalcode, '') AS postalcode,
            NULLIF(c.city, '') AS city,
            NULLIF(c.stateprov, '') AS stateprov,
            NULLIF(c.country, '') AS country,
            CASE
                WHEN c.c_local_1 IS NULL THEN c.c_local_1
                ELSE CONCAT(
                    CASE
                        WHEN
                            c.c_ctry_1 IS NOT NULL
                            THEN '+' + c.c_ctry_1 + ' '
                        ELSE ''
                    END,
                    CASE
                        WHEN
                            c.c_area_1 IS NOT NULL
                            THEN '(' + c.c_area_1 + ') '
                        ELSE ''
                    END,
                    c.c_local_1,
                    ISNULL(c.c_ext_1, '')
                )
            END AS phone1,
            CASE
                WHEN c.c_local_2 IS NULL THEN c.c_local_2
                ELSE CONCAT(
                    CASE
                        WHEN
                            c.c_ctry_2 IS NOT NULL
                            THEN '+' + c.c_ctry_2 + ' '
                        ELSE ''
                    END,
                    CASE
                        WHEN
                            c.c_area_2 IS NOT NULL
                            THEN '(' + c.c_area_2 + ') '
                        ELSE ''
                    END,
                    c.c_local_2,
                    ISNULL(c.c_ext_2, '')
                )
            END AS phone2,
            CASE
                WHEN c.c_local_3 IS NULL THEN c.c_local_3
                ELSE CONCAT(
                    CASE
                        WHEN
                            c.c_ctry_3 IS NOT NULL
                            THEN '+' + c.c_ctry_3 + ' '
                        ELSE ''
                    END,
                    CASE
                        WHEN
                            c.c_area_3 IS NOT NULL
                            THEN '(' + c.c_area_3 + ') '
                        ELSE ''
                    END,
                    c.c_local_3,
                    ISNULL(c.c_ext_3, '')
                )
            END AS phone3,
            NULLIF(c.email1, '') AS email1,
            NULLIF(c.email2, '') AS email2,
            c.lcl_tx_id,
            c.nat_tx_id,
            c.batchid,
            CAST(bd.batchdate AS DATETIME2) AS update_ts,
            CONCAT(c.customerid, '-', CAST(bd.batchdate AS DATETIME2))
                AS sk_customerid
        FROM {{ ref('CustomerIncremental') }} AS c
            INNER JOIN {{ ref('BatchDate') }} AS bd
                ON c.batchid = bd.batchid
            INNER JOIN {{ ref('StatusType') }} AS s
                ON c.status = s.st_id
    ) AS c
) AS t
