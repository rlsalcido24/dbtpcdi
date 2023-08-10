{{
    config(
        materialized = 'table'
    )
}}
--,index='HEAP'
--,dist='ROUND_ROBIN'
SELECT *
FROM (
    SELECT
        sk_customerid,
        customerid,
        --coalesce(taxid, last_value(taxid) IGNORE NULLS OVER (
        COALESCE(taxid, LAST_VALUE(taxid IGNORE NULLS) OVER (
            PARTITION BY customerid
            ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS taxid,
        status,
        --coalesce(lastname, last_value(lastname) IGNORE NULLS OVER (
        COALESCE(lastname, LAST_VALUE(lastname IGNORE NULLS) OVER (
            PARTITION BY customerid
            ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS lastname,
        --coalesce(firstname, last_value(firstname) IGNORE NULLS OVER (
        COALESCE(firstname, LAST_VALUE(firstname IGNORE NULLS) OVER (
            PARTITION BY customerid
            ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS firstname,
        --coalesce(middleinitial, last_value(middleinitial) IGNORE NULLS OVER (
        COALESCE(middleinitial, LAST_VALUE(middleinitial IGNORE NULLS) OVER (
            PARTITION BY customerid
            ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS middleinitial,
        --coalesce(gender, last_value(gender) IGNORE NULLS OVER (
        COALESCE(gender, LAST_VALUE(gender IGNORE NULLS) OVER (
            PARTITION BY customerid
            ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS gender,
        --coalesce(tier, last_value(tier) IGNORE NULLS OVER (
        COALESCE(tier, LAST_VALUE(tier IGNORE NULLS) OVER (
            PARTITION BY customerid
            ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS tier,
        --coalesce(dob, last_value(dob) IGNORE NULLS OVER (
        COALESCE(dob, LAST_VALUE(dob IGNORE NULLS) OVER (
            PARTITION BY customerid
            ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS dob,
        --coalesce(addressline1, last_value(addressline1) IGNORE NULLS OVER (
        COALESCE(addressline1, LAST_VALUE(addressline1 IGNORE NULLS) OVER (
            PARTITION BY customerid
            ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS addressline1,
        --coalesce(addressline2, last_value(addressline2) IGNORE NULLS OVER (
        COALESCE(addressline2, LAST_VALUE(addressline2 IGNORE NULLS) OVER (
            PARTITION BY customerid
            ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS addressline2,
        --coalesce(postalcode, last_value(postalcode) IGNORE NULLS OVER (
        COALESCE(postalcode, LAST_VALUE(postalcode IGNORE NULLS) OVER (
            PARTITION BY customerid
            ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS postalcode,
        --coalesce(CITY, last_value(CITY) IGNORE NULLS OVER (
        COALESCE(city, LAST_VALUE(city IGNORE NULLS) OVER (
            PARTITION BY customerid
            ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS city,
        --coalesce(stateprov, last_value(stateprov) IGNORE NULLS OVER (
        COALESCE(stateprov, LAST_VALUE(stateprov IGNORE NULLS) OVER (
            PARTITION BY customerid
            ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS stateprov,
        --coalesce(country, last_value(country) IGNORE NULLS OVER (
        COALESCE(country, LAST_VALUE(country IGNORE NULLS) OVER (
            PARTITION BY customerid
            ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS country,
        --coalesce(phone1, last_value(phone1) IGNORE NULLS OVER (
        COALESCE(phone1, LAST_VALUE(phone1 IGNORE NULLS) OVER (
            PARTITION BY customerid
            ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS phone1,
        --coalesce(phone2, last_value(phone2) IGNORE NULLS OVER (
        COALESCE(phone2, LAST_VALUE(phone2 IGNORE NULLS) OVER (
            PARTITION BY customerid
            ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS phone2,
        --coalesce(phone3, last_value(phone3) IGNORE NULLS OVER (
        COALESCE(phone3, LAST_VALUE(phone3 IGNORE NULLS) OVER (
            PARTITION BY customerid
            ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS phone3,
        --coalesce(email1, last_value(email1) IGNORE NULLS OVER (
        COALESCE(email1, LAST_VALUE(email1 IGNORE NULLS) OVER (
            PARTITION BY customerid
            ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS email1,
        --coalesce(email2, last_value(email2) IGNORE NULLS OVER (
        COALESCE(email2, LAST_VALUE(email2 IGNORE NULLS) OVER (
            PARTITION BY customerid
            ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS email2,
        --coalesce(LCL_TX_ID, last_value(LCL_TX_ID) IGNORE NULLS OVER (
        COALESCE(lcl_tx_id, LAST_VALUE(lcl_tx_id IGNORE NULLS) OVER (
            PARTITION BY customerid
            ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS lcl_tx_id,
        --coalesce(NAT_TX_ID, last_value(NAT_TX_ID) IGNORE NULLS OVER (
        COALESCE(nat_tx_id, LAST_VALUE(nat_tx_id IGNORE NULLS) OVER (
            PARTITION BY customerid
            ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )) AS nat_tx_id,
        batchid,
        --nvl2(lead(update_ts) OVER (PARTITION BY customerid ORDER BY update_ts), false, true) iscurrent, -- noqa: LT05
        CASE
            WHEN
                LEAD(update_ts)
                    OVER (PARTITION BY customerid ORDER BY update_ts)
                IS NOT NULL
                THEN 0
            ELSE 1
        END AS iscurrent,
        --date(update_ts) effectivedate,
        CAST(update_ts AS TIMESTAMP) AS effectivedate,
        --coalesce(lead(date(update_ts)) OVER (PARTITION BY customerid ORDER BY update_ts), date('9999-12-31')) enddate -- noqa: LT05
        COALESCE(
            LEAD(CAST(update_ts AS TIMESTAMP))
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
            TO_TIMESTAMP(update_ts, 'YYYY-MM-DD HH24:MI:SS') AS update_ts,
            CONCAT(CONCAT(customerid, '-'), update_ts) AS sk_customerid
        FROM {{ ref('customermgmtview') }}
        --    FROM stg.CustomerMgmt c
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
                --WHEN isnull(c.c_local_1) then c.c_local_1
                WHEN c.c_local_1 IS NULL THEN c.c_local_1
                ELSE
                    CONCAT(
                        CONCAT(CONCAT(
                            CASE
                                WHEN
                                    c.c_ctry_1 IS NOT NULL
                                    THEN CONCAT(CONCAT('+', c.c_ctry_1), ' ')
                                ELSE ''
                            END,
                            CASE
                                WHEN
                                    c.c_area_1 IS NOT NULL
                                    THEN CONCAT(CONCAT('(', c.c_area_1), ') ')
                                ELSE ''
                            END
                        ),
                        c.c_local_1),
                        COALESCE(c.c_ext_1, '')
                    )

            END AS phone1,
            CASE
                --WHEN isnull(c.c_local_2) then c.c_local_2
                WHEN c.c_local_2 IS NULL THEN c.c_local_2
                ELSE
                    CONCAT(
                        CONCAT(CONCAT(
                            CASE
                                WHEN
                                    c.c_ctry_2 IS NOT NULL
                                    THEN CONCAT(CONCAT('+', c.c_ctry_2), ' ')
                                ELSE ''
                            END,
                            CASE
                                WHEN
                                    c.c_area_2 IS NOT NULL
                                    THEN CONCAT(CONCAT('(', c.c_area_2), ') ')
                                ELSE ''
                            END
                        ),
                        c.c_local_2),
                        COALESCE(c.c_ext_2, '')
                    )
            END AS phone2,
            CASE
                --WHEN isnull(c.c_local_3) then c.c_local_3
                WHEN c.c_local_3 IS NULL THEN c.c_local_3
                ELSE
                    CONCAT(
                        CONCAT(CONCAT(
                            CASE
                                WHEN
                                    c.c_ctry_3 IS NOT NULL
                                    THEN CONCAT(CONCAT('+', c.c_ctry_3), ' ')
                                ELSE ''
                            END,
                            CASE
                                WHEN
                                    c.c_area_3 IS NOT NULL
                                    THEN CONCAT(CONCAT('(', c.c_area_3), ') ')
                                ELSE ''
                            END
                        ),
                        c.c_local_3),
                        COALESCE(c.c_ext_3, '')
                    )


            END AS phone3,
            NULLIF(c.email1, '') AS email1,
            NULLIF(c.email2, '') AS email2,
            c.lcl_tx_id,
            c.nat_tx_id,
            c.batchid,
            --timestamp(bd.batchdate) update_ts,
            TO_TIMESTAMP(bd.batchdate, 'YYYY-MM-DD HH24:MI:SS') AS update_ts,
            --concat(c.customerid, '-', update_ts) as sk_customerid
            CONCAT(CONCAT(c.customerid, '-'), bd.batchdate) AS sk_customerid
        FROM {{ ref('customerincremental') }} AS c
            --    FROM stg.CustomerIncremental c
            INNER JOIN {{ ref('batchdate') }} AS bd
                --    INNER JOIN prd.BatchDate bd
                ON c.batchid = bd.batchid
            INNER JOIN {{ source('tpcdi', 'StatusType') }} AS s
                --    INNER JOIN sf10.StatusType s
                ON c.status = s.st_id
    ) AS c
) AS t
