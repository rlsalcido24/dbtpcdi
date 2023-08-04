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
        --coalesce(taxid, last_value(taxid) IGNORE NULLS OVER (
        COALESCE(taxid, LAST_VALUE(taxid) OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) taxid,
        status,
        --coalesce(lastname, last_value(lastname) IGNORE NULLS OVER (
        COALESCE(lastname, LAST_VALUE(lastname) OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) lastname,
        --coalesce(firstname, last_value(firstname) IGNORE NULLS OVER (
        COALESCE(firstname, LAST_VALUE(firstname) OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) firstname,
        --coalesce(middleinitial, last_value(middleinitial) IGNORE NULLS OVER (
        COALESCE(middleinitial, LAST_VALUE(middleinitial) OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) middleinitial,
        --coalesce(gender, last_value(gender) IGNORE NULLS OVER (
        COALESCE(gender, LAST_VALUE(gender) OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) gender,
        --coalesce(tier, last_value(tier) IGNORE NULLS OVER (
        COALESCE(tier, LAST_VALUE(tier) OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) tier,
        --coalesce(dob, last_value(dob) IGNORE NULLS OVER (
        COALESCE(dob, LAST_VALUE(dob) OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) dob,
        --coalesce(addressline1, last_value(addressline1) IGNORE NULLS OVER (
        COALESCE(addressline1, LAST_VALUE(addressline1) OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) addressline1,
        --coalesce(addressline2, last_value(addressline2) IGNORE NULLS OVER (
        COALESCE(addressline2, LAST_VALUE(addressline2) OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) addressline2,
        --coalesce(postalcode, last_value(postalcode) IGNORE NULLS OVER (
        COALESCE(postalcode, LAST_VALUE(postalcode) OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) postalcode,
        --coalesce(CITY, last_value(CITY) IGNORE NULLS OVER (
        COALESCE(city, LAST_VALUE(city) OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) city,
        --coalesce(stateprov, last_value(stateprov) IGNORE NULLS OVER (
        COALESCE(stateprov, LAST_VALUE(stateprov) OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) stateprov,
        --coalesce(country, last_value(country) IGNORE NULLS OVER (
        COALESCE(country, LAST_VALUE(country) OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) country,
        --coalesce(phone1, last_value(phone1) IGNORE NULLS OVER (
        COALESCE(phone1, LAST_VALUE(phone1) OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) phone1,
        --coalesce(phone2, last_value(phone2) IGNORE NULLS OVER (
        COALESCE(phone2, LAST_VALUE(phone2) OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) phone2,
        --coalesce(phone3, last_value(phone3) IGNORE NULLS OVER (
        COALESCE(phone3, LAST_VALUE(phone3) OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) phone3,
        --coalesce(email1, last_value(email1) IGNORE NULLS OVER (
        COALESCE(email1, LAST_VALUE(email1) OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) email1,
        --coalesce(email2, last_value(email2) IGNORE NULLS OVER (
        COALESCE(email2, LAST_VALUE(email2) OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) email2,
        --coalesce(LCL_TX_ID, last_value(LCL_TX_ID) IGNORE NULLS OVER (
        COALESCE(lcl_tx_id, LAST_VALUE(lcl_tx_id) OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) lcl_tx_id,
        --coalesce(NAT_TX_ID, last_value(NAT_TX_ID) IGNORE NULLS OVER (
        COALESCE(nat_tx_id, LAST_VALUE(nat_tx_id) OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) nat_tx_id,
        batchid,
        CASE
            WHEN
                LEAD(update_ts)
                    OVER (PARTITION BY customerid ORDER BY update_ts)
                IS NOT NULL
                THEN 0
            ELSE 1
        END iscurrent,
        CONVERT(DATE, update_ts) effectivedate,
        COALESCE(
            LEAD(CONVERT(DATE, update_ts))
                OVER (PARTITION BY customerid ORDER BY update_ts),
            CONVERT(DATE, '9999-12-31')
        ) enddate
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
            1 batchid,
            update_ts,
            CONCAT(customerid, '-', update_ts) AS sk_customerid
        FROM {{ ref('CustomerMgmt') }} c
        WHERE actiontype IN ('NEW', 'INACT', 'UPDCUST')

        UNION ALL

        SELECT
            c.customerid,
            NULLIF(c.taxid, '') taxid,
            NULLIF(s.st_name, '') AS status,
            NULLIF(c.lastname, '') lastname,
            NULLIF(c.firstname, '') firstname,
            NULLIF(c.middleinitial, '') middleinitial,
            gender,
            c.tier,
            c.dob,
            NULLIF(c.addressline1, '') addressline1,
            NULLIF(c.addressline2, '') addressline2,
            NULLIF(c.postalcode, '') postalcode,
            NULLIF(c.city, '') city,
            NULLIF(c.stateprov, '') stateprov,
            NULLIF(c.country, '') country,
            CASE
                WHEN c_local_1 IS NULL THEN c_local_1
                ELSE CONCAT(
                    CASE
                        WHEN
                            c_ctry_1 IS NOT NULL
                            THEN '+' + c_ctry_1 + ' '
                        ELSE ''
                    END,
                    CASE
                        WHEN
                            c_area_1 IS NOT NULL
                            THEN '(' + c_area_1 + ') '
                        ELSE ''
                    END,
                    c_local_1,
                    ISNULL(c_ext_1, '')
                )
            END AS phone1,
            CASE
                WHEN c_local_2 IS NULL THEN c_local_2
                ELSE CONCAT(
                    CASE
                        WHEN
                            c_ctry_2 IS NOT NULL
                            THEN '+' + c_ctry_2 + ' '
                        ELSE ''
                    END,
                    CASE
                        WHEN
                            c_area_2 IS NOT NULL
                            THEN '(' + c_area_2 + ') '
                        ELSE ''
                    END,
                    c_local_2,
                    ISNULL(c_ext_2, '')
                )
            END AS phone2,
            CASE
                WHEN c_local_3 IS NULL THEN c_local_3
                ELSE CONCAT(
                    CASE
                        WHEN
                            c_ctry_3 IS NOT NULL
                            THEN '+' + c_ctry_3 + ' '
                        ELSE ''
                    END,
                    CASE
                        WHEN
                            c_area_3 IS NOT NULL
                            THEN '(' + c_area_3 + ') '
                        ELSE ''
                    END,
                    c_local_3,
                    ISNULL(c_ext_3, '')
                )
            END AS phone3,
            NULLIF(c.email1, '') email1,
            NULLIF(c.email2, '') email2,
            c.lcl_tx_id,
            c.nat_tx_id,
            c.batchid,
            CONVERT(DATETIME2, bd.batchdate) update_ts,
            CONCAT(c.customerid, '-', CONVERT(DATETIME2, bd.batchdate))
                AS sk_customerid
        FROM {{ ref('CustomerIncremental') }} c
            JOIN {{ ref('BatchDate') }} bd
                ON c.batchid = bd.batchid
            JOIN {{ ref('StatusType') }} s
                ON c.status = s.st_id
    ) c
) t
