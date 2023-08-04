{{
    config(
        materialized = 'table'
    )
}}
SELECT * FROM (
    SELECT
        sk_customerid,
        customerid,
        COALESCE(taxid, LAST_VALUE(taxid) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) taxid,
        status,
        COALESCE(lastname, LAST_VALUE(lastname) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) lastname,
        COALESCE(firstname, LAST_VALUE(firstname) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) firstname,
        COALESCE(middleinitial, LAST_VALUE(middleinitial) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) middleinitial,
        COALESCE(gender, LAST_VALUE(gender) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) gender,
        COALESCE(tier, LAST_VALUE(tier) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) tier,
        COALESCE(dob, LAST_VALUE(dob) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) dob,
        COALESCE(addressline1, LAST_VALUE(addressline1) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) addressline1,
        COALESCE(addressline2, LAST_VALUE(addressline2) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) addressline2,
        COALESCE(postalcode, LAST_VALUE(postalcode) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) postalcode,
        COALESCE(city, LAST_VALUE(city) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) city,
        COALESCE(stateprov, LAST_VALUE(stateprov) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) stateprov,
        COALESCE(country, LAST_VALUE(country) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) country,
        COALESCE(phone1, LAST_VALUE(phone1) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) phone1,
        COALESCE(phone2, LAST_VALUE(phone2) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) phone2,
        COALESCE(phone3, LAST_VALUE(phone3) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) phone3,
        COALESCE(email1, LAST_VALUE(email1) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) email1,
        COALESCE(email2, LAST_VALUE(email2) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) email2,
        COALESCE(lcl_tx_id, LAST_VALUE(lcl_tx_id) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) lcl_tx_id,
        COALESCE(nat_tx_id, LAST_VALUE(nat_tx_id) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) nat_tx_id,
        batchid,
        NVL2(
            LEAD(update_ts) OVER (PARTITION BY customerid ORDER BY update_ts),
            false,
            true
        ) iscurrent,
        DATE(update_ts) effectivedate,
        COALESCE(
            LEAD(DATE(update_ts))
                OVER (PARTITION BY customerid ORDER BY update_ts),
            DATE('9999-12-31')
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
            BIGINT(
                CONCAT(
                    DATE_FORMAT(update_ts, 'yyyyMMdd'),
                    CAST(customerid AS STRING)
                )
            ) AS sk_customerid
        FROM {{ ref('CustomerMgmtView') }} c
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
                WHEN ISNULL(c_local_1) THEN c_local_1
                ELSE CONCAT(
                    NVL2(c_ctry_1, '+' || c_ctry_1 || ' ', ''),
                    NVL2(c_area_1, '(' || c_area_1 || ') ', ''),
                    c_local_1,
                    COALESCE(c_ext_1, '')
                )
            END AS phone1,
            CASE
                WHEN ISNULL(c_local_2) THEN c_local_2
                ELSE CONCAT(
                    NVL2(c_ctry_2, '+' || c_ctry_2 || ' ', ''),
                    NVL2(c_area_2, '(' || c_area_2 || ') ', ''),
                    c_local_2,
                    COALESCE(c_ext_2, '')
                )
            END AS phone2,
            CASE
                WHEN ISNULL(c_local_3) THEN c_local_3
                ELSE CONCAT(
                    NVL2(c_ctry_3, '+' || c_ctry_3 || ' ', ''),
                    NVL2(c_area_3, '(' || c_area_3 || ') ', ''),
                    c_local_3,
                    COALESCE(c_ext_3, '')
                )
            END AS phone3,
            NULLIF(c.email1, '') email1,
            NULLIF(c.email2, '') email2,
            c.lcl_tx_id,
            c.nat_tx_id,
            c.batchid,
            TIMESTAMP(bd.batchdate) update_ts,
            BIGINT(
                CONCAT(
                    DATE_FORMAT(update_ts, 'yyyyMMdd'),
                    CAST(customerid AS STRING)
                )
            ) AS sk_customerid
        FROM {{ ref('CustomerIncremental') }} c
            JOIN {{ ref('BatchDate') }} bd
                ON c.batchid = bd.batchid
            JOIN {{ source('tpcdi', 'StatusType') }} s
                ON c.status = s.st_id
    ) c
)
