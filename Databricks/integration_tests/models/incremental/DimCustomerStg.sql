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
        )) AS taxid,
        status,
        COALESCE(lastname, LAST_VALUE(lastname) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS lastname,
        COALESCE(firstname, LAST_VALUE(firstname) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS firstname,
        COALESCE(middleinitial, LAST_VALUE(middleinitial) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS middleinitial,
        COALESCE(gender, LAST_VALUE(gender) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS gender,
        COALESCE(tier, LAST_VALUE(tier) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS tier,
        COALESCE(dob, LAST_VALUE(dob) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS dob,
        COALESCE(addressline1, LAST_VALUE(addressline1) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS addressline1,
        COALESCE(addressline2, LAST_VALUE(addressline2) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS addressline2,
        COALESCE(postalcode, LAST_VALUE(postalcode) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS postalcode,
        COALESCE(city, LAST_VALUE(city) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS city,
        COALESCE(stateprov, LAST_VALUE(stateprov) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS stateprov,
        COALESCE(country, LAST_VALUE(country) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS country,
        COALESCE(phone1, LAST_VALUE(phone1) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS phone1,
        COALESCE(phone2, LAST_VALUE(phone2) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS phone2,
        COALESCE(phone3, LAST_VALUE(phone3) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS phone3,
        COALESCE(email1, LAST_VALUE(email1) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS email1,
        COALESCE(email2, LAST_VALUE(email2) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS email2,
        COALESCE(lcl_tx_id, LAST_VALUE(lcl_tx_id) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS lcl_tx_id,
        COALESCE(nat_tx_id, LAST_VALUE(nat_tx_id) IGNORE NULLS OVER (
            PARTITION BY customerid
            ORDER BY update_ts
        )) AS nat_tx_id,
        batchid,
        NVL2(
            LEAD(update_ts) OVER (PARTITION BY customerid ORDER BY update_ts),
            FALSE,
            TRUE
        ) AS iscurrent,
        DATE(update_ts) AS effectivedate,
        COALESCE(
            LEAD(DATE(update_ts))
                OVER (PARTITION BY customerid ORDER BY update_ts),
            DATE('9999-12-31')
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
            BIGINT(
                CONCAT(
                    DATE_FORMAT(update_ts, 'yyyyMMdd'),
                    CAST(customerid AS STRING)
                )
            ) AS sk_customerid
        FROM {{ ref('CustomerMgmtView') }}
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
                WHEN ISNULL(c.c_local_1) THEN c.c_local_1
                ELSE CONCAT(
                    NVL2(c.c_ctry_1, '+' || c.c_ctry_1 || ' ', ''),
                    NVL2(c.c_area_1, '(' || c.c_area_1 || ') ', ''),
                    c.c_local_1,
                    COALESCE(c.c_ext_1, '')
                )
            END AS phone1,
            CASE
                WHEN ISNULL(c.c_local_2) THEN c.c_local_2
                ELSE CONCAT(
                    NVL2(c.c_ctry_2, '+' || c.c_ctry_2 || ' ', ''),
                    NVL2(c.c_area_2, '(' || c.c_area_2 || ') ', ''),
                    c.c_local_2,
                    COALESCE(c.c_ext_2, '')
                )
            END AS phone2,
            CASE
                WHEN ISNULL(c.c_local_3) THEN c.c_local_3
                ELSE CONCAT(
                    NVL2(c.c_ctry_3, '+' || c.c_ctry_3 || ' ', ''),
                    NVL2(c.c_area_3, '(' || c.c_area_3 || ') ', ''),
                    c.c_local_3,
                    COALESCE(c.c_ext_3, '')
                )
            END AS phone3,
            NULLIF(c.email1, '') AS email1,
            NULLIF(c.email2, '') AS email2,
            c.lcl_tx_id,
            c.nat_tx_id,
            c.batchid,
            TIMESTAMP(bd.batchdate) AS update_ts,
            BIGINT(
                CONCAT(
                    DATE_FORMAT(update_ts, 'yyyyMMdd'),
                    CAST(c.customerid AS STRING)
                )
            ) AS sk_customerid
        FROM {{ ref('CustomerIncremental') }} AS c
            INNER JOIN {{ ref('BatchDate') }} AS bd
                ON c.batchid = bd.batchid
            INNER JOIN {{ source('tpcdi', 'StatusType') }} AS s
                ON c.status = s.st_id
    ) AS c
)
