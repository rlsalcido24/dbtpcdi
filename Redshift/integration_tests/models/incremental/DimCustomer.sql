{{
    config(
        materialized = 'table'
    )
}}
SELECT
    c.sk_customerid,
    c.customerid,
    c.taxid,
    c.status,
    c.lastname,
    c.firstname,
    c.middleinitial,
    CASE WHEN c.gender IN ('M', 'F') THEN c.gender ELSE 'U' END AS gender,
    c.tier,
    c.dob,
    c.addressline1,
    c.addressline2,
    c.postalcode,
    c.city,
    c.stateprov,
    c.country,
    c.phone1,
    c.phone2,
    c.phone3,
    c.email1,
    c.email2,
    r_nat.tx_name AS nationaltaxratedesc,
    r_nat.tx_rate AS nationaltaxrate,
    r_lcl.tx_name AS localtaxratedesc,
    r_lcl.tx_rate AS localtaxrate,
    p.agencyid,
    p.creditrating,
    p.networth,
    p.marketingnameplate,
    c.iscurrent,
    c.batchid,
    c.effectivedate,
    c.enddate
FROM {{ ref('dimcustomerstg') }} AS c
    LEFT JOIN {{ source('tpcdi', 'TaxRate') }} AS r_lcl
        ON c.lcl_tx_id = r_lcl.tx_id
    LEFT JOIN {{ source('tpcdi', 'TaxRate') }} AS r_nat
        ON c.nat_tx_id = r_nat.tx_id
    LEFT JOIN {{ ref('prospect') }} AS p
        ON
            UPPER(p.lastname) = UPPER(c.lastname)
            AND UPPER(p.firstname) = UPPER(c.firstname)
            AND UPPER(p.addressline1) = UPPER(c.addressline1)
            AND UPPER(ISNULL(p.addressline2, ''))
            = UPPER(ISNULL(c.addressline2, ''))
            AND UPPER(p.postalcode) = UPPER(c.postalcode)
