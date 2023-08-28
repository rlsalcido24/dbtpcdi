{{
    config(
        materialized = 'table'
    )
}}
SELECT
    hh.hh_h_t_id AS tradeid,
    hh.hh_t_id AS currenttradeid,
    dt.sk_customerid,
    dt.sk_accountid,
    dt.sk_securityid,
    dt.sk_companyid,
    dt.sk_closedateid AS sk_dateid,
    dt.sk_closetimeid AS sk_timeid,
    dt.tradeprice AS currentprice,
    hh.hh_after_qty AS currentholding,
    hh.batchid
FROM (
    SELECT
        *,
        1 AS batchid
    FROM {{ source('tpcdi', 'HoldingHistory') }}
    UNION ALL
    SELECT
        hh_h_t_id,
        hh_t_id,
        hh_before_qty,
        hh_after_qty,
        batchid
    FROM {{ ref('holdingincremental') }}
) AS hh
-- Converts to LEFT JOIN if this is run as DQ EDITION. It is possible, because
-- of the issues upstream with DimSecurity/DimAccount on "some" scale factors,
-- that dimtrade may be missing some rows.
--${dq_left_flg}
    LEFT JOIN {{ ref('dimtrade') }} AS dt
        ON dt.tradeid = hh.hh_t_id
