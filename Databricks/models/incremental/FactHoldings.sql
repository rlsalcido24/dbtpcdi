{{
    config(
        materialized = 'table'
    )
}}

SELECT
    hh.hh_h_t_id AS tradeid,
    hh.hh_t_id AS currenttradeid,
    sk_customerid,
    sk_accountid,
    sk_securityid,
    sk_companyid,
    sk_closedateid AS sk_dateid,
    sk_closetimeid AS sk_timeid,
    tradeprice AS currentprice,
    hh.hh_after_qty AS currentholding,
    hh.batchid
FROM (
    SELECT
        *,
        1 AS batchid
    FROM {{ source('tpcdi', 'HoldingHistory') }}
    UNION ALL
    SELECT * EXCEPT (cdc_flag, cdc_dsn)
    FROM {{ ref('HoldingIncremental') }}
) AS hh
-- Converts to LEFT JOIN if this is run as DQ EDITION. It is possible, because
-- of the issues upstream with DimSecurity/DimAccount on "some" scale factors,
-- that DimTrade may be missing some rows.
--${dq_left_flg}
    LEFT JOIN {{ ref('DimTrade') }} AS dt
        ON dt.tradeid = hh.hh_t_id
