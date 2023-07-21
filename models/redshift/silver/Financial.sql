{{
    config(
        materialized = 'table'
    )
}}
        --,index='CLUSTERED COLUMNSTORE INDEX'
        --,dist='HASH(sk_companyid)'

SELECT 
  sk_companyid,
  fi_year,
  fi_qtr,
  fi_qtr_start_date,
  fi_revenue,
  fi_net_earn,
  fi_basic_eps,
  fi_dilut_eps,
  fi_margin,
  fi_inventory,
  fi_assets,
  fi_liability,
  fi_out_basic,
  fi_out_dilut
FROM (
  SELECT 
    PTS,
    fi_year,
    fi_qtr,
    fi_qtr_start_date,
    fi_revenue,
    fi_net_earn,
    fi_basic_eps,
    fi_dilut_eps,
    fi_margin,
    fi_inventory,
    fi_assets,
    fi_liability,
    fi_out_basic,
    fi_out_dilut,
    isnull(cast(case
        when trim(conameorcik) ~ '^[0-9]+$' then trim(conameorcik)
        else null
    end::bigint as varchar), conameorcik) conameorcik
  FROM (
    SELECT
      to_timestamp(substring(value, 1, 15), 'YYYYMMDDHH24MISS') AS PTS,
      CAST(SUBSTRING(value, 19, 4) AS INT) AS fi_year,
      CAST(SUBSTRING(value, 23, 1) AS INT) AS fi_qtr,
      TO_DATE(SUBSTRING(value, 24, 8), 'YYYYMMDD') AS fi_qtr_start_date,
      TO_DATE(SUBSTRING(value, 32, 8), 'YYYYMMDD') AS PostingDate,
      CAST(SUBSTRING(value, 40, 17) AS FLOAT) AS fi_revenue,
      CAST(SUBSTRING(value, 57, 17) AS FLOAT) AS fi_net_earn,
      CAST(SUBSTRING(value, 74, 12) AS FLOAT) AS fi_basic_eps,
      CAST(SUBSTRING(value, 86, 12) AS FLOAT) AS fi_dilut_eps,
      CAST(SUBSTRING(value, 98, 12) AS FLOAT) AS fi_margin,
      CAST(SUBSTRING(value, 110, 17) AS FLOAT) AS fi_inventory,
      CAST(SUBSTRING(value, 127, 17) AS FLOAT) AS fi_assets,
      CAST(SUBSTRING(value, 144, 17) AS FLOAT) AS fi_liability,
      CAST(SUBSTRING(value, 161, 13) AS BIGINT) AS fi_out_basic,
      CAST(SUBSTRING(value, 174, 13) AS BIGINT) AS fi_out_dilut,
      TRIM(SUBSTRING(value, 187, 60)) AS conameorcik

    FROM {{ ref('finwire') }}
    --FROM stg.FinWire
    WHERE rectype = 'FIN'
  ) f 
) f
JOIN (
  SELECT 
    sk_companyid,
    name conameorcik,
    EffectiveDate,
    EndDate
  FROM {{ ref('dimcompany') }}
  --FROM dbo.DimCompany
  UNION ALL
  SELECT 
    sk_companyid,
    cast(companyid as varchar) conameorcik,
    EffectiveDate,
    EndDate
  FROM {{ ref('dimcompany') }}
  --FROM dbo.DimCompany
) dc 
ON
  f.conameorcik = dc.conameorcik 
  AND cast(PTS as date) >= dc.effectivedate 
  AND cast(PTS as date) < dc.enddate