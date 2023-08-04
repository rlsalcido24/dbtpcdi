{{
    config(
        materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='HASH(sk_companyid)'
    )
}}


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
    isnull(cast(try_cast(conameorcik as bigint) as varchar), conameorcik) conameorcik
  FROM (
    SELECT
      convert(datetime2, substring([value],1,8)+' '+substring([value],10,2)+':'+substring([value],12,2)+':'+substring([value],14,2), 112) as PTS,
      cast(substring(value, 19, 4) AS INT) AS fi_year,
      cast(substring(value, 23, 1) AS INT) AS fi_qtr,
      convert(date, substring([value],24,8), 112) AS fi_qtr_start_date,
      cast(substring(value, 40, 17) AS decimal(15,2)) AS fi_revenue,        --float
      cast(substring(value, 57, 17) AS decimal(15,2)) AS fi_net_earn,       --float
      cast(substring(value, 74, 12) AS decimal(10,2)) AS fi_basic_eps,      --float
      cast(substring(value, 86, 12) AS decimal(10,2)) AS fi_dilut_eps,      --float
      cast(substring(value, 98, 12) AS decimal(10,2)) AS fi_margin,         --float
      cast(substring(value, 110, 17) AS decimal(15,2)) AS fi_inventory,     --float
      cast(substring(value, 127, 17) AS decimal(15,2)) AS fi_assets,        --float
      cast(substring(value, 144, 17) AS decimal(15,2)) AS fi_liability,     --float
      cast(substring(value, 161, 13) AS BIGINT) AS fi_out_basic,
      cast(substring(value, 174, 13) AS BIGINT) AS fi_out_dilut,
      trim(substring(value, 187, 60)) AS conameorcik
    FROM {{ ref('FinWire_FIN') }}
  ) f 
) f
JOIN (
  SELECT 
    sk_companyid,
    name conameorcik,
    EffectiveDate,
    EndDate
  FROM {{ ref('DimCompany') }}
  UNION ALL
  SELECT 
    sk_companyid,
    cast(companyid as varchar) conameorcik,
    EffectiveDate,
    EndDate
  FROM {{ ref('DimCompany') }}
) dc 
ON
  f.conameorcik = dc.conameorcik 
  AND cast(PTS as date) >= dc.effectivedate 
  AND cast(PTS as date) < dc.enddate