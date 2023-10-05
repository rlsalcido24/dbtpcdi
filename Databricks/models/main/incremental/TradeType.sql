{{
    config(
        materialized = 'streaming_table'
    )
}}
SELECT
  *
FROM
  STREAM read_files(
    "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/sf={{ var('scalefactor') }}/Batch1",
    format => "csv",
    inferSchema => False,
    header => 'False',
    sep => '|',
    fileNamePattern => "TradeType.txt",
    schema => "tt_id STRING NOT NULL COMMENT 'Trade type code', tt_name STRING NOT NULL COMMENT 'Trade type description', tt_is_sell INT NOT NULL COMMENT 'Flag indicating a sale', tt_is_mrkt INT NOT NULL COMMENT 'Flag indicating a market order'"
  )

