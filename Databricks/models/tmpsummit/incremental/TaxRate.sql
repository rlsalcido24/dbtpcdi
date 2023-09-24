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
    fileNamePattern => "TaxRate.txt",
    schema => "tx_id STRING NOT NULL COMMENT 'Tax rate code', tx_name STRING NOT NULL COMMENT 'Tax rate description', tx_rate FLOAT NOT NULL COMMENT 'Tax rate'"
  )
