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
    fileNamePattern => "Industry.txt",
    schema => "in_id STRING NOT NULL COMMENT 'Industry code', in_name STRING NOT NULL COMMENT 'Industry description', in_sc_id STRING NOT NULL COMMENT 'Sector identifier'"
  )
