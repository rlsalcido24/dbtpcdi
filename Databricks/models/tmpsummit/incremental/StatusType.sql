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
    fileNamePattern => "StatusType.txt",
    schema => "st_id STRING NOT NULL COMMENT 'Status code', st_name STRING NOT NULL COMMENT 'Status description'"
  )
