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
    fileNamePattern => "Time.txt",
    schema => "sk_timeid BIGINT NOT NULL COMMENT 'Surrogate key for the time', timevalue STRING NOT NULL COMMENT 'The time stored appropriately for doing', hourid INT NOT NULL COMMENT 'Hour number as a number, e.g. 01', hourdesc STRING NOT NULL COMMENT 'Hour number as text, e.g. 01', minuteid INT NOT NULL COMMENT 'Minute as a number, e.g. 23', minutedesc STRING NOT NULL COMMENT 'Minute as text, e.g. 01:23', secondid INT NOT NULL COMMENT 'Second as a number, e.g. 45', seconddesc STRING NOT NULL COMMENT 'Second as text, e.g. 01:23:45', markethoursflag BOOLEAN COMMENT 'Indicates a time during market hours', officehoursflag BOOLEAN COMMENT 'Indicates a time during office hours'"
  )