{{
    config(
        materialized = 'streaming_table'
    )
}}

SELECT
  *,
  cast(
    substring(
      _metadata.file_path
      FROM
        (position('/Batch', _metadata.file_path) + 6) FOR 1
    ) as int
  ) batchid
FROM
  STREAM read_files(
    "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/sf={{ var('scalefactor') }}/Batch*",
    format => "csv",
    inferSchema => False,
    header => 'False',
    sep => '|',
    fileNamePattern => "BatchDate.txt",
    schema => "batchdate DATE NOT NULL COMMENT 'Batch date'"
  )

