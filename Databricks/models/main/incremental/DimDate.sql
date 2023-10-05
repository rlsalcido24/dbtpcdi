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
    fileNamePattern => "Date.txt",
    schema => "sk_dateid BIGINT NOT NULL COMMENT 'Surrogate key for the date', datevalue DATE NOT NULL COMMENT 'The date stored appropriately for doing comparisons in the Data Warehouse', datedesc STRING NOT NULL COMMENT 'The date in full written form, e.g. July 7,2004', calendaryearid INT NOT NULL COMMENT 'Year number as a number', calendaryeardesc STRING NOT NULL COMMENT 'Year number as text', calendarqtrid INT NOT NULL COMMENT 'Quarter as a number, e.g. 20042', calendarqtrdesc STRING NOT NULL COMMENT 'Quarter as text, e.g. 2004 Q2', calendarmonthid INT NOT NULL COMMENT 'Month as a number, e.g. 20047', calendarmonthdesc STRING NOT NULL COMMENT 'Month as text, e.g. 2004 July', calendarweekid INT NOT NULL COMMENT 'Week as a number, e.g. 200428', calendarweekdesc STRING NOT NULL COMMENT 'Week as text, e.g. 2004-W28', dayofweeknum INT NOT NULL COMMENT 'Day of week as a number, e.g. 3', dayofweekdesc STRING NOT NULL COMMENT 'Day of week as text, e.g. Wednesday', fiscalyearid INT NOT NULL COMMENT 'Fiscal year as a number, e.g. 2005', fiscalyeardesc STRING NOT NULL COMMENT 'Fiscal year as text, e.g. 2005', fiscalqtrid INT NOT NULL COMMENT 'Fiscal quarter as a number, e.g. 20051', fiscalqtrdesc STRING NOT NULL COMMENT 'Fiscal quarter as text, e.g. 2005 Q1', holidayflag BOOLEAN COMMENT 'Indicates holidays'"
  )

