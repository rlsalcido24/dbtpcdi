{{
    config(
        pre_hook=[
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[DimDate]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[DimDate]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[DimDate]
                        ( 
                            [sk_dateid] [bigint]  NULL,
                            [datevalue] [date]  NULL,
                            [datedesc] [varchar](20)  NULL,
                            [calendaryearid] [int]  NULL,
                            [calendaryeardesc] [varchar](20)  NULL,
                            [calendarqtrid] [int]  NULL,
                            [calendarqtrdesc] [varchar](20)  NULL,
                            [calendarmonthid] [int]  NULL,
                            [calendarmonthdesc] [varchar](20)  NULL,
                            [calendarweekid] [int]  NULL,
                            [calendarweekdesc] [varchar](20)  NULL,
                            [dayofweeknum] [int]  NULL,
                            [dayofweekdesc] [varchar](20)  NULL,
                            [fiscalyearid] [int]  NULL,
                            [fiscalyeardesc] [varchar](20)  NULL,
                            [fiscalqtrid] [int]  NULL,
                            [fiscalqtrdesc] [varchar](20)  NULL,
                            [holidayflag] [bit]  NULL
                        )"
            },
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[DimDate]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch1/Date.txt'
                        WITH (
                            CREDENTIAL=(IDENTITY= 'Shared Access Signature', SECRET='{{ env_var('FABRIC_SAS') }}')
                            , FIELDTERMINATOR =  '|' 
                        )"
            }
        ]
        , materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='REPLICATE'
    )
}}



select
    *
from
    {{ source('tpcdi', 'DimDate') }}
