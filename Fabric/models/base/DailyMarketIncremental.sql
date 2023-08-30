{{
    config(
        pre_hook=[
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[DailyMarketIncremental2]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[DailyMarketIncremental2]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[DailyMarketIncremental2]
                        ( 
                             [cdc_flag] [varchar](1)  NULL,
                            [cdc_dsn] [bigint]  NULL,
                            [dm_date] [date]  NULL,
                            [dm_s_symb] [varchar](15)  NULL,
                            [dm_close] [decimal](8,2)  NULL,
                            [dm_high] [decimal](8,2)  NULL,
                            [dm_low] [decimal](8,2)  NULL,
                            [dm_vol] [int]  NULL
                        )"
            },
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[DailyMarketIncremental3]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[DailyMarketIncremental3]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[DailyMarketIncremental3]
                        ( 
                            [cdc_flag] [varchar](1)  NULL,
                            [cdc_dsn] [bigint]  NULL,
                            [dm_date] [date]  NULL,
                            [dm_s_symb] [varchar](15)  NULL,
                            [dm_close] [decimal](8,2)  NULL,
                            [dm_high] [decimal](8,2)  NULL,
                            [dm_low] [decimal](8,2)  NULL,
                            [dm_vol] [int]  NULL
                        )"
            },
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[DailyMarketIncremental2]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch2/DailyMarket.txt'
                        WITH (
                            CREDENTIAL=(IDENTITY= 'Shared Access Signature', SECRET='{{ env_var('FABRIC_SAS') }}')
                            , FIELDTERMINATOR =  '|' 
                        )"
            },
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[DailyMarketIncremental3]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch3/DailyMarket.txt'
                        WITH (
                            CREDENTIAL=(IDENTITY= 'Shared Access Signature', SECRET='{{ env_var('FABRIC_SAS') }}')
                            , FIELDTERMINATOR =  '|' 
                        )"
            }
        ]
        , materialized = 'view'
    )
}}


select
    *,
    2 as batchid
from
    {{ source('tpcdi', 'DailyMarketIncremental2') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{ source('tpcdi', 'DailyMarketIncremental3') }}

