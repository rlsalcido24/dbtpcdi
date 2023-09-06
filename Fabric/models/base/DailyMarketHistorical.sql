{{
    config(
        pre_hook=[
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[DailyMarketHistorical]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[DailyMarketHistorical]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[DailyMarketHistorical]
                        ( 
                            [dm_date] [date]  NULL,
                            [dm_s_symb] [varchar](15)  NULL,
                            [dm_close] [decimal](8,2)  NULL,
                            [dm_high] [decimal](8,2)  NULL,
                            [dm_low] [decimal](8,2)  NULL,
                            [dm_vol] [int]  NULL
                        )"
            },
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[DailyMarketHistorical]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch1/DailyMarket.txt'
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
    1 as batchid
from
    {{ source('tpcdi', 'DailyMarketHistorical') }}