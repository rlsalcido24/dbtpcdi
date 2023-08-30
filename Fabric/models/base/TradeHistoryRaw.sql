{{
    config(
        pre_hook=[
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[TradeHistoryRaw]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[TradeHistoryRaw]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[TradeHistoryRaw]
                        ( 
                            [th_t_id] [bigint]  NULL,
                            [th_dts] [datetime2](6)  NULL,
                            [th_st_id] [varchar](4)  NULL
                        )"
            },        
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[TradeHistoryRaw]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch1/TradeHistory.txt'
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
    *
from
    {{ source('tpcdi', 'TradeHistoryRaw') }}


