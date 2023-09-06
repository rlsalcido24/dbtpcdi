{{
    config(
        pre_hook=[
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[TradeType]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[TradeType]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[TradeType]
                        ( 
                            [tt_id] [varchar](3)  NULL,
                            [tt_name] [varchar](12)  NULL,
                            [tt_is_sell] [int]  NULL,
                            [tt_is_mrkt] [int]  NULL
                        )"
            },        
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[TradeType]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch1/TradeType.txt'
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
    {{ source('tpcdi', 'TradeType') }}
