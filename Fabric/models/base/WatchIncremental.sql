{{
    config(
        pre_hook=[
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[WatchIncremental2]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[WatchIncremental2]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[WatchIncremental2]
                        ( 
                            [cdc_flag] [varchar](1)  NULL,
                            [cdc_dsn] [bigint]  NULL,
                            [w_c_id] [bigint]  NULL,
                            [w_s_symb] [varchar](15)  NULL,
                            [w_dts] [datetime2](6)  NULL,
                            [w_action] [varchar](4)  NULL
                        )"
            },        
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[WatchIncremental3]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[WatchIncremental3]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[WatchIncremental3]
                        ( 
                            [cdc_flag] [varchar](1)  NULL,
                            [cdc_dsn] [bigint]  NULL,
                            [w_c_id] [bigint]  NULL,
                            [w_s_symb] [varchar](15)  NULL,
                            [w_dts] [datetime2](6)  NULL,
                            [w_action] [varchar](4)  NULL
                        )"
            },            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[WatchIncremental2]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch2/WatchHistory.txt'
                        WITH (
                            CREDENTIAL=(IDENTITY= 'Shared Access Signature', SECRET='{{ env_var('FABRIC_SAS') }}')
                            , FIELDTERMINATOR =  '|' 
                        )"
            },        
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[WatchIncremental3]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch3/WatchHistory.txt'
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
    {{ source('tpcdi', 'WatchIncremental2') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{ source('tpcdi', 'WatchIncremental3') }}

