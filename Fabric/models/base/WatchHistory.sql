{{
    config(
        pre_hook=[
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[WatchHistory]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[WatchHistory]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[WatchHistory]
                        ( 
                            [w_c_id] [bigint]  NULL,
                            [w_s_symb] [varchar](15)  NULL,
                            [w_dts] [datetime2](6)  NULL,
                            [w_action] [varchar](4)  NULL
                        )"
            },        
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[WatchHistory]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch1/WatchHistory.txt'
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
    {{ source('tpcdi', 'WatchHistory') }}


