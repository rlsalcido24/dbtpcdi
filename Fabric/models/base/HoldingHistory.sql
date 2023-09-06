{{
    config(
        pre_hook=[
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[HoldingHistory]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[HoldingHistory]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[HoldingHistory]
                        ( 
                            [hh_h_t_id] [int]  NULL,
                            [hh_t_id] [int]  NULL,
                            [hh_before_qty] [int]  NULL,
                            [hh_after_qty] [int]  NULL
                        )"
            },
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[HoldingHistory]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch1/HoldingHistory.txt'
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
    {{ source('tpcdi', 'HoldingHistory') }}
