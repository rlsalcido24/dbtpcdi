{{
    config(
        pre_hook=[
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[HoldingIncremental2]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[HoldingIncremental2]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[HoldingIncremental2]
                        ( 
                            [cdc_flag] [varchar](1)  NULL,
                            [cdc_dsn] [bigint]  NULL,
                            [hh_h_t_id] [int]  NULL,
                            [hh_t_id] [int]  NULL,
                            [hh_before_qty] [int]  NULL,
                            [hh_after_qty] [int]  NULL
                        )"
            },
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[HoldingIncremental3]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[HoldingIncremental3]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[HoldingIncremental3]
                        ( 
                            [cdc_flag] [varchar](1)  NULL,
                            [cdc_dsn] [bigint]  NULL,
                            [hh_h_t_id] [int]  NULL,
                            [hh_t_id] [int]  NULL,
                            [hh_before_qty] [int]  NULL,
                            [hh_after_qty] [int]  NULL
                        )"
            },
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[HoldingIncremental2]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch2/HoldingHistory.txt'
                        WITH (
                            CREDENTIAL=(IDENTITY= 'Shared Access Signature', SECRET='{{ env_var('FABRIC_SAS') }}')
                            , FIELDTERMINATOR =  '|' 
                        )"
            },
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[HoldingIncremental3]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch3/HoldingHistory.txt'
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
    {{ source('tpcdi', 'HoldingIncremental2') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{ source('tpcdi', 'HoldingIncremental3') }}

