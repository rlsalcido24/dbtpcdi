{{
    config(
        pre_hook=[
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[AccountIncremental2]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[AccountIncremental2]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[AccountIncremental2]
                        ( 
                            [cdc_flag] [varchar](1)  NULL,
                            [cdc_dsn] [bigint]  NULL,
                            [accountid] [bigint]  NULL,
                            [ca_b_id] [bigint]  NULL,
                            [ca_c_id] [bigint]  NULL,
                            [accountDesc] [varchar](50)  NULL,
                            [taxstatus] [int]  NULL,
                            [ca_st_id] [varchar](4)  NULL
                        )"
            },
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[AccountIncremental3]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[AccountIncremental3]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[AccountIncremental3]
                        ( 
                            [cdc_flag] [varchar](1)  NULL,
                            [cdc_dsn] [bigint]  NULL,
                            [accountid] [bigint]  NULL,
                            [ca_b_id] [bigint]  NULL,
                            [ca_c_id] [bigint]  NULL,
                            [accountDesc] [varchar](50)  NULL,
                            [taxstatus] [int]  NULL,
                            [ca_st_id] [varchar](4)  NULL
                        )"
            },
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[AccountIncremental2]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch2/Account.txt'
                        WITH (
                            CREDENTIAL=(IDENTITY= 'Shared Access Signature', SECRET='{{ env_var('FABRIC_SAS') }}')
                            , FIELDTERMINATOR =  '|' 
                        )"
            },
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[AccountIncremental3]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch3/Account.txt'
                        WITH (
                            CREDENTIAL=(IDENTITY= 'Shared Access Signature', SECRET='{{ env_var('FABRIC_SAS') }}')
                            , FIELDTERMINATOR =  '|' 
                        )"
            }
        ]
        ,materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='HASH(accountid)'
    )
}}


select
    *,
    2 as batchid
from
    {{ source('tpcdi', 'AccountIncremental2') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{ source('tpcdi', 'AccountIncremental3') }}
