{{
    config(
        pre_hook=[
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[CashTransactionIncremental2]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[CashTransactionIncremental2]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[CashTransactionIncremental2]
                        ( 
                            [cdc_flag] [varchar](1)  NULL,
                            [cdc_dsn] [bigint]  NULL,
                            [ct_ca_id] [bigint]  NULL,
                            [ct_dts] [datetime2](6)  NULL,
                            [ct_amt] [decimal](10,2)  NULL,
                            [ct_name] [varchar](100)  NULL
                        )"
            },
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[CashTransactionIncremental3]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[CashTransactionIncremental3]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[CashTransactionIncremental3]
                        ( 
                            [cdc_flag] [varchar](1)  NULL,
                            [cdc_dsn] [bigint]  NULL,
                            [ct_ca_id] [bigint]  NULL,
                            [ct_dts] [datetime2](6)  NULL,
                            [ct_amt] [decimal](10,2)  NULL,
                            [ct_name] [varchar](100)  NULL
                        )"
            },
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[CashTransactionIncremental2]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch2/CashTransaction.txt'
                        WITH (
                            CREDENTIAL=(IDENTITY= 'Shared Access Signature', SECRET='{{ env_var('FABRIC_SAS') }}')
                            , FIELDTERMINATOR =  '|' 
                        )"
            },
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[CashTransactionIncremental3]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch3/CashTransaction.txt'
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
    {{ source('tpcdi', 'CashTransactionIncremental2') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{ source('tpcdi', 'CashTransactionIncremental3') }}

