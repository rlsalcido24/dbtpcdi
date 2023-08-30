{{
    config(
        pre_hook=[
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[CashTransactionHistory]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[CashTransactionHistory]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[CashTransactionHistory]
                        ( 
                            [ct_ca_id] [bigint]  NULL,
                            [ct_dts] [datetime2](6)  NULL,
                            [ct_amt] [decimal](10,2)  NULL,
                            [ct_name] [varchar](100)  NULL
                        )"
            },
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[CashTransactionHistory]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch1/CashTransaction.txt'
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
    {{ source('tpcdi', 'CashTransactionHistory') }}


