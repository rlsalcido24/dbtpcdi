{{
    config(
        pre_hook=[
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[TaxRate]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[TaxRate]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[TaxRate]
                        ( 
                            [tx_id] [varchar](4)  NULL,
                            [tx_name] [varchar](50)  NULL,
                            [tx_rate] [decimal](6,5)  NULL
                        )"
            },        
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[TaxRate]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch1/TaxRate.txt'
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
    {{ source('tpcdi', 'TaxRate') }}
