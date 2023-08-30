{{
    config(
        pre_hook=[
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[Industry]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[Industry]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[Industry]
                        ( 
                            [in_id] [varchar](2)  NULL,
                            [in_name] [varchar](50)  NULL,
                            [in_sc_id] [varchar](4)  NULL
                        )"
            },        
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[Industry]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch1/Industry.txt'
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
    {{ source('tpcdi', 'Industry') }}
