{{
    config(
        pre_hook=[
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[StatusType]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[StatusType]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[StatusType]
                        ( 
                            [st_id] [char](4)  NULL,
                            [st_name] [char](10)  NULL
                        )"
            },        
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[StatusType]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch1/StatusType.txt'
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
    {{ source('tpcdi', 'StatusType') }}
