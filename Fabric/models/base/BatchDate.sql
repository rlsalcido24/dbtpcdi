{{
    config(
        pre_hook=[
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[BatchDate1]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[BatchDate1]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[BatchDate1]
                        ( 
                            [batchdate] [date]  NULL
                        )"
            },
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[BatchDate2]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[BatchDate2]"
            },
            {
                "sql": "CREATE TABLE [SF10].[BatchDate2]
                        ( 
                            [batchdate] [date]  NULL
                        )"
            },
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[BatchDate3]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[BatchDate3]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[BatchDate3]
                        ( 
                            [batchdate] [date]  NULL
                        )"
            },
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[BatchDate1]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch1/BatchDate.txt'
                        WITH (
                            CREDENTIAL=(IDENTITY= 'Shared Access Signature', SECRET='{{ env_var('FABRIC_SAS') }}')
                            , FIELDTERMINATOR =  '|' 
                        )"
            },
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[BatchDate2]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch2/BatchDate.txt'
                        WITH (
                            CREDENTIAL=(IDENTITY= 'Shared Access Signature', SECRET='{{ env_var('FABRIC_SAS') }}')
                            , FIELDTERMINATOR =  '|' 
                        )"
            },
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[BatchDate3]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch3/BatchDate.txt'
                        WITH (
                            CREDENTIAL=(IDENTITY= 'Shared Access Signature', SECRET='{{ env_var('FABRIC_SAS') }}')
                            , FIELDTERMINATOR =  '|' 
                        )"
            }
        ]
        ,materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='REPLICATE'
    )
}}



select
    *,
    1 as batchid
from
    {{ source('tpcdi', 'BatchDate1') }}

 UNION ALL

select
    *,
    2 as batchid
from
    {{ source('tpcdi', 'BatchDate2') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{ source('tpcdi', 'BatchDate3') }}
