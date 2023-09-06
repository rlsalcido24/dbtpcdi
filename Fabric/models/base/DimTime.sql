{{
    config(
        pre_hook=[
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[DimTime]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[DimTime]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[DimTime]
                        ( 
                            [sk_timeid] [bigint]  NULL,
                            [timevalue] [time](6)  NULL,
                            [hourid] [int]  NULL,
                            [hourdesc] [varchar](20)  NULL,
                            [minuteid] [int]  NULL,
                            [minutedesc] [varchar](20)  NULL,
                            [secondid] [int]  NULL,
                            [seconddesc] [varchar](20)  NULL,
                            [markethoursflag] [bit]  NULL,
                            [officehoursflag] [bit]  NULL
                        )"
            },
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[DimTime]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch1/Time.txt'
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
    {{ source('tpcdi', 'DimTime') }}
