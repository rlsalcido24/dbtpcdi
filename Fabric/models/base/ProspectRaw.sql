{{
    config(
        pre_hook=[
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[ProspectRaw1]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[ProspectRaw1]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[ProspectRaw1]
                        ( 
                            [agencyid] [varchar](30)  NULL,
                            [lastname] [varchar](30)  NULL,
                            [firstname] [varchar](30)  NULL,
                            [middleinitial] [varchar](1)  NULL,
                            [gender] [varchar](1)  NULL,
                            [addressline1] [varchar](80)  NULL,
                            [addressline2] [varchar](80)  NULL,
                            [postalcode] [varchar](12)  NULL,
                            [city] [varchar](25)  NULL,
                            [state] [varchar](20)  NULL,
                            [country] [varchar](24)  NULL,
                            [phone] [varchar](30)  NULL,
                            [income] [int]  NULL,
                            [numbercars] [int]  NULL,
                            [numberchildren] [int]  NULL,
                            [maritalstatus] [varchar](1)  NULL,
                            [age] [int]  NULL,
                            [creditrating] [int]  NULL,
                            [ownorrentflag] [varchar](1)  NULL,
                            [employer] [varchar](30)  NULL,
                            [numbercreditcards] [int]  NULL,
                            [networth] [int]  NULL
                        )"
            },
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[ProspectRaw2]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[ProspectRaw2]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[ProspectRaw2]
                        ( 
                            [agencyid] [varchar](30)  NULL,
                            [lastname] [varchar](30)  NULL,
                            [firstname] [varchar](30)  NULL,
                            [middleinitial] [varchar](1)  NULL,
                            [gender] [varchar](1)  NULL,
                            [addressline1] [varchar](80)  NULL,
                            [addressline2] [varchar](80)  NULL,
                            [postalcode] [varchar](12)  NULL,
                            [city] [varchar](25)  NULL,
                            [state] [varchar](20)  NULL,
                            [country] [varchar](24)  NULL,
                            [phone] [varchar](30)  NULL,
                            [income] [int]  NULL,
                            [numbercars] [int]  NULL,
                            [numberchildren] [int]  NULL,
                            [maritalstatus] [varchar](1)  NULL,
                            [age] [int]  NULL,
                            [creditrating] [int]  NULL,
                            [ownorrentflag] [varchar](1)  NULL,
                            [employer] [varchar](30)  NULL,
                            [numbercreditcards] [int]  NULL,
                            [networth] [int]  NULL
                        )"
            },
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[ProspectRaw3]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[ProspectRaw3]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[ProspectRaw3]
                        ( 
                            [agencyid] [varchar](30)  NULL,
                            [lastname] [varchar](30)  NULL,
                            [firstname] [varchar](30)  NULL,
                            [middleinitial] [varchar](1)  NULL,
                            [gender] [varchar](1)  NULL,
                            [addressline1] [varchar](80)  NULL,
                            [addressline2] [varchar](80)  NULL,
                            [postalcode] [varchar](12)  NULL,
                            [city] [varchar](25)  NULL,
                            [state] [varchar](20)  NULL,
                            [country] [varchar](24)  NULL,
                            [phone] [varchar](30)  NULL,
                            [income] [int]  NULL,
                            [numbercars] [int]  NULL,
                            [numberchildren] [int]  NULL,
                            [maritalstatus] [varchar](1)  NULL,
                            [age] [int]  NULL,
                            [creditrating] [int]  NULL,
                            [ownorrentflag] [varchar](1)  NULL,
                            [employer] [varchar](30)  NULL,
                            [numbercreditcards] [int]  NULL,
                            [networth] [int]  NULL
                        )"
            },
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[ProspectRaw1]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch1/Prospect.csv'
                        WITH (
                            CREDENTIAL=(IDENTITY= 'Shared Access Signature', SECRET='{{ env_var('FABRIC_SAS') }}')
                            , FIELDTERMINATOR =  ',' 
                        )"
            },
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[ProspectRaw2]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch2/Prospect.csv'
                        WITH (
                            CREDENTIAL=(IDENTITY= 'Shared Access Signature', SECRET='{{ env_var('FABRIC_SAS') }}')
                            , FIELDTERMINATOR =  ',' 
                        )"
            },
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[ProspectRaw3]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch3/Prospect.csv'
                        WITH (
                            CREDENTIAL=(IDENTITY= 'Shared Access Signature', SECRET='{{ env_var('FABRIC_SAS') }}')
                            , FIELDTERMINATOR =  ',' 
                        )"
            }
        ]
        , materialized = 'view'
    )
}}

select
    *,
    1 as batchid
from
    {{ source('tpcdi', 'ProspectRaw1') }}

 UNION ALL

select
    *,
    2 as batchid
from
    {{ source('tpcdi', 'ProspectRaw2') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{ source('tpcdi', 'ProspectRaw3') }}