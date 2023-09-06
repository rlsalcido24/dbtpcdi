{{
    config(
        pre_hook=[
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[CustomerIncremental2]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[CustomerIncremental2]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[CustomerIncremental2]
                        ( 
                            [cdc_flag] [varchar](1)  NULL,
                            [cdc_dsn] [bigint]  NULL,
                            [customerid] [bigint]  NULL,
                            [taxid] [varchar](20)  NULL,
                            [status] [varchar](4)  NULL,
                            [lastname] [varchar](25)  NULL,
                            [firstname] [varchar](20)  NULL,
                            [middleinitial] [varchar](1)  NULL,
                            [gender] [varchar](1)  NULL,
                            [tier] [int]  NULL,
                            [dob] [date]  NULL,
                            [addressline1] [varchar](80)  NULL,
                            [addressline2] [varchar](80)  NULL,
                            [postalcode] [varchar](12)  NULL,
                            [city] [varchar](25)  NULL,
                            [stateprov] [varchar](20)  NULL,
                            [country] [varchar](24)  NULL,
                            [c_ctry_1] [varchar](3)  NULL,
                            [c_area_1] [varchar](3)  NULL,
                            [c_local_1] [varchar](10)  NULL,
                            [c_ext_1] [varchar](5)  NULL,
                            [c_ctry_2] [varchar](3)  NULL,
                            [c_area_2] [varchar](3)  NULL,
                            [c_local_2] [varchar](10)  NULL,
                            [c_ext_2] [varchar](5)  NULL,
                            [c_ctry_3] [varchar](3)  NULL,
                            [c_area_3] [varchar](3)  NULL,
                            [c_local_3] [varchar](10)  NULL,
                            [c_ext_3] [varchar](5)  NULL,
                            [email1] [varchar](50)  NULL,
                            [email2] [varchar](50)  NULL,
                            [lcl_tx_id] [varchar](4)  NULL,
                            [nat_tx_id] [varchar](4)  NULL
                        )"
            },
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[CustomerIncremental3]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[CustomerIncremental3]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[CustomerIncremental3]
                        ( 
                           [cdc_flag] [varchar](1)  NULL,
                            [cdc_dsn] [bigint]  NULL,
                            [customerid] [bigint]  NULL,
                            [taxid] [varchar](20)  NULL,
                            [status] [varchar](4)  NULL,
                            [lastname] [varchar](25)  NULL,
                            [firstname] [varchar](20)  NULL,
                            [middleinitial] [varchar](1)  NULL,
                            [gender] [varchar](1)  NULL,
                            [tier] [int]  NULL,
                            [dob] [date]  NULL,
                            [addressline1] [varchar](80)  NULL,
                            [addressline2] [varchar](80)  NULL,
                            [postalcode] [varchar](12)  NULL,
                            [city] [varchar](25)  NULL,
                            [stateprov] [varchar](20)  NULL,
                            [country] [varchar](24)  NULL,
                            [c_ctry_1] [varchar](3)  NULL,
                            [c_area_1] [varchar](3)  NULL,
                            [c_local_1] [varchar](10)  NULL,
                            [c_ext_1] [varchar](5)  NULL,
                            [c_ctry_2] [varchar](3)  NULL,
                            [c_area_2] [varchar](3)  NULL,
                            [c_local_2] [varchar](10)  NULL,
                            [c_ext_2] [varchar](5)  NULL,
                            [c_ctry_3] [varchar](3)  NULL,
                            [c_area_3] [varchar](3)  NULL,
                            [c_local_3] [varchar](10)  NULL,
                            [c_ext_3] [varchar](5)  NULL,
                            [email1] [varchar](50)  NULL,
                            [email2] [varchar](50)  NULL,
                            [lcl_tx_id] [varchar](4)  NULL,
                            [nat_tx_id] [varchar](4)  NULL
                        )"
            },            
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[CustomerIncremental2]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch2/Customer.txt'
                        WITH (
                            CREDENTIAL=(IDENTITY= 'Shared Access Signature', SECRET='{{ env_var('FABRIC_SAS') }}')
                            , FIELDTERMINATOR =  '|' 
                        )"
            },            
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[CustomerIncremental3]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch3/Customer.txt'
                        WITH (
                            CREDENTIAL=(IDENTITY= 'Shared Access Signature', SECRET='{{ env_var('FABRIC_SAS') }}')
                            , FIELDTERMINATOR =  '|' 
                        )"
            }
        ]
        , materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='HASH(customerid)'
    )  
}}


select
    *,
    2 as batchid
from
    {{ source('tpcdi', 'CustomerIncremental2') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{ source('tpcdi', 'CustomerIncremental3') }}
