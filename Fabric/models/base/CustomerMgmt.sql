{{
    config(
        pre_hook=[
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[CustomerMgmt]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[CustomerMgmt]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[CustomerMgmt]
                        ( 
                            [customerid] [bigint]  NULL,
                            [accountid] [bigint]  NULL,
                            [brokerid] [bigint]  NULL,
                            [taxid] [varchar](20)  NULL,
                            [accountdesc] [varchar](50)  NULL,
                            [taxstatus] [int]  NULL,
                            [status] [varchar](100)  NULL,
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
                            [phone1] [varchar](100)  NULL,
                            [phone2] [varchar](100)  NULL,
                            [phone3] [varchar](100)  NULL,
                            [email1] [varchar](50)  NULL,
                            [email2] [varchar](51)  NULL,
                            [lcl_tx_id] [varchar](4)  NULL,
                            [nat_tx_id] [varchar](4)  NULL,
                            [update_ts] [datetime2](6)  NULL,
                            [ActionType] [varchar](9)  NULL
                        )"
            },        
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[CustomerMgmt]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch1/CustomerMgmt/'
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
    *
from
    {{ source('tpcdi', 'CustomerMgmt') }}
