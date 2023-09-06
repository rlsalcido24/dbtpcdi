{{
    config(
        pre_hook=[
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[TradeHistory]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[TradeHistory]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[TradeHistory]
                        ( 
                            [t_id] [bigint]  NULL,
                            [t_dts] [datetime2](6)  NULL,
                            [t_st_id] [varchar](4)  NULL,
                            [t_tt_id] [varchar](3)  NULL,
                            [t_is_cash] [int]  NULL,
                            [t_s_symb] [varchar](15)  NULL,
                            [t_qty] [int]  NULL,
                            [t_bid_price] [decimal](8,2)  NULL,
                            [t_ca_id] [bigint]  NULL,
                            [t_exec_name] [varchar](49)  NULL,
                            [t_trade_price] [decimal](8,2)  NULL,
                            [t_chrg] [decimal](10,2)  NULL,
                            [t_comm] [decimal](10,2)  NULL,
                            [t_tax] [decimal](10,2)  NULL
                        )"
            },        
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[TradeHistory]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch1/Trade.txt'
                        WITH (
                            CREDENTIAL=(IDENTITY= 'Shared Access Signature', SECRET='{{ env_var('FABRIC_SAS') }}')
                            , FIELDTERMINATOR =  '|' 
                        )"
            }
        ]
        ,materialized = 'view'
    )
}}

        
select
    *
from
    {{ source('tpcdi', 'TradeHistory') }}


