{{
    config(
        pre_hook=[
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[FinWireStg]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[FinWireStg]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[FinWireStg]
                        ( 
	                        [value] [varchar](4000)  NULL
                        )"
            },
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[FinWireStg]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch1/FinWire/'
                        WITH (
                            CREDENTIAL=(IDENTITY= 'Shared Access Signature', SECRET='{{ env_var('FABRIC_SAS') }}')
                            , FIELDTERMINATOR =  '\t' 
                        )"
            }
        ]
        , materialized = 'view',
        partition_by = 'rectype'
    )
}}

select
    [value],
    SUBSTRING([value],16,3) as rectype
from
    {{ source('tpcdi', 'FinWireStg') }}
