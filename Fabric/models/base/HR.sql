{{
    config(
        pre_hook=[
            {
                "sql": "IF OBJECT_ID('[SF{{var('scalefactor')}}].[HR]') IS NOT NULL DROP TABLE [SF{{var('scalefactor')}}].[HR]"
            },
            {
                "sql": "CREATE TABLE [SF{{var('scalefactor')}}].[HR]
                        ( 
                            [employeeid] [bigint]  NULL,
                            [managerid] [bigint]  NULL,
                            [employeefirstname] [varchar](30)  NULL,
                            [employeelastname] [varchar](30)  NULL,
                            [employeemi] [varchar](1)  NULL,
                            [employeejobcode] [varchar](3)  NULL,
                            [employeebranch] [varchar](30)  NULL,
                            [employeeoffice] [varchar](10)  NULL,
                            [employeephone] [varchar](14)  NULL
                        )"
            },        
            {
                "sql": "COPY INTO [SF{{var('scalefactor')}}].[HR]
                        FROM 'https://{{var('storageaccountname')}}.dfs.core.windows.net/tpc-di/SF{{var('scalefactor')}}/Batch1/HR.csv'
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
    *
from
    {{ source('tpcdi', 'HR') }}
