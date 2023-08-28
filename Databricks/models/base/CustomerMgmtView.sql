{{
    config(
        materialized = 'view'
    )
}}
select
    *
from
    hive_metastore.{{var('stagingschema')}}.CustomerMgmt

