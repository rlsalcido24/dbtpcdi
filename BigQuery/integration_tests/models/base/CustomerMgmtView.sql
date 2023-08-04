{{
    config(
        materialized = 'view'
    )
}}
select
    *
from
    fe-dev-sandbox.tpcdi_eu.CustomerMgmt_{{var('benchmark')}}
    

