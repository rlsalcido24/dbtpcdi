{{
    config(
        materialized = 'view'
    )
}}
select
    *
from
    tpcdi_sf1000.rsprodtest.CustomerXML

