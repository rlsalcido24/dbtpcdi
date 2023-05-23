# DBTPCDI

This repo is an end to end implementation of tpcdi using dbt and databricks. Assuming dbt is installed, here is four step process to get repo up and running!

i) Update profiles.yml with your prod schema, host, token, and http_path
ii) Update sources.yml with your staging schemas
iii) dbt run-operation stage_external_sources
iv) dbt run
