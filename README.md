# DBTPCDI

This repo is an end to end implementation of tpcdi using dbt and snowflake. Assuming dbt is installed, here is four step process to get repo up and running! 

Pre-reqs
This assumes that you have a stage created to read files from and that you have created the apporpriate file formats (and that the stages/file formats reside in the staging database.schema). Also assumes that the xml table already exists and catalog.schema.table is hard coded in base/customermgmtview.sql

i) Update profiles.yml with your creds and prod database and schema \
ii) Update sources.yml with your staging database and schema \
iii) dbt run-operation stage_external_sources to stage raw tables \
iv) dbt run to build silve and gold tables
