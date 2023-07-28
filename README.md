# DBTPCDI

This repo is an end to end implementation of [TPC-DI](https://www.tpc.org/tpcdi/default5.asp). The repo is desinged to be run against Databricks, Snowflake, BigQuery, Redshift, and Synapse. You can select your desired target warehouse with different targets that are defined in the profiles.yml; for ex dbt run --target databricks --select databricks will build the TPCDI tables in Databricks while dbt run --target redshift --select redshift will build the tables in Redshift. The steps below will be applicable regardless of the target warehouse. For warehouse specific considerations, refer to the read.me within the relevant directories of the models directory. 

## Data Preparation
This project uses the TPC-DI Kit TPC-DI Data Generator https://github.com/haydarai/tpcdi-kit

### Generating test data files
Using the DIGen Tool

```shell
java -jar DIGen.jar -o ../staging/10/ -sf 10
```
Once data files is generated, upload the files to your working cloud storage account. We recommend using the cli tool for the relevant cloud (aws CLI for Redshift, azure SLI for synapse, gcloud sli for big query, etc). 

Please refer to the individual readmes to determine how to best handle the customermgmt xml file.


### Configuration and running

(Note that do not run dbt deps as some of the packages/macros the code relies on are custom)

i) create your prod and staging schemas (if not already created) \
ii) Update profiles.yml with your prod schema and other warehouse specific configurations. \
iii) Update sources.yml with your staging schema\
iv) Update project.yml with desired scalefactor and bucketname \
v) dbt run-operation stage_external_sources \
vi) dbt run
