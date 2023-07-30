# DBTPCDI

This repo is an end to end implementation of
[TPC-DI](https://www.tpc.org/tpcdi/default5.asp). The repo is desinged to
be run against Databricks, Snowflake, BigQuery, Redshift, and Synapse.
You can select your desired target warehouse with different targets that
are defined in the profiles.yml; for ex `dbt run --target databricks --select databricks`
will build the TPCDI tables in Databricks while `dbt run --target redshift --select redshift`
will build the tables in Redshift. The steps below will be applicable
regardless of the target warehouse. For warehouse specific considerations,
refer to the read.me within the relevant directories of the models
directory. 

## Data Preparation
This project uses the TPC-DI Kit TPC-DI Data Generator https://github.com/haydarai/tpcdi-kit

### Generating test data files
Using the DIGen Tool

```shell
java -jar DIGen.jar -o ../staging/10/ -sf 10
```
Once data files is generated, upload the files to your working cloud
storage account. We recommend using the cli tool for the relevant cloud
(aws CLI for Redshift, azure SLI for synapse, gcloud sli for big query,
etc). 

Please refer to the individual readmes to determine how to best handle the
customermgmt xml file.


### Configuration and running

1. create your prod and staging schemas (if not already created)
2. Update profiles.yml with your prod schema and other warehouse specific
configurations.
3. Update sources.yml with your staging schema
4. Update project.yml with desired scalefactor and bucketname
5. dbt run-operation stage_external_sources
6. dbt run

### Contributing

Additional contributions are welcome.

For small changes, please submit a pull request with your changes.
For larger changes that would change the majority of code in a single
file or span multiple files, please open an issue first for discussion.

#### Linting

This project uses [yamllint](https://yamllint.readthedocs.io/en/stable/)
to lint the yaml files. To run the linter, run `yamllint .` from the root
of the project. Errors and warnings will be printed to the console for review.

These will need to be resolved or justified before a pull request can be merged.

Instructions to install yamllint can be found [here](https://yamllint.readthedocs.io/en/stable/quickstart.html#installing-yamllint).
