# DBTPCDI

The goal of this repo is to collaborate to refactor the DLT code in tpc-di repo to be .sql DBT files. Once the refactoring is complete we can run this repo as a task post DLT ingestion to facilitate transformations. You can build the models locally as well assuming you have dbt-core on your local and a profiles.yml file with appropriate credentials. Feel free to fork the repo and submit PR's for any of the 18 models that we need to refactor-- thanks!!

remaining action items:

i) dbt unit tests/ column descriptions
ii) verify table contents match
