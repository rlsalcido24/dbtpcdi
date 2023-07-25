# **Welcome to dbt redshift benchmark**

## **Using the project**
This project uses the TPC-DI Kit
TPC-DI Data Generator
https://github.com/haydarai/tpcdi-kit

Using the DIGen Tool

`java -jar DIGen.jar -o ../staging/10/ -sf 10`

Using this data we need to do some basic prep to get it ready for Redshift Spectrum


Navigate to root folder of the data and run the following command

OSX
`./tools/raw_file_preprocess.sh`


Finally we need to convert the CustomerMgmt.xml to csv as Redshift does not support XML Directly.

Using `python tools/dataprep_v2.py {inputfile.xml} {outputfile.csv}`

Command `python tools/dataprep_v2.py CustomerMgmt.xml CustomerMgmt.csv`

Delete the original XML file.

## **AWS Resources**
If you do not have AWS resources configured in your account you can use the Cloudformation Stack as part of this Repo.

It will deploy a Redshift cluster into the default VPC with default Security group. You may need to change the ingress rules depending on where you are executing DBT from.

**Connect to Redshift Cluster via AWS Console**

create the database `tpcdi`


In Query Editor run
`create external schema ext_stage from data catalog 
database 'tpcdi' 
iam_role 'arn:aws:iam::{aws account}:role/RS-Spectrum-Role'
create external database if not exists;`



## **DBT**
Try running the following commands:
- dbt init : *configure your taget warehouse*
- dbt debug : *test connectivity to warehouse*
- dbt run-operation stage_external_sources 
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
