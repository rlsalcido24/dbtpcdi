# Fabric dbt TPC-DI benchmark project

## !!! Issues !!!
1. **tinyint** data type not supported
2. **EXTERNAL TABLES** are not supported.
3. Names are **case-sensitive**
4. datetime2 precision must be from 0 to 6.

## Data Preparation
This project uses the TPC-DI Kit TPC-DI Data Generator https://github.com/haydarai/tpcdi-kit

### Generating test data files
Using the DIGen Tool

```shell
java -jar DIGen.jar -o ../staging/10/ -sf 10
```
Once data files is generated, upload the files to your working ADLSgen2 storage account. We recommend using the following folders structure:
```
<container>/<scalefactor>/
```
Once data files are uploaded,  we need to perform some extra preparation to get it ready for Fabric Synapse Warehouse.

### FinWire files preparation
Due to Synapse external tables limitations FinWire files shall be stored in a separate folder.
Copy or move FinWire data files (excluding audit files) into a FinWire subfolder. For this you can use [azcopy](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-v10) utility.
**Note** that container, scalefactor, and subfolder names in URL are ***case-sensitive***.

```shell
azcopy login
azcopy copy https://<storageaccount>.blob.core.windows.net/<container>/<scalefactor>/Batch1/* https://<storageaccount>.blob.core.windows.net/<container>/<scalefactor>/Batch1/FinWire --exclude-pattern "*_audit.csv" --include-pattern "FINWIRE*"
```

### CustomerMgmt files preparation
Convert the CustomerMgmt.xml to tabular format as Fabric Synapse Warehouse does not support XML directly.
1. Import the notebook in the helpers directory.
2. Create and attach Spark pool, e.g. 3 to 8 Medium nodes.
3. Adjust note book parameters in **Setup** section. This includes scale factor, storage account, container, folder.
4. Execute the notebook

## Fabric Synapse Warehouse preparation
### External files access
Fabric Synapse Warehouses uses COPY INTO to ingest data from files stored in ADLSgen2. To access ADLSgen2 Fabric uses SAS token. Hence, such SAS token must be created and specified via **FABRIC_SAS** environment variable.

### Authentication
Default dbt profile [profiles.yml](../../profiles.yml) is configured to use environment variables for authentication configuration. The following environment variables are used:
- **FABRIC_SERVER** - fully qualified domain name of Fabric Synapse endpoint, must be ***.datawarehouse.pbidedicated.windows.net or similar.
- **FABRIC_DATABASE** - the name of Fabric Synapse Warehouse.
- **FABRIC_SCHEMA** - the name of a schema where TPC-DI objects, such as tables and views, will be created.
- **FABRIC_AUTH** - the authentication type. It can be **CLI** for Azure CLI authentication, **MSI** for Managed Service Identity authentication, or **SQL** for SQL basic authentication (user name and password).
Please note that in order to use MSI authentication the VM where dbt will be running must be assigned a managed identity and respective user must be registered in Fabric Synapse Warehouse.
- **FABRIC_TEST_USER** - the name of a user for SQL baseic authentication, it must not be empty for other types of authentication.
- **FABRIC_TEST_PASSWORD** - the password for SQL baseic authentication, it must not be empty for other types of authentication.
- **FABRIC_SAS** - SAS token to access data in ADLSgen2.

As an alternative you can use a non-default profile by specifying ```--profiles-dir``` parameter in dbt command line. 

## Using the project

Use the following commands:
- Initialize **dbt** project.
```shell
dbt init
```
- Test connectivity to Fabric Synapse Warehouse.
```shell
dbt debug
```
- Execute the project and actully run TPC-DI transformations.
```shell
dbt run
```

You can also specify ```--profiles-dir``` to use dbt profile other than default profile specified in [profiles.yml](./profiles.yml).
