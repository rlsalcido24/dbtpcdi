# Synapse dbt TPC-DI benchmark project

## Installation
In order to run dbt model on Azure Synapse Dedicated SQL Pool you need to install **dbt-synapse** adapter.
1. Install Microsoft ODBC driver 18 for SQL Server
- MacOS - [Install the Microsoft ODBC driver for SQL Server (macOS)](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos?view=sql-server-ver16)
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql18 mssql-tools18
```
- Windows - [Download ODBC Driver for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16)
- Linux - [Install the Microsoft ODBC driver for SQL Server (Linux)](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver16&tabs=alpine18-install%2Calpine17-install%2Cdebian8-install%2Credhat7-13-install%2Crhel7-offline)
2. Install **dbt-synapse** adapter
```bash
pip install dbt-synapse
```

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
Once data files are uploaded,  we need to perform some extra preparation to get it ready for Synapse Dedicated SQL Pool.

### FinWire files preparation
Due to Synapse external tables limitations FinWire files shall be stored in a separate folder.
Copy or move FinWire data files (excluding audit files) into a FinWire subfolder. For this you can use [azcopy](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-v10) utility.
**Note** that container, scalefactor, and subfolder names in URL are ***case-sensitive***.

```shell
azcopy login
azcopy copy https://<storageaccount>.blob.core.windows.net/<container>/<scalefactor>/Batch1/* https://<storageaccount>.blob.core.windows.net/<container>/<scalefactor>/Batch1/FinWire --exclude-pattern "*_audit.csv" --include-pattern "FINWIRE*"
```

### CustomerMgmt files preparation
Convert the CustomerMgmt.xml to tabular format as Synapse Dedicated SQL Pool does not support XML directly.
1. Import the notebook in the helpers directory.
2. Create and attach Spark pool, e.g. 3 to 8 Medium nodes.
3. Adjust note book parameters in **Setup** section. This includes scale factor, storage account, container, folder.
4. Execute the notebook

## Synapse preparation
### External tables
In order to Synapse being able to read data from ADLSgen2 we need to configure external data source and external file formats. For that you need to execute the following SQL-script using administrator permission. Note that it is assumed to use Managed Service Identity for authentication ADLSgen2. Therefore, you also need to assign **Storage Blob Data Reader** role for your Synapse managed service identity.
The code is also available in the helpers directory.

```sql
CREATE MASTER KEY ENCRYPTION BY PASSOWORD = '<strong_password>';
GO
CREATE DATABASE SCOPED CREDENTIAL msi_cred WITH IDENTITY = 'Managed Service Identity';
GO
CREATE EXTERNAL DATA SOURCE [AzureDataLakeStorage] WITH (TYPE = HADOOP, LOCATION = N'abfss://<container>@<storageaccount>.dfs.core.windows.net', CREDENTIAL = [msi_cred]);
GO
CREATE EXTERNAL FILE FORMAT [CsvFileFormat] WITH (FORMAT_TYPE = DELIMITEDTEXT, FORMAT_OPTIONS (FIELD_TERMINATOR = N',', FIRST_ROW = 1, USE_TYPE_DEFAULT = False));
GO
CREATE EXTERNAL FILE FORMAT [TsvFileFormat] WITH (FORMAT_TYPE = DELIMITEDTEXT, FORMAT_OPTIONS (FIELD_TERMINATOR = N'\t', FIRST_ROW = 1, USE_TYPE_DEFAULT = False));
GO
CREATE EXTERNAL FILE FORMAT [PipeFileFormat] WITH (FORMAT_TYPE = DELIMITEDTEXT, FORMAT_OPTIONS (FIELD_TERMINATOR = N'|', FIRST_ROW = 1, USE_TYPE_DEFAULT = False));
GO
```

### Workload Management
In order to use Synapse resources efficiently we need to configure Workload Management using the following script. In the script we define a login, a user, a workload group, and a workload classifier which will assign the configure workload group to all queries initiated by dbt.
In the present configuration all queries will be assigned at least 33% of resources, hence up to 3 concurrent queries. This configuration proved to be the most efficient in our tests.
The code is also available in the helpers directory.

```sql
CREATE LOGIN dbt WITH PASSWORD = '<strong_password>'     -- to be executed in master database
GO
CREATE USER dbt FOR LOGIN dbt;                           -- to be executed in actual TPC-DI database
GO

ALTER WORKLOAD GROUP wgDBT
WITH
  ( MIN_PERCENTAGE_RESOURCE = 99              
    , REQUEST_MIN_RESOURCE_GRANT_PERCENT = 33  
    , REQUEST_MAX_RESOURCE_GRANT_PERCENT = 66
    , CAP_PERCENTAGE_RESOURCE = 100
    , IMPORTANCE = HIGH
  )
GO

CREATE WORKLOAD CLASSIFIER wcDBT  
WITH  
    (   WORKLOAD_GROUP = 'wgDBT'  
    ,   MEMBERNAME = 'dbt' 
    ,   IMPORTANCE = HIGH
    )
GO
```

### Authentication
Default dbt profile [profiles.yml](../../profiles.yml) is configured to use environment variables for authentication configuration. The following environment variables are used:
- **SYNAPSE_SERVER** - fully qualified domain name of Dedicated SQL Pool endpoint, must be ***.sql.azuresynapse.net or similar.
- **SYNAPSE_DATABASE** - the name of Dedicated SQL Pool.
- **SYNAPSE_SCHEMA** - the name of a schema where TPC-DI objects, such as tables and views, will be created.
- **SYNAPSE_AUTH** - the authentication type. It can be **CLI** for Azure CLI authentication, **MSI** for Managed Service Identity authentication, or **SQL** for SQL basic authentication (user name and password).
Please note that in order to use MSI authentication the VM where dbt will be running must be assigned a managed identity and respective user must be registered in Azure Synapse Dedicated SQL Pool.
- **SYNAPSE_TEST_USER** - the name of a user for SQL baseic authentication, it must not be empty for other types of authentication.
- **SYNAPSE_TEST_PASSWORD** - the password for SQL baseic authentication, it must not be empty for other types of authentication.

You can use the following commands set environment variables.

MacOS/Linux bash:
```shell
export SYNAPSE_SERVER=***.sql.azuresynapse.net
export SYNAPSE_DATABASE=tpcdi
export SYNAPSE_SCHEMA=dbo
export SYNAPSE_AUTH=SQL
export SYNAPSE_TEST_USER=***
export SYNAPSE_TEST_PASSWORD=***
```
Windows OS command prompt:
```shell
SET SYNAPSE_SERVER=***.sql.azuresynapse.net
SET SYNAPSE_DATABASE=tpcdi
SET SYNAPSE_SCHEMA=dbo
SET SYNAPSE_AUTH=SQL
SET SYNAPSE_TEST_USER=***
SET SYNAPSE_TEST_PASSWORD=***
```
Windows OS PowerShell:
```shell
$Env:SYNAPSE_SERVER="***.sql.azuresynapse.net"
$Env:SYNAPSE_DATABASE="tpcdi"
$Env:SYNAPSE_SCHEMA="dbo"
$Env:SYNAPSE_AUTH="SQL"
$Env:SYNAPSE_TEST_USER="***"
$Env:SYNAPSE_TEST_PASSWORD="***"
```
As an alternative you can use a non-default profile by specifying ```--profiles-dir``` parameter in dbt command line. 

## Using the project

Use the following commands:
- Initialize **dbt** project.
```shell
dbt init
```
- Test connectivity to Synapse Dedicated SQL Pool.
```shell
dbt debug
```
- Configure external tables in Synapse Dedicated SQL Pool.*
```shell
dbt run-operation stage_external_sources --vars "ext_full_refresh: true"
```
- Execute the project and actully run TPC-DI transformations.
```shell
dbt run
```

You can also specify ```--profiles-dir``` to use dbt profile other than default profile specified in [profiles.yml](./profiles.yml).
