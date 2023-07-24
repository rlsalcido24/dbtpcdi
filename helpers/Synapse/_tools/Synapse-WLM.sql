-- Make sure to specify password for the login and execute the queries in the right context

CREATE LOGIN dbt WITH PASSWORD = '<strong_password>'     -- to be executed in master database
GO

CREATE USER dbt FOR LOGIN dbt;                      -- to be executed in actual TPC-DI database
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