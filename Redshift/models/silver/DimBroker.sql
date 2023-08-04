{{
    config(
        materialized = 'table'

    )
}}
--,index='CLUSTERED COLUMNSTORE INDEX'
--,dist='REPLICATE'
SELECT
    CAST(employeeid AS BIGINT) brokerid,
    CAST(managerid AS BIGINT) managerid,
    employeefirstname firstname,
    employeelastname lastname,
    employeemi middleinitial,
    employeebranch branch,
    employeeoffice office,
    employeephone phone,
    1 iscurrent,
    1 batchid,
    (
        SELECT MIN(CAST(datevalue AS DATE)) AS effectivedate
        FROM {{ ref('dimdate_view') }}
    ) effectivedate,
    CAST('9999-12-31' AS DATE) enddate,
    CONCAT(CONCAT(employeeid, '-'), '9999-12-31') AS sk_brokerid
FROM {{ ref('hr_view') }}
WHERE employeejobcode = '314'
