

SELECT
  md5(employeeid) as sk_brokerid,
  cast(employeeid as BIGINT) brokerid,
  cast(managerid as BIGINT) managerid,
  employeefirstname firstname,
  employeelastname lastname,
  employeemi middleinitial,
  employeebranch branch,
  employeeoffice office,
  employeephone phone,
  true iscurrent,
  1 batchid,
  (SELECT min(to_date(datevalue)) as effectivedate FROM `roberto_salcido_tpcdi_dlt_advanced_10_wh`.`DimDate`) effectivedate,
  date('9999-12-31') enddate
FROM  `roberto_salcido_tpcdi_dlt_advanced_10_wh`.`HR`
WHERE employeejobcode = 314