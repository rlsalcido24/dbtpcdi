SELECT 
    md5(accountid::string) AS sk_customerid,
    *
FROM `roberto_salcido_tpcdi_dlt_advanced_10_wh`.`AccountIncremental`