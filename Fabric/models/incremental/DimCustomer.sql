{{
    config(
        materialized = 'table'
        ,index='CLUSTERED COLUMNSTORE INDEX'
        ,dist='HASH(customerid)'
    )
}}


SELECT 
  c.sk_customerid,
  c.customerid,
  c.taxid,
  c.status,
  c.lastname,
  c.firstname,
  c.middleinitial,
  case when c.gender IN ('M', 'F') then c.gender else 'U' end gender,
  c.tier,
  c.dob,
  c.addressline1,
  c.addressline2,
  c.postalcode,
  c.city,
  c.stateprov,
  c.country,
  c.phone1,
  c.phone2,
  c.phone3,
  c.email1,
  c.email2,
  r_nat.tx_name as nationaltaxratedesc,
  r_nat.tx_rate as nationaltaxrate,
  r_lcl.tx_name as localtaxratedesc,
  r_lcl.tx_rate as localtaxrate,
  p.agencyid,
  p.creditrating,
  p.networth,
  p.marketingnameplate,
  c.iscurrent,
  c.batchid,
  c.effectivedate,
  c.enddate
FROM {{ ref('DimCustomerStg') }} c
LEFT JOIN {{ ref('TaxRate') }} r_lcl 
  ON c.LCL_TX_ID = r_lcl.tx_id
LEFT JOIN {{ ref('TaxRate') }} r_nat 
  ON c.nat_tx_id = r_nat.tx_id
LEFT JOIN {{ ref('Prospect') }} p 
  on upper(p.lastname) = upper(c.lastname)
  and upper(p.firstname) = upper(c.firstname)
  and upper(p.addressline1) = upper(c.addressline1)
  and upper(isnull(p.addressline2, '')) = upper(isnull(c.addressline2, ''))
  and upper(p.postalcode) = upper(c.postalcode);
