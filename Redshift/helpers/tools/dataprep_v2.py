import csv
import xml.etree.ElementTree as ET
import sys

# Access the command-line arguments
inputFile = sys.argv[1]
outputFile = sys.argv[2]

tree = ET.parse(inputFile)
root = tree.getroot()

# Define namespace
ns = {"tpcdi": "http://www.tpc.org/tpc-di"}

data_to_csv = []
for action in root.findall("tpcdi:Action", ns):
    row = {}
    customer = action.find("Customer")
    account = customer.find("Account") if customer is not None else None

    row["customerid"] = customer.get("C_ID") if customer is not None else None
    row["accountid"] = account.get("CA_ID") if account is not None else None
    row["brokerid"] = (
        account.find("CA_B_ID").text
        if account is not None and account.find("CA_B_ID") is not None
        else None
    )
    row["taxid"] = customer.get("C_TAX_ID") if customer is not None else None
    row["accountdesc"] = (
        account.find("CA_NAME").text
        if account is not None and account.find("CA_NAME") is not None
        else None
    )
    row["taxstatus"] = account.get("CA_TAX_ST") if account is not None else None
    row["status"] = None  # This field is not in the xml
    row["lastname"] = (
        customer.find("Name/C_L_NAME").text
        if customer is not None and customer.find("Name/C_L_NAME") is not None
        else None
    )
    row["firstname"] = (
        customer.find("Name/C_F_NAME").text
        if customer is not None and customer.find("Name/C_F_NAME") is not None
        else None
    )
    row["middleinitial"] = (
        customer.find("Name/C_M_NAME").text
        if customer is not None and customer.find("Name/C_M_NAME") is not None
        else None
    )
    row["gender"] = customer.get("C_GNDR") if customer is not None else None
    row["tier"] = customer.get("C_TIER") if customer is not None else None
    row["dob"] = customer.get("C_DOB") if customer is not None else None
    row["addressline1"] = (
        customer.find("Address/C_ADLINE1").text
        if customer is not None and customer.find("Address/C_ADLINE1") is not None
        else None
    )
    row["addressline2"] = (
        customer.find("Address/C_ADLINE2").text
        if customer is not None and customer.find("Address/C_ADLINE2") is not None
        else None
    )
    row["postalcode"] = (
        customer.find("Address/C_ZIPCODE").text
        if customer is not None and customer.find("Address/C_ZIPCODE") is not None
        else None
    )
    row["city"] = (
        customer.find("Address/C_CITY").text
        if customer is not None and customer.find("Address/C_CITY") is not None
        else None
    )
    row["stateprov"] = (
        customer.find("Address/C_STATE_PROV").text
        if customer is not None and customer.find("Address/C_STATE_PROV") is not None
        else None
    )
    row["country"] = (
        customer.find("Address/C_CTRY").text
        if customer is not None and customer.find("Address/C_CTRY") is not None
        else None
    )
    row["phone1"] = (
        customer.find("ContactInfo/C_PHONE_1/C_LOCAL").text
        if customer is not None
        and customer.find("ContactInfo/C_PHONE_1/C_LOCAL") is not None
        else None
    )
    row["phone2"] = (
        customer.find("ContactInfo/C_PHONE_2/C_LOCAL").text
        if customer is not None
        and customer.find("ContactInfo/C_PHONE_2/C_LOCAL") is not None
        else None
    )
    row["phone3"] = (
        customer.find("ContactInfo/C_PHONE_3/C_LOCAL").text
        if customer is not None
        and customer.find("ContactInfo/C_PHONE_3/C_LOCAL") is not None
        else None
    )
    row["email1"] = (
        customer.find("ContactInfo/C_PRIM_EMAIL").text
        if customer is not None
        and customer.find("ContactInfo/C_PRIM_EMAIL") is not None
        else None
    )
    row["email2"] = (
        customer.find("ContactInfo/C_ALT_EMAIL").text
        if customer is not None and customer.find("ContactInfo/C_ALT_EMAIL") is not None
        else None
    )
    row["lcl_tx_id"] = (
        customer.find("TaxInfo/C_LCL_TX_ID").text
        if customer is not None and customer.find("TaxInfo/C_LCL_TX_ID") is not None
        else None
    )
    row["nat_tx_id"] = (
        customer.find("TaxInfo/C_NAT_TX_ID").text
        if customer is not None and customer.find("TaxInfo/C_NAT_TX_ID") is not None
        else None
    )
    row["update_ts"] = action.get("ActionTS") if action is not None else None
    row["ActionType"] = action.get("ActionType") if action is not None else None

    data_to_csv.append(row)

# Write data to csv
keys = data_to_csv[0].keys()
with open(outputFile, "w", newline="") as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(data_to_csv)
