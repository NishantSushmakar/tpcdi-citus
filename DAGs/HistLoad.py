from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
import re
import xmltodict
import json
import numpy as np

PATH = "/Users/marwasulaiman/Documents/BDMA/DW/Project/tpcdi-citus"

with open(os.path.join(PATH, 'schema.sql'), 'r') as file:
    createSchema = file.read()

with open(os.path.join(PATH, 'temp_schema.sql'), 'r') as file:
    createTempSchema = file.read()

with open(os.path.join(PATH, 'LoadBroker.sql'), 'r') as file:
    load_dimBroker_sql = file.read()

with open(os.path.join(PATH, 'LoadCompany.sql'), 'r') as file:
    load_dimCompany_sql = file.read()

with open(os.path.join(PATH, 'LoadFinancial.sql'), 'r') as file:
    load_Financial_sql = file.read()

with open(os.path.join(PATH, 'LoadProspect.sql'), 'r') as file:
    load_prospect_sql = file.read()

with open(os.path.join(PATH, 'LoadCustomer.sql'), 'r') as file:
    load_customer_sql = file.read()

with open(os.path.join(PATH, 'Load_dimessages_dimcustomer.sql'), 'r') as file:
    load_dimessages_customer_sql = file.read()

with open(os.path.join(PATH, 'UpdateProspect.sql'), 'r') as file:
    update_prospect_sql = file.read()

with open(os.path.join(PATH, 'LoadAccount.sql'), 'r') as file:
    load_account_sql = file.read()

with open(os.path.join(PATH, 'LoadCashBalances.sql'), 'r') as file:
    LoadCashBalances_sql = file.read()

with open(os.path.join(PATH, 'LoadSecurity.sql'), 'r') as file:
    load_security_sql = file.read()

with open(os.path.join(PATH, 'LoadTrade.sql'), 'r') as file:
    load_trade_sql = file.read()

with open(os.path.join(PATH, 'Load_dimessages_dimtrade.sql'), 'r') as file:
    load_dimessages_dimtrade_sql = file.read()

with open(os.path.join(PATH, 'LoadHoldings.sql'), 'r') as file:
    load_holdings_sql = file.read()

with open(os.path.join(PATH, 'LoadFactMarketHistory.sql'), 'r') as file:
    load_fact_market_history_sql = file.read()

with open(os.path.join(PATH, 'Load_dimessages_factmarkethistory.sql'), 'r') as file:
    load_dimessages_factmarkethistory_sql = file.read()

with open(os.path.join(PATH, 'LoadFactWatches.sql'), 'r') as file:
    load_factwatches_sql = file.read()


# Helper function to safely trim and extract substrings
def extract_field(row, start, length):
    return row[start - 1: start - 1 + length].strip() if row else None

# Extract year and quarter from the filename for sorting
def extract_year_quarter(file_name):
    match = re.search(r'(\d{4})Q(\d)', file_name)
    if match:
        year = int(match.group(1))
        quarter = int(match.group(2))
        return year, quarter
    return float('inf'), float('inf')  # Default value for files that don't match the pattern

def ProcessFinwire():
# Directory containing the Finwire files
    input_dir = f"{PATH}/data/Batch1"  # Replace with your directory path


    # Get the list of files and sort them by year and quarter
    files = [f for f in os.listdir(input_dir) if f.startswith("FINWIRE") and not f.endswith("_audit.csv")]
    sorted_files = sorted(files, key=extract_year_quarter)

    # Initialize empty lists for each table
    finwire_cmp = []
    finwire_sec = []
    finwire_fin = []

    # Process each file in sorted order
    for file_name in sorted_files:
        file_path = os.path.join(input_dir, file_name)
        print(f"Processing file: {file_path}")
        
        # Read the file line by line
        with open(file_path, "r", encoding="utf-8") as file:  # Adjust encoding if needed
            lines = file.readlines()
        
        # Process each line (record)
        for row in lines:
            record_type = extract_field(row, 16, 3)
            
            if record_type == 'CMP':
                finwire_cmp.append({
                    "pts": extract_field(row, 1, 15),
                    "rectype": extract_field(row, 16, 3),
                    "companyname": extract_field(row, 19, 60),
                    "cik": extract_field(row, 79, 10),
                    "status": extract_field(row, 89, 4),
                    "industryid": extract_field(row, 93, 2),
                    "sprating": extract_field(row, 95, 4),
                    "foundingdate": extract_field(row, 99, 8),
                    "addressline1": extract_field(row, 107, 80),
                    "addressline2": extract_field(row, 187, 80),
                    "postalcode": extract_field(row, 267, 12),
                    "city": extract_field(row, 279, 25),
                    "stateprovince": extract_field(row, 304, 20),
                    "country": extract_field(row, 324, 24),
                    "ceoname": extract_field(row, 348, 46),
                    "description": extract_field(row, 394, 150),
                })
            elif record_type == 'SEC':
                finwire_sec.append({
                    "pts": extract_field(row, 1, 15),
                    "rectype": extract_field(row, 16, 3),
                    "symbol": extract_field(row, 19, 15),
                    "issuetype": extract_field(row, 34, 6),
                    "status": extract_field(row, 40, 4),
                    "name": extract_field(row, 44, 70),
                    "exid": extract_field(row, 114, 6),
                    "shout": extract_field(row, 120, 13),
                    "firsttradedate": extract_field(row, 133, 8),
                    "firsttradeexchg": extract_field(row, 141, 8),
                    "dividend": extract_field(row, 149, 12),
                    "conameorcik": extract_field(row, 161, 60),
                })
            elif record_type == 'FIN':
                finwire_fin.append({
                    "pts": extract_field(row, 1, 15),
                    "rectype": extract_field(row, 16, 3),
                    "year": extract_field(row, 19, 4),
                    "quarter": extract_field(row, 23, 1),
                    "qtrstartdate": extract_field(row, 24, 8),
                    "postingdate": extract_field(row, 32, 8),
                    "revenue": extract_field(row, 40, 17),
                    "earnings": extract_field(row, 57, 17),
                    "eps": extract_field(row, 74, 12),
                    "dilutedeps": extract_field(row, 86, 12),
                    "margin": extract_field(row, 98, 12),
                    "inventory": extract_field(row, 110, 17),
                    "assets": extract_field(row, 127, 17),
                    "liability": extract_field(row, 144, 17),
                    "shout": extract_field(row, 161, 13),
                    "dilutedshout": extract_field(row, 174, 13),
                    "conameorcik": extract_field(row, 187, 60),
                })

    # Convert lists to DataFrames
    df_cmp = pd.DataFrame(finwire_cmp)
    df_sec = pd.DataFrame(finwire_sec)
    df_fin = pd.DataFrame(finwire_fin)

    # Save to CSV
    df_cmp.to_csv(f"{PATH}/finwire_cmp.csv", index=False, header=False)
    df_sec.to_csv(f"{PATH}/finwire_sec.csv", index=False, header=False)
    df_fin.to_csv(f"{PATH}]finwire_fin.csv", index=False, header=False)

    print("All files processed and CSV outputs created!")


def customermgmt_convert():

    with open(os.path.join(PATH, 'data/Batch1/CustomerMgmt.xml')) as fd:
        doc = xmltodict.parse(fd.read()) 
        fd.close()

    with open(os.path.join(PATH, 'data/Batch1/CustomerData.json'), "w") as outfile:
        outfile.write(json.dumps(doc))
        outfile.close()

    f = open(os.path.join(PATH, 'data/Batch1/CustomerData.json'),'r')

    cust = json.load(f)
    actions = cust['TPCDI:Actions']
    action = actions['TPCDI:Action']
    cust_df = pd.DataFrame(columns = np.arange(0, 36))


    for a in action:
        
        cust_row = {}
        
        # action element
        cust_row.update({0: [f"{a.get('@ActionType')}"]})
        cust_row.update({1: [f"{a.get('@ActionTS')}"]})
        
        # action.customer element
        cust_row.update({2: [f"{a.get('Customer').get('@C_ID')}"]})
        cust_row.update({3: [f"{a.get('Customer').get('@C_TAX_ID')}"]})
        cust_row.update({4: [f"{a.get('Customer').get('@C_GNDR')}"]})
        cust_row.update({5: [f"{a.get('Customer').get('@C_TIER')}"]})
        cust_row.update({6: [f"{a.get('Customer').get('@C_DOB')}"]})
        
        # action.customer.name element
        if a.get('Customer').get('Name') != None:
            cust_row.update({7: [f"{a.get('Customer').get('Name').get('C_L_NAME')}"]})
            cust_row.update({8: [f"{a.get('Customer').get('Name').get('C_F_NAME')}"]})
            cust_row.update({9: [f"{a.get('Customer').get('Name').get('C_M_NAME')}"]})
        else:
            cust_row.update({7: [None]})
            cust_row.update({8: [None]})
            cust_row.update({9: [None]})
        
        # action.customer.address element
        if a.get('Customer').get('Address') != None:
            cust_row.update({10: [f"{a.get('Customer').get('Address').get('C_ADLINE1')}"]})
            cust_row.update({11: [f"{a.get('Customer').get('Address').get('C_ADLINE2')}"]})
            cust_row.update({12: [f"{a.get('Customer').get('Address').get('C_ZIPCODE')}"]})
            cust_row.update({13: [f"{a.get('Customer').get('Address').get('C_CITY')}"]})
            cust_row.update({14: [f"{a.get('Customer').get('Address').get('C_STATE_PROV')}"]})
            cust_row.update({15: [f"{a.get('Customer').get('Address').get('C_CTRY')}"]})
        else:
            cust_row.update({10: [None]})
            cust_row.update({11: [None]})
            cust_row.update({12: [None]})
            cust_row.update({13: [None]})
            cust_row.update({14: [None]})
            cust_row.update({15: [None]})
            
        # action.customer.contactinfo element
        if a.get('Customer').get('ContactInfo') != None:     
            cust_row.update({16: [f"{a.get('Customer').get('ContactInfo').get('C_PRIM_EMAIL')}"]})
            cust_row.update({17: [f"{a.get('Customer').get('ContactInfo').get('C_ALT_EMAIL')}"]})
            
            # action.customer.contactinfo.phone element
            
            # phone_1
            cust_row.update({18: [f"{a.get('Customer').get('ContactInfo').get('C_PHONE_1').get('C_CTRY_CODE')}"]})
            cust_row.update({19: [f"{a.get('Customer').get('ContactInfo').get('C_PHONE_1').get('C_AREA_CODE')}"]})
            cust_row.update({20: [f"{a.get('Customer').get('ContactInfo').get('C_PHONE_1').get('C_LOCAL')}"]})
            cust_row.update({21: [f"{a.get('Customer').get('ContactInfo').get('C_PHONE_1').get('C_EXT')}"]})

            # phone_2
            cust_row.update({22: [f"{a.get('Customer').get('ContactInfo').get('C_PHONE_2').get('C_CTRY_CODE')}"]})
            cust_row.update({23: [f"{a.get('Customer').get('ContactInfo').get('C_PHONE_2').get('C_AREA_CODE')}"]})
            cust_row.update({24: [f"{a.get('Customer').get('ContactInfo').get('C_PHONE_2').get('C_LOCAL')}"]})
            cust_row.update({25: [f"{a.get('Customer').get('ContactInfo').get('C_PHONE_2').get('C_EXT')}"]})
        
            # phone_3
            cust_row.update({26: [f"{a.get('Customer').get('ContactInfo').get('C_PHONE_3').get('C_CTRY_CODE')}"]})
            cust_row.update({27: [f"{a.get('Customer').get('ContactInfo').get('C_PHONE_3').get('C_AREA_CODE')}"]})
            cust_row.update({28: [f"{a.get('Customer').get('ContactInfo').get('C_PHONE_3').get('C_LOCAL')}"]})
            cust_row.update({29: [f"{a.get('Customer').get('ContactInfo').get('C_PHONE_3').get('C_EXT')}"]})
        else:
            cust_row.update({16: [None]})
            cust_row.update({17: [None]})
            cust_row.update({18: [None]})
            cust_row.update({19: [None]})
            cust_row.update({20: [None]})
            cust_row.update({21: [None]})
            cust_row.update({22: [None]})
            cust_row.update({23: [None]})
            cust_row.update({24: [None]})
            cust_row.update({25: [None]})
            cust_row.update({26: [None]})
            cust_row.update({27: [None]})
            cust_row.update({28: [None]})
            cust_row.update({29: [None]})
        
        # action.customer.taxinfo element
        if a.get('Customer').get('TaxInfo') != None:
            cust_row.update({30: [f"{a.get('Customer').get('TaxInfo').get('C_LCL_TX_ID')}"]})
            cust_row.update({31: [f"{a.get('Customer').get('TaxInfo').get('C_NAT_TX_ID')}"]})
        else:
            cust_row.update({30:  [None]})
            cust_row.update({31:  [None]})
        
        # action.customer.account attribute
        if a.get('Customer').get('Account') != None:
            cust_row.update({32: [f"{a.get('Customer').get('Account').get('@CA_ID')}"]})
            cust_row.update({33: [f"{a.get('Customer').get('Account').get('@CA_TAX_ST')}"]})
            
            # action.customer.account element
            cust_row.update({34: [f"{a.get('Customer').get('Account').get('CA_B_ID')}"]})
            cust_row.update({35: [f"{a.get('Customer').get('Account').get('CA_NAME')}"]})
        else:
            cust_row.update({32: [None]})
            cust_row.update({33: [None]})
            cust_row.update({34: [None]})
            cust_row.update({35: [None]})
        
        # append to dataframe
        cust_df = pd.concat([cust_df, pd.DataFrame.from_dict(cust_row)], axis = 0)

    cust_df.replace(to_replace = np.nan, value = "", inplace = True)
    cust_df.replace(to_replace = "None", value = "", inplace = True)
    cust_df.to_csv(f"{PATH}/CustomerMgmt.csv", index = False)
    print('Customer Management data converted from XML to CSV')
    f.close()


with DAG(
    "TPCDI_Hist_Load",
    start_date=datetime(2024, 12, 20),
    schedule_interval="@once",
    catchup=False,
) as dag:

    set_path = PostgresOperator(
        task_id="set_path",
        postgres_conn_id="citus_master_conn",
        sql=f"truncate table config; INSERT INTO config (key_name, value_text) VALUES ('base_path', '{PATH}');"
    )

    create_schema = PostgresOperator(
        task_id="create_schema",
        postgres_conn_id="citus_master_conn",
        sql=createSchema
    )

    create_temp_schema = PostgresOperator(
        task_id="create_temp_schema",
        postgres_conn_id="citus_master_conn",
        sql=createTempSchema
    )

    load_BatchDate = PostgresOperator(
        task_id="load_BatchDate",
        postgres_conn_id="citus_master_conn",
        sql=f"truncate table batchdate; COPY batchdate FROM '{PATH}/data/Batch1/BatchDate.txt';"
    )

    load_dimDate = PostgresOperator(
        task_id="load_dimDate",
        postgres_conn_id="citus_master_conn",
        sql=f"truncate table dimdate; COPY dimdate FROM '{PATH}/data/Batch1/Date.txt' delimiter '|';"
    )
    
    load_taxRate = PostgresOperator(
        task_id="load_taxRate",
        postgres_conn_id="citus_master_conn",
        sql=f"truncate table taxrate; COPY taxrate FROM '{PATH}/data/Batch1/TaxRate.txt' delimiter '|';"
    )

    load_statusType = PostgresOperator(
        task_id="load_statusType",
        postgres_conn_id="citus_master_conn",
        sql=f"truncate table statustype; COPY statustype FROM '{PATH}/data/Batch1/StatusType.txt' delimiter '|';"
    )

    load_Industry = PostgresOperator(
        task_id="load_Industry",
        postgres_conn_id="citus_master_conn",
        sql=f"truncate table industry; COPY industry FROM '{PATH}/data/Batch1/Industry.txt' delimiter '|';"
    )

    load_tradetype = PostgresOperator(
        task_id="load_tradetype",
        postgres_conn_id="citus_master_conn",
        sql=f"truncate table tradetype; COPY tradetype FROM '{PATH}/data/Batch1/TradeType.txt' delimiter '|';"
    )

    load_dimTime = PostgresOperator(
        task_id="load_dimTime",
        postgres_conn_id="citus_master_conn",
        sql=f"truncate table dimtime; COPY dimtime FROM '{PATH}/data/Batch1/Time.txt' delimiter '|';"
    )

    load_dimBroker = PostgresOperator(
        task_id="load_dimBroker",
        postgres_conn_id="citus_master_conn",
        sql=load_dimBroker_sql
    )

    Parse_Finwire = PythonOperator(
        task_id='Parse_Finwire', 
        python_callable=ProcessFinwire
    )

    load_dimCompany = PostgresOperator(
        task_id="load_dimCompany",
        postgres_conn_id="citus_master_conn",
        sql=load_dimCompany_sql
    )

    load_Financial = PostgresOperator(
        task_id="load_Financial",
        postgres_conn_id="citus_master_conn",
        sql=load_Financial_sql
    )


    load_prospect = PostgresOperator(
         task_id ="load_prospect",
       postgres_conn_id="citus_master_conn",
        sql=load_prospect_sql
    )

    cnvrt_customermgmt = PythonOperator(
        task_id='cnvrt_customermgmt', 
        python_callable=customermgmt_convert
    )

    load_customermgmt = PostgresOperator(
        task_id = 'load_customermgmt',
        postgres_conn_id="citus_master_conn",
        sql =f"COPY customermgmt FROM '{PATH}/CustomerMgmt.csv' DELIMITER ',' CSV HEADER;"
    )

    load_dimcustomer = PostgresOperator(
        task_id ='load_dimcustomer',
        postgres_conn_id='citus_master_conn',
        sql = load_customer_sql
    )

    load_dimessages_dimcustomer = PostgresOperator(
            task_id ='load_dimessages_dimcustomer',
            postgres_conn_id='citus_master_conn',
            sql = load_dimessages_customer_sql

    )

    update_prospect = PostgresOperator(
            task_id ='update_prospect',
            postgres_conn_id='citus_master_conn',
            sql = update_prospect_sql 

    )

    load_dimaccount = PostgresOperator(
            task_id ='load_dimaccount',
            postgres_conn_id='citus_master_conn',
            sql = load_account_sql

    )

    load_CashBalances = PostgresOperator(
            task_id ='load_CashBalances',
            postgres_conn_id='citus_master_conn',
            sql = LoadCashBalances_sql

    )
    
    load_dimSecurity = PostgresOperator(
            task_id = 'load_dimSecurity',
            postgres_conn_id = 'citus_master_conn',
            sql = load_security_sql
    )

    load_trade_history = PostgresOperator(
            task_id = 'load_trade_history',
            postgres_conn_id = 'citus_master_conn',
            sql = f"COPY tradehistory FROM '{PATH}/data/Batch1/TradeHistory.txt' delimiter '|';"
    )
    
    
    load_trade = PostgresOperator(
       task_id = "load_trade",
       postgres_conn_id="citus_master_conn", 
       sql =f"COPY trade FROM '{PATH}/data/Batch1/Trade.txt' delimiter '|' null as '';"  
    )

    load_dimtrade = PostgresOperator(
        task_id ='load_dimtrade',
        postgres_conn_id='citus_master_conn',
        sql = load_trade_sql
    )

    load_dimessages_dimtrade = PostgresOperator(
        task_id ='load_dimessages_dimtrade',
        postgres_conn_id='citus_master_conn',
        sql = load_dimessages_dimtrade_sql
    )

    load_dailymarket = PostgresOperator(
       task_id = "load_dailymarket",
       postgres_conn_id="citus_master_conn", 
       sql =f"COPY dailymarket FROM '{PATH}/data/Batch1/DailyMarket.txt' delimiter '|';"  
    )

    load_fact_market_history = PostgresOperator(
        task_id ='load_fact_market_history',
        postgres_conn_id='citus_master_conn',
        sql = load_fact_market_history_sql
    )

    load_dimessages_factmarkethistory = PostgresOperator(
        task_id ='load_dimessages_factmarkethistory',
        postgres_conn_id='citus_master_conn',
        sql = load_dimessages_factmarkethistory_sql 
    )

    load_watchhistory = PostgresOperator(
       task_id = "load_watchhistory",
       postgres_conn_id="citus_master_conn", 
       sql =f"COPY watchhistory FROM '{PATH}/data/Batch1/WatchHistory.txt' delimiter '|';"  
    )

    load_factwatches = PostgresOperator(
        task_id ='load_factwatches',
        postgres_conn_id='citus_master_conn',
        sql = load_factwatches_sql 
    )

    load_holdings = PostgresOperator(
        task_id ='load_holdings',
        postgres_conn_id='citus_master_conn',
        sql = load_holdings_sql
    )

    create_schema >> create_temp_schema >> set_path

    set_path >> load_BatchDate >> load_dimDate >> load_taxRate >> load_statusType >> load_Industry >> load_tradetype >> load_dimTime >> load_dimBroker >> Parse_Finwire >> load_dimCompany >> load_Financial

    load_Financial >> load_prospect >> cnvrt_customermgmt >> load_customermgmt >> load_dimcustomer >> load_dimessages_dimcustomer >> update_prospect

    update_prospect >> load_dimaccount >> load_CashBalances >> load_dimSecurity >> load_trade_history >> load_trade >> load_dailymarket >> load_fact_market_history >> load_dimessages_factmarkethistory >> load_watchhistory >> load_factwatches
    
    load_factwatches >> load_dimtrade >> load_dimessages_dimtrade >> load_holdings

    