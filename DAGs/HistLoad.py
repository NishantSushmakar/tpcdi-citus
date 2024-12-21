from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
import re

with open('/Users/marwasulaiman/Documents/BDMA/DW/Project/tpcdi-citus/LoadBroker.sql', 'r') as file:
    load_dimBroker_sql = file.read()


with open('/Users/marwasulaiman/Documents/BDMA/DW/Project/tpcdi-citus/LoadCompany.sql', 'r') as file:
    load_dimCompany_sql = file.read()

with open('/Users/marwasulaiman/Documents/BDMA/DW/Project/tpcdi-citus/LoadFinancial.sql', 'r') as file:
    load_Financial_sql = file.read()


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
    input_dir = "/Users/marwasulaiman/Documents/BDMA/DW/Project/tpcdi-citus/data/Batch1"  # Replace with your directory path


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
    df_cmp.to_csv("/Users/marwasulaiman/Documents/BDMA/DW/Project/tpcdi-citus/finwire_cmp.csv", index=False, header=False)
    df_sec.to_csv("/Users/marwasulaiman/Documents/BDMA/DW/Project/tpcdi-citus/finwire_sec.csv", index=False, header=False)
    df_fin.to_csv("/Users/marwasulaiman/Documents/BDMA/DW/Project/tpcdi-citus/finwire_fin.csv", index=False, header=False)

    print("All files processed and CSV outputs created!")


with DAG(
    "TPCDI_Hist_Load",
    start_date=datetime(2024, 12, 20),
    schedule_interval="@once",
    catchup=False,
) as dag:
    
    load_BatchDate = PostgresOperator(
        task_id="load_BatchDate",
        postgres_conn_id="citus_master_conn",
        sql="truncate table batchdate; COPY batchdate FROM '/Users/marwasulaiman/Documents/BDMA/DW/Project/tpcdi-citus/data/Batch1/BatchDate.txt';"
    )

    load_dimDate = PostgresOperator(
        task_id="load_dimDate",
        postgres_conn_id="citus_master_conn",
        sql="truncate table dimdate; COPY dimdate FROM '/Users/marwasulaiman/Documents/BDMA/DW/Project/tpcdi-citus/data/Batch1/Date.txt' delimiter '|';"
    )
    
    load_taxRate = PostgresOperator(
        task_id="load_taxRate",
        postgres_conn_id="citus_master_conn",
        sql="truncate table taxrate; COPY taxrate FROM '/Users/marwasulaiman/Documents/BDMA/DW/Project/tpcdi-citus/data/Batch1/TaxRate.txt' delimiter '|';"
    )

    load_statusType = PostgresOperator(
        task_id="load_statusType",
        postgres_conn_id="citus_master_conn",
        sql="truncate table statustype; COPY statustype FROM '/Users/marwasulaiman/Documents/BDMA/DW/Project/tpcdi-citus/data/Batch1/StatusType.txt' delimiter '|';"
    )

    load_Industry = PostgresOperator(
        task_id="load_Industry",
        postgres_conn_id="citus_master_conn",
        sql="truncate table industry; COPY industry FROM '/Users/marwasulaiman/Documents/BDMA/DW/Project/tpcdi-citus/data/Batch1/Industry.txt' delimiter '|';"
    )

    load_tradetype = PostgresOperator(
        task_id="load_tradetype",
        postgres_conn_id="citus_master_conn",
        sql="truncate table tradetype; COPY tradetype FROM '/Users/marwasulaiman/Documents/BDMA/DW/Project/tpcdi-citus/data/Batch1/TradeType.txt' delimiter '|';"
    )

    load_dimTime = PostgresOperator(
        task_id="load_dimTime",
        postgres_conn_id="citus_master_conn",
        sql="truncate table dimtime; COPY dimtime FROM '/Users/marwasulaiman/Documents/BDMA/DW/Project/tpcdi-citus/data/Batch1/Time.txt' delimiter '|';"
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
    
    load_BatchDate >> load_dimDate >> load_taxRate >> load_statusType >> load_Industry >> load_tradetype >> load_dimTime >> load_dimBroker >> Parse_Finwire >> load_dimCompany >> load_Financial


