import pandas as pd
import os
import re

# Directory containing the Finwire files
input_dir = "/Users/marwasulaiman/Documents/BDMA/DW/Project/tpcdi-citus/data/Batch1"  # Replace with your directory path

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
df_cmp.to_csv("finwire_cmp.csv", index=False, header=False)
df_sec.to_csv("finwire_sec.csv", index=False, header=False)
df_fin.to_csv("finwire_fin.csv", index=False, header=False)

print("All files processed and CSV outputs created!")
