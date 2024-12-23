import json
import csv

# List of input files
input_files = ["Results/scale3Tasks.json", "Results/scale5Tasks.json", "Results/scale10Tasks.json", "Results/scale15Tasks.json"]

# Process each file
for input_file in input_files:
    try:
        # Read the JSON data from the input file
        with open(input_file, 'r') as file:
            data = json.load(file)
        
        # Extract task_id and duration
        tasks = data.get("task_instances", [])
        output_data = [(task["task_id"], task["duration"]) for task in tasks]
        
        # Define the output file name based on the input file name
        output_file = input_file.replace(".json", ".csv")
        
        # Write the results to the CSV file
        with open(output_file, 'w', newline='') as file:
            writer = csv.writer(file)
            # Write header
            writer.writerow(["task_id", "duration"])
            # Write data rows
            writer.writerows(output_data)
        
        print(f"Processed {input_file} -> {output_file}")
    
    except FileNotFoundError:
        print(f"Error: File {input_file} not found.")
    except json.JSONDecodeError:
        print(f"Error: Failed to decode JSON from {input_file}.")
    except KeyError:
        print(f"Error: Missing expected data in {input_file}.")
