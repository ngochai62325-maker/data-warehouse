import pandas as pd
import glob
import os

# 1. Define your Windows input and output folders
input_folder = "/datasets/parquet-format" 
output_folder = "/datasets/csv-format"

# Ensure the output folder exists
os.makedirs(output_folder, exist_ok=True)

# 2. Find all parquet files in the input folder
for file_path in glob.glob(os.path.join(input_folder, "*.parquet")):
    print(f"Processing: {os.path.basename(file_path)}...")
    
    # Read the parquet file
    df = pd.read_parquet(file_path)
    
    # 3. Create the new CSV file name and place it in the 'csv-format' folder
    base_name = os.path.basename(file_path) 
    csv_name = base_name.replace('.parquet', '.csv')
    output_file_path = os.path.join(output_folder, csv_name)
    
    # Save it as a CSV
    df.to_csv(output_file_path, index=False)
    print(f"Successfully saved to: {output_file_path}\n")

print("All files converted successfully!")