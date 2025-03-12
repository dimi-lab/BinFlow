#!/usr/bin/env python3
import os
import glob
import pandas as pd

# --- Configuration ---
file_list = "${tables_collected}".split(' ')
output_file = "label_counts.tsv"  # Output TSV file
label_column = "${params.singleLabelColumn}"  # Replace with your column name containing the labels   

data = []  # List to store the count dictionaries for each file

for file_path in file_list:
    # Read the file into a DataFrame
    try:
        df = pd.read_csv(file_path, sep="\t", low_memory=False)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        continue

    # Check if the 'name' column exists; skip file if not found
    if label_column not in df.columns:
        print(f"Skipping {file_path}: '{label_column}' column not found.")
        continue

    # Split each cell by "|" and remove any extra whitespace, then explode the list into individual rows
    all_values = df[label_column].dropna().apply(lambda x: [val.strip() for val in x.split("|")]).explode()
    # Count occurrences of each unique value in the 'name' column
    counts = all_values.value_counts().to_dict()
    # Create a row dictionary: add the file's base name as 'file', and then update with counts
    row = {"file": os.path.basename(file_path)}
    row.update(counts)
    data.append(row)

# --- Create a combined DataFrame ---
result_df = pd.DataFrame(data)

# Ensure the 'file' column is the first column
cols = ['file'] + [col for col in result_df.columns if col != 'file']
result_df = result_df[cols]

# Replace missing values with 0
result_df = result_df.fillna(0)

# Ensure the 'file' column is the first column and sort other columns alphabetically
file_col = 'file'
other_cols = sorted([col for col in result_df.columns if col != file_col])
result_df = result_df[[file_col] + other_cols]

# Save the resulting DataFrame to a TSV file
result_df.to_csv(output_file, sep="\t", index=False)
print(f"Combined counts saved to '{output_file}'")

