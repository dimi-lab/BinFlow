#!/usr/bin/env python3
import os
import pandas as pd

# --- Configuration ---
file_list = "${tables_collected}".split(' ')
output_table = "perlabel_table.tsv"  # Output TSV file with per-label counts
label_column = "${params.singleLabelColumn}"  # The column name containing the labels

data = []  # List to store the count dictionaries for each file
summary_data = []  # List to store the summary counts

for file_path in file_list:
    # Read the file into a DataFrame
    try:
        df = pd.read_csv(file_path, sep="\t", low_memory=False)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        continue

    # Check if the label column exists; skip file if not found
    if label_column not in df.columns:
        print(f"Skipping {file_path}: '{label_column}' column not found.")
        summary_data.append({"file": os.path.basename(file_path), "label_count": 0})
        continue

    # Split each cell by "|" and remove any extra whitespace, then explode the list into individual rows
    all_values = df[label_column].dropna().apply(lambda x: [val.strip() for val in str(x).split("|")]).explode()
    # Count occurrences of each unique value in the label column
    counts = all_values.value_counts().to_dict()
    # Create a row dictionary: add the file's base name as 'file', and then update with counts
    row = {"file": os.path.basename(file_path)}
    row.update(counts)
    data.append(row)
    # Add summary count for this file
    summary_data.append({"file": os.path.basename(file_path), "label_count": int(all_values.shape[0])})

# --- Create a combined DataFrame for per-label counts ---
result_df = pd.DataFrame(data)
if not result_df.empty:
    # Ensure the 'file' column is the first column and sort other columns alphabetically
    file_col = 'file'
    other_cols = sorted([col for col in result_df.columns if col != file_col])
    result_df = result_df[[file_col] + other_cols]
    result_df = result_df.fillna(0)
    result_df.to_csv(output_table, sep="\t", index=False)
    print(f"Per-label counts saved to '{output_table}'")
else:
    print("No label data found for any file.")
    # Still write an empty table with just the file column if possible
    pd.DataFrame(columns=["file"]).to_csv(output_table, sep="\t", index=False)


