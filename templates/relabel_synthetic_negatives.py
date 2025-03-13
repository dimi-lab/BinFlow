#!/usr/bin/env python3

import pandas as pd
import os, sys
import numpy as np
import random

def find_unpaired_columns(df):
    headers = df.columns.tolist()
    # Separate column names based on "+" or "-" suffix
    plus_columns = {col[:-1] for col in headers if col.endswith("+")}
    minus_columns = {col[:-1] for col in headers if col.endswith("-")}
    # Find common pairs and unpaired columns
    paired = plus_columns & minus_columns
    unpaired = (plus_columns ^ minus_columns)  # Symmetric difference finds non-paired ones
    
    return {"paired": sorted(paired), "unpaired": sorted(unpaired)}


def process_counts_and_modify_df(counts_df, df, output_filename, add_only_missing=True, min_selection=5, prec_threshold=5):
    """
    Processes the counts_df to determine which "-" suffix columns to modify in df.
    If add_only_missing is True, only modifies columns where the first-row count is <= 1.
    If False, processes all "-" suffix columns.
    Args:
    - counts_df (pd.DataFrame): The DataFrame containing counts.
    - df (pd.DataFrame): The quantification DataFrame to modify.
    - add_only_missing (bool): Whether to only modify missing values.
    - output_filename (str): Output filename where the modified df is saved.
    
    Returns:
    - pd.DataFrame: The modified DataFrame.
    """
    
    # Identify columns with "-" suffix
    negative_cols = [col for col in counts_df.columns if col.endswith("-")]
    
    for col in negative_cols:
        # Skip if add_only_missing is True and first-row count is greater than 1
        if add_only_missing and counts_df.at[0, col] > 1:
            continue

        # Find the corresponding median column by searching for a match
        matching_cols = [c for c in df.columns if col[:-1] in c and "Median" in c]
        if not matching_cols:
            print(f"Warning: No matching 'Median' column found for {col}, skipping.")
            continue
        median_col = matching_cols[0]  # Select the first match

        # Compute the 0.2 percentile threshold
        percentile_threshold = np.percentile(df[median_col].dropna(), prec_threshold)

        # Filter rows where the median column value is within the 0.2 percentile
        filtered_rows = df[df[median_col] <= percentile_threshold]

        # Select random indices from the filtered rows
        num_samples = min(min_selection, len(filtered_rows))  # Choose up to 5 samples
        random_indices = random.sample(list(filtered_rows.index), num_samples) if num_samples > 0 else []

        # Modify "Classification" column for selected rows
        for idx in random_indices:
            current_class = df.at[idx, "Classification"]
            new_value = col if pd.isna(current_class) or current_class == "" else f"{current_class}|{col}"
            df.at[idx, "Classification"] = new_value

    # Save the modified df with "_mod.tsv" suffix
    output_file = output_filename.replace(".tsv", "_mod.tsv")
    df.to_csv(output_file, sep="\t", index=False)
    print(f"Modified DataFrame saved to {output_file}")


      

if __name__ == "__main__":
    fhName = "${quant_table}"
    label_delimiter = "|"  # Replace with your label delimiter
    add_only_missing = False # Control to only add negatives to missing labels as a whole. If False will add some annotations to every single negative lable.
    n_cells_to_label = int("${params.huerustic_negative_n_cells}")
    below_percentile = float("${params.huerustic_negative_percentile}")
    countsTable = pd.read_csv("${counts_tsv}", sep="\t")
    df = pd.read_csv(fhName, sep="\t", low_memory=False)
    thisFocus = countsTable[countsTable['file']==fhName]
    
    colGroups = find_unpaired_columns(thisFocus)
    print("Paired:", colGroups["paired"])
    print("Unpaired:", colGroups["unpaired"])
    
    for col in colGroups["unpaired"]:
        thisFocus[f"{col}-"] = 0

    #print(thisFocus)
    process_counts_and_modify_df(thisFocus, df, fhName, add_only_missing, n_cells_to_label, below_percentile)
    
    
  
