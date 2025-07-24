#!/usr/bin/env python3

import pandas as pd
import os, sys
import numpy as np
import random
import json

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
        if add_only_missing and counts_df.iloc[0][col] > 1:
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
    write_split_files(df, output_filename)


def write_split_files(df, base_output_file, chunk_size=100_000):
    n_rows = len(df)
    if n_rows <= 200_000:
        output_file = base_output_file.replace(".tsv", "_mod.tsv")
        df.to_csv(output_file, sep="\t", index=False)
        print(f"Modified DataFrame saved to {output_file}")
    else:
        n_splits = int(np.ceil(n_rows / chunk_size))
        print(f"Splitting output into {n_splits} files of ~{chunk_size} rows each (round-robin).")
        split_indices = np.arange(n_rows) % n_splits
        for split in range(n_splits):
            split_df = df.iloc[split_indices == split]
            split_file = base_output_file.replace(".tsv", f"_subset{split+1}_mod.tsv")
            split_df.to_csv(split_file, sep="\t", index=False)
            print(f"Saved {len(split_df)} rows to {split_file}")

if __name__ == "__main__":
    if len(sys.argv) < 8:
        print("Usage: relabel_synthetic_negatives.py <quant_table> <counts_tsv> <n_cells_to_label> <below_percentile> <add_only_missing> <singleLabelColumn> <keptContextColumns>")
        sys.exit(1)
    fhName = sys.argv[1]
    counts_tsv = sys.argv[2]
    n_cells_to_label = int(sys.argv[3])
    below_percentile = float(sys.argv[4])
    add_only_missing = sys.argv[5].lower() == "true"
    singleLabelColumn = sys.argv[6]
    keptContextColumns = [col.strip() for col in sys.argv[7].split(",")]

    label_delimiter = "|"  # Still hardcoded, change if needed

    countsTable = pd.read_csv(counts_tsv, sep="\t")
    # Only read singleLabelColumn, keptContextColumns, and relevant 'Median' columns
    with open(fhName) as f:
        header = f.readline().strip().split('\t')
        median_cols = [col for col in header if 'Median' in col]
        context_cols = [col for col in header if col in keptContextColumns]
        cols_to_read = [singleLabelColumn] + context_cols + median_cols

    df = pd.read_csv(fhName, sep="\t", usecols=cols_to_read, low_memory=True)
    thisFocus = countsTable[countsTable['file'] == fhName]
    if thisFocus.empty:
        print(f"Warning: No counts found for file {fhName}. Skipping negative label boosting.")
        # Still save the cleaned df and log
        write_split_files(df, fhName)
        sys.exit(0)
    else:
        process_counts_and_modify_df(thisFocus, df, fhName, add_only_missing, n_cells_to_label, below_percentile)

    colGroups = find_unpaired_columns(thisFocus)
    print("Paired:", colGroups["paired"])
    print("Unpaired:", colGroups["unpaired"])

    # --- NEW: Remove unmatched labels from Classification and log them ---
    unmatched_labels = []
    unmatched_counts = {}

    for label in colGroups["unpaired"]:
        # Check if this label matches any marker column in df
        marker_match = any(label in col for col in df.columns if 'Median' in col)
        if not marker_match:
            unmatched_labels.append(label)
            # Remove from Classification and count removals
            count = 0
            def remove_label(val):
                if pd.isna(val) or val == "":
                    return val
                parts = [v.strip() for v in str(val).split(label_delimiter)]
                if label in parts:
                    nonlocal_count = parts.count(label)
                    nonlocal_count = int(nonlocal_count)
                    # Use a mutable object to store count
                    nonlocal_count_ref[0] += nonlocal_count
                    parts = [v for v in parts if v != label]
                return label_delimiter.join(parts) if parts else ""
            nonlocal_count_ref = [0]
            df['Classification'] = df['Classification'].apply(remove_label)
            unmatched_counts[label] = nonlocal_count_ref[0]

    # Write JSON log for unmatched labels
    log_name = fhName.replace(".tsv", "_unmatched_labels.json")
    with open(log_name, "w") as logf:
        json.dump(unmatched_counts, logf, indent=2)
    print(f"Unmatched label log written to {log_name}")

    # --- continue with your existing code ---
    for col in colGroups["unpaired"]:
        thisFocus[f"{col}-"] = 0

    process_counts_and_modify_df(thisFocus, df, fhName, add_only_missing, n_cells_to_label, below_percentile)

    write_split_files(df, fhName)



