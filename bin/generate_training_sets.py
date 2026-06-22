#!/usr/bin/env python3

import os
import random
import string
import pandas as pd
import sys
import re

def filter_and_reduce_labels(df, label_column, label_prefix):
    """
    Filter a column of lists to retain only elements that match a given prefix (case insensitive)
    and reduce the lists to a string of remaining elements.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing the column with lists.
        label_column (str): The name of the column containing lists of strings.
        label_prefix (str): The prefix to filter elements in the lists.
    
    Returns:
        pd.DataFrame: The modified DataFrame with filtered and reduced labels.
    """
    # Define a case-insensitive match function
    label_prefix_lower = label_prefix.lower()

    def filter_labels(label_list):
        # Filter list for matching labels
        if not isinstance(label_list, list):
            return ""
        filtered = [
            label for label in label_list
            if re.match(rf"^{re.escape(label_prefix_lower)}(\+|-)?$", label.strip(), re.IGNORECASE) # updated to avoid catching CD45RA and CD45RO for CD45 or CD31 for CD3
            ] 
        return "|".join(filtered)

    # Apply the filter to the specified column
    df['key_label'] = df[label_column].apply(filter_labels)
    return df


def process_files(input_files, label_column, label_delimiter, chunksize=250_000):
    """
    Process large files in chunks to generate child tables based on unique labels
    from the `label_column` column and output them as separate TSV files.
    """
    import json
    label_summary = {}
    label_files = {}  # label_prefix -> filename
    label_columns = {}  # label_prefix -> set of columns
    rand_suffix = ''.join(random.choices(string.ascii_letters + string.digits, k=8))

    for file in input_files:
        print(f"Processing file: {file}")
        try:
            reader = pd.read_csv(file, sep="\t", low_memory=False, chunksize=chunksize)
        except Exception as e:
            print(f"Error reading {file}: {e}")
            continue

        for chunk in reader:
            if label_column not in chunk.columns:
                print(f"ERROR: Label column '{label_column}' not found in columns: {chunk.columns.tolist()}")
                continue
            chunk = chunk[chunk[label_column].notna()]
            chunk["labels"] = chunk[label_column].fillna("").astype(str).str.split(label_delimiter)
            all_labels = [label for labels in chunk["labels"] for label in labels if label.endswith(("+", "-"))]
            unique_labels = set(all_labels)

            # Update summary counts
            for label in unique_labels:
                count = sum(label in labels for labels in chunk["labels"])
                pos_count = sum(label in labels and label.endswith("+") for labels in chunk["labels"])
                neg_count = sum(label in labels and label.endswith("-") for labels in chunk["labels"])
                label_summary[label] = {
                    "total": label_summary.get(label, {}).get("total", 0) + count,
                    "+": label_summary.get(label, {}).get("+", 0) + pos_count,
                    "-": label_summary.get(label, {}).get("-", 0) + neg_count,
                }

            # Write child tables incrementally
            for label in unique_labels:
                matching_rows = chunk[chunk["labels"].apply(lambda labels: label in labels)]
                label_prefix = label.rstrip('+-')
                matching_rows = filter_and_reduce_labels(matching_rows, label_column="labels", label_prefix=label_prefix)
                pattern = re.compile(rf"^{re.escape(label_prefix)}\s*:", re.IGNORECASE)
                matching_columns = [col for col in chunk.columns if pattern.match(col)]
                additional_columns = [col for col in matching_rows.columns if any(key in col for key in ['key_', 'Centroid', 'Image', 'ROI', label_column])]
                matching_columns = list(set(matching_columns + additional_columns))
                child_table = matching_rows[matching_columns]
                if child_table.shape[0] == 0:
                    continue
                # Track columns for this label
                if label_prefix not in label_columns:
                    label_columns[label_prefix] = set(child_table.columns)
                else:
                    label_columns[label_prefix].update(child_table.columns)
                # Write to file incrementally
                filename = f"training_{label_prefix}_{rand_suffix}.tsv"
                label_files[label_prefix] = filename
                write_header = not os.path.exists(filename)
                child_table.to_csv(filename, sep='\t', index=False, mode='a', header=write_header)
                print(f"Appended {child_table.shape[0]} rows to {filename}")

    # Write summary
    summary_df = pd.DataFrame.from_dict(label_summary, orient="index")
    summary_df.index.name = "Label"
    summary_df.reset_index(inplace=True)
    summary_df.to_csv("label_summary.tsv", sep="\t", index=False)
    print("Label summary saved to label_summary.tsv")

    # Write column info for small tables
    for label_prefix, filename in label_files.items():
        # Check number of columns
        columns = list(label_columns[label_prefix])
        if len(columns) <= 5:
            json_filename = f"training_{label_prefix}_{rand_suffix}_columns.json"
            with open(json_filename, 'w') as jf:
                json.dump({"columns": columns}, jf, indent=2)
            print(f"Table for label {label_prefix} has {len(columns)} columns (<=5). Wrote column names to {json_filename}")
        else:
            print(f"Saved {filename}")

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: generate_training_sets.py <label_column> <label_delimiter> <file1> [<file2> ...]")
        sys.exit(1)
    label_column = sys.argv[1]
    label_delimiter = sys.argv[2]
    input_files = sys.argv[3:]

    process_files(input_files, label_column, label_delimiter)

