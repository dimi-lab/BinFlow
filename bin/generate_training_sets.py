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
        filtered = [label for label in label_list if label.lower().startswith(label_prefix_lower)]
        return "|".join(filtered)

    # Apply the filter to the specified column
    df['key_label'] = df[label_column].apply(filter_labels)
    return df

def process_files(input_files, label_column, label_delimiter):
    """
    Process multiple files to generate child tables based on unique labels
    from the `label_column` column and output them as separate TSV files.
    """
    label_summary = {}
    label_tables = {}

    for file in input_files:
        print(f"Processing file: {file}")
        try:
            # Read the input file into a DataFrame
            df = pd.read_csv(file, sep="\t", low_memory=False)
        except Exception as e:
            print(f"Error reading {file}: {e}")
            continue

        # Skip rows where the label column is blank
        df = df[df[label_column].notna()]

        # Extract unique labels ending in "+" or "-" and count occurrences
        df["labels"] = df[label_column].fillna("").astype(str).str.split(label_delimiter)
        all_labels = [label for labels in df["labels"] for label in labels if label.endswith(("+", "-"))]
        unique_labels = set(all_labels)

        # Generate a summary table for label counts
        for label in unique_labels:
            count = sum(label in labels for labels in df["labels"])
            pos_count = sum(label in labels and label.endswith("+") for labels in df["labels"])
            neg_count = sum(label in labels and label.endswith("-") for labels in df["labels"])
            label_summary[label] = {
                "total": label_summary.get(label, {}).get("total", 0) + count,
                "+": label_summary.get(label, {}).get("+", 0) + pos_count,
                "-": label_summary.get(label, {}).get("-", 0) + neg_count,
            }

        # Split the data into child tables based on unique labels
        for label in unique_labels:
            matching_rows = df[df["labels"].apply(lambda labels: label in labels)]
            label_prefix = label.rstrip('+-')
            # Make column for binary key labelling
            matching_rows = filter_and_reduce_labels(matching_rows, label_column="labels", label_prefix=label_prefix)
            # Apply the filter to the specified column
            pattern = re.compile(rf"^{re.escape(label_prefix)}\s*:", re.IGNORECASE)
            matching_columns = [col for col in df.columns if pattern.match(col)]
            # Add additional columns that contain 'Centroid', 'Image', or 'ROI'
            additional_columns = [col for col in matching_rows.columns if any(key in col for key in ['key_', 'Centroid', 'Image', 'ROI', label_column])]
            matching_columns = list(set(matching_columns + additional_columns))  # Ensure no duplicates
            print(", ".join(matching_columns))
            
            ## Append a few static columns (centroid, image, labels)
            child_table = matching_rows[matching_columns]
            print(child_table)

            # If the label is already in the dictionary, merge the tables
            if child_table.shape[0] > 0:
                print(f"{label} in {file} contains {child_table.shape[0]} rows")
                if label_prefix in label_tables:
                    label_tables[label_prefix] = pd.concat([label_tables[label_prefix], child_table], ignore_index=True)
                else:
                    # Otherwise, add it to the dictionary
                    label_tables[label_prefix] = child_table

            

    # Create a summary table for label counts and save as TSV
    summary_df = pd.DataFrame.from_dict(label_summary, orient="index")
    summary_df.index.name = "Label"
    summary_df.reset_index(inplace=True)
    summary_df.to_csv("label_summary.tsv", sep="\t", index=False)
    print("Label summary saved to label_summary.tsv")
    
    # Generate a random 8-character string for this run
    rand_suffix = ''.join(random.choices(string.ascii_letters + string.digits, k=8))

    # Write the child table to a TSV file
    # Save each merged table to a file
    for label, table in label_tables.items():
        filename = f"training_{label}_{rand_suffix}.tsv"
        table.to_csv(filename, sep='\t', index=False)
        print(f"Saved {filename}")

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: generate_training_sets.py <label_column> <label_delimiter> <file1> [<file2> ...]")
        sys.exit(1)
    label_column = sys.argv[1]
    label_delimiter = sys.argv[2]
    input_files = sys.argv[3:]

    process_files(input_files, label_column, label_delimiter)

