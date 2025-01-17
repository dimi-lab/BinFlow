#!/usr/bin/env python3

import os
import pandas as pd

def process_files(input_files, label_column, label_delimiter):
    """
    Process multiple files to generate child tables based on unique labels
    from the `label_column` column and output them as separate TSV files.
    """
    label_summary = {}

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
        df["labels"] = df[label_column].str.split(label_delimiter)
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
            matching_columns = [col for col in df.columns if label.rstrip("+-") in col]
            ## Append a few static columns (centroid, image, labels)
            child_table = matching_rows[matching_columns]

            # Write the child table to a TSV file
            output_filename = f"training_{label.replace('+', 'pos').replace('-', 'neg')}.tsv"
            if not child_table.empty:
                child_table.to_csv(output_filename, sep="\t", index=False)
                print(f"Written child table for label '{label}' to {output_filename}")

    # Create a summary table for label counts and save as TSV
    summary_df = pd.DataFrame.from_dict(label_summary, orient="index")
    summary_df.index.name = "Label"
    summary_df.reset_index(inplace=True)
    summary_df.to_csv("label_summary.tsv", sep="\t", index=False)
    print("Label summary saved to label_summary.tsv")


if __name__ == "__main__":
    # Example usage
    input_files = "${tables_collected}".split(' ')
    label_column = "${params.singleLabelColumn}"  # Replace with your column name containing the labels
    label_delimiter = "|"  # Replace with your label delimiter

    process_files(input_files, label_column, label_delimiter)

