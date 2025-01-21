#!/usr/bin/env python3

import os
import pandas as pd

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
            label_prefix = label.rstrip('+-')
            # Make column for binary key labelling
            matching_rows = filter_and_reduce_labels(matching_rows, label_column="labels", label_prefix=label_prefix)
            # Apply the filter to the specified column
            matching_columns = [col for col in df.columns if label_prefix.lower() in col.lower()]
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
    
    # Write the child table to a TSV file
    # Save each merged table to a file
    for label, table in label_tables.items():
        filename = f"training_{label}.tsv"
        table.to_csv(filename, sep='\t', index=False)
        print(f"Saved {filename}")

if __name__ == "__main__":
    #input_files = "${tables_collected}".split(' ')
    #label_column = "${params.singleLabelColumn}"  # Replace with your column name containing the labels
   
    input_files = "SLIDE-1943_FullPanel_R12.ome.tiff_QUANT_Mod.tsv SLIDE-1943_FullPanel_R0.ome.tiff_QUANT_Mod.tsv SLIDE-1943_FullPanel_R5.ome.tiff_QUANT_Mod.tsv SLIDE-1943_FullPanel_R15.ome.tiff_QUANT_Mod.tsv SLIDE-1943_FullPanel_R3.ome.tiff_QUANT_Mod.tsv SLIDE-1943_FullPanel_R9.ome.tiff_QUANT_Mod.tsv SLIDE-1943_FullPanel_R1.ome.tiff_QUANT_Mod.tsv SLIDE-1943_FullPanel_R10.ome.tiff_QUANT_Mod.tsv SLIDE-1943_FullPanel_R11.ome.tiff_QUANT_Mod.tsv SLIDE-1943_FullPanel_R4.ome.tiff_QUANT_Mod.tsv SLIDE-1943_FullPanel_R13.ome.tiff_QUANT_Mod.tsv SLIDE-1943_FullPanel_R6.ome.tiff_QUANT_Mod.tsv SLIDE-1943_FullPanel_R17.ome.tiff_QUANT_Mod.tsv SLIDE-1943_FullPanel_R2.ome.tiff_QUANT_Mod.tsv SLIDE-1943_FullPanel_R16.ome.tiff_QUANT_Mod.tsv SLIDE-1943_FullPanel_R8.ome.tiff_QUANT_Mod.tsv SLIDE-1943_FullPanel_R14.ome.tiff_QUANT_Mod.tsv SLIDE-1943_FullPanel_R7.ome.tiff_QUANT_Mod.tsv".split(' ')
    label_column = "OriginalClasses"  # Replace with your column name containing the labels
   
    label_delimiter = "|"  # Replace with your label delimiter

    process_files(input_files, label_column, label_delimiter)

