#!/usr/bin/env python3

import pandas as pd
import os, sys
import numpy as np
import random
import json

def find_unpaired_columns(df):
    headers = df.columns.tolist()
    plus_columns = {col[:-1] for col in headers if col.endswith("+")}
    minus_columns = {col[:-1] for col in headers if col.endswith("-")}
    paired = plus_columns & minus_columns
    unpaired = (plus_columns ^ minus_columns)
    return {"paired": sorted(paired), "unpaired": sorted(unpaired)}

def compute_percentiles(quant_file, median_cols, prec_threshold, chunksize=500_000):
    """
    Compute the percentile threshold for each median column using chunked reading.
    Returns a dict: {col: threshold}
    """
    values = {col: [] for col in median_cols}
    for chunk in pd.read_csv(quant_file, sep="\t", usecols=median_cols, chunksize=chunksize, low_memory=True):
        for col in median_cols:
            values[col].extend(chunk[col].dropna().tolist())
    thresholds = {col: np.percentile(values[col], prec_threshold) if values[col] else None for col in median_cols}
    return thresholds

def process_and_write_chunks(
    quant_file, counts_df, output_filename, thresholds, negative_cols, add_only_missing,
    min_selection, delimiter, singleLabelColumn, context_cols, median_cols, label_delimiter, chunksize=500_000
):
    """
    Process the quant_table in chunks, modify as needed, and write to output in append mode.
    """
    header_written = False
    # For each chunk, process and write
    for chunk in pd.read_csv(quant_file, sep="\t", usecols=[singleLabelColumn]+context_cols+median_cols, chunksize=chunksize, low_memory=True):
        # For each negative col, process chunk
        for col in negative_cols:
            # Skip if add_only_missing is True and first-row count is greater than 1
            if add_only_missing and counts_df.iloc[0][col] > 1:
                continue
            # Find matching median col
            matching_cols = [c for c in median_cols if col[:-1] in c]
            median_col = next((c for c in matching_cols if "Cell:" in c), None)
            if not median_col and matching_cols:
                median_col = matching_cols[0]
            if not median_col or thresholds[median_col] is None:
                continue
            percentile_threshold = thresholds[median_col]
            filtered_rows = chunk[chunk[median_col] <= percentile_threshold]
            # Select random indices from filtered rows
            num_samples = min(min_selection, len(filtered_rows))
            if num_samples > 0:
                valid_indices = filtered_rows.index.tolist()
                random_indices = random.sample(valid_indices, num_samples)
            else:
                random_indices = []
            # Modify Classification for selected rows
            for idx in random_indices:
                current_class = chunk.at[idx, singleLabelColumn]
                new_value = col if pd.isna(current_class) or current_class == "" else f"{current_class}{delimiter}{col}"
                # Remove duplicate labels
                parts = new_value.split(delimiter)
                seen = set()
                unique = [p for p in parts if p and not (p in seen or seen.add(p))]
                final_value = delimiter.join(unique) + (delimiter if new_value.endswith(delimiter) else '')
                chunk.at[idx, singleLabelColumn] = final_value
        # Write chunk to output
        output_file = output_filename.replace(".tsv", "_mod.tsv")
        chunk.to_csv(output_file, sep="\t", index=False, mode='a', header=not header_written)
        header_written = True
    print(f"Modified DataFrame saved to {output_file}")

def remove_unmatched_labels_chunked(
    quant_file, output_file, colGroups, median_cols, singleLabelColumn, label_delimiter, context_cols, chunksize=500_000
):
    """
    Remove unmatched labels from Classification column in chunks.
    """
    unmatched_counts = {label: 0 for label in colGroups["unpaired"]}
    header_written = False
    for chunk in pd.read_csv(quant_file, sep="\t", usecols=[singleLabelColumn]+context_cols+median_cols, chunksize=chunksize, low_memory=True):
        for label in colGroups["unpaired"]:
            marker_match = any(label in col for col in median_cols)
            if not marker_match:
                def remove_label(val):
                    if pd.isna(val) or val == "":
                        return val
                    parts = [v.strip() for v in str(val).split(label_delimiter)]
                    count = parts.count(label)
                    unmatched_counts[label] += count
                    parts = [v for v in parts if v != label]
                    return label_delimiter.join(parts) if parts else ""
                chunk[singleLabelColumn] = chunk[singleLabelColumn].apply(remove_label)
        chunk.to_csv(output_file, sep="\t", index=False, mode='a', header=not header_written)
        header_written = True
    return unmatched_counts

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

    label_delimiter = "|"  # Still hardcoded

    countsTable = pd.read_csv(counts_tsv, sep="\t")
    # Only read singleLabelColumn, keptContextColumns, and relevant 'Median' columns
    with open(fhName) as f:
        header = f.readline().strip().split('\t')
        median_cols = [col for col in header if 'Median' in col]
        context_cols = [col for col in header if col in keptContextColumns]
        cols_to_read = [singleLabelColumn] + context_cols + median_cols

    # Compute percentiles for each median column (first pass)
    thresholds = compute_percentiles(fhName, median_cols, below_percentile)

    thisFocus = countsTable[countsTable['file'] == fhName]
    if thisFocus.empty:
        print(f"Warning: No counts found for file {fhName}. Skipping negative label boosting.")
        # Still save the cleaned df and log (just copy input to output in chunks)
        output_file = fhName.replace(".tsv", "_mod.tsv")
        header_written = False
        for chunk in pd.read_csv(fhName, sep="\t", usecols=cols_to_read, chunksize=500_000, low_memory=True):
            chunk.to_csv(output_file, sep="\t", index=False, mode='a', header=not header_written)
            header_written = True
        sys.exit(0)
    else:
        negative_cols = [col for col in thisFocus.columns if col.endswith("-")]
        process_and_write_chunks(
            fhName, thisFocus, fhName, thresholds, negative_cols, add_only_missing,
            n_cells_to_label, label_delimiter, singleLabelColumn, context_cols, median_cols, label_delimiter, chunksize=500_000
        )

    colGroups = find_unpaired_columns(thisFocus)
    print("Paired:", colGroups["paired"])
    print("Unpaired:", colGroups["unpaired"])

    # Remove unmatched labels in chunks and log
    output_file = fhName.replace(".tsv", "_mod.tsv")
    unmatched_counts = remove_unmatched_labels_chunked(
        output_file, output_file, colGroups, median_cols, singleLabelColumn, label_delimiter, context_cols, chunksize=500_000
    )
    log_name = fhName.replace(".tsv", "_unmatched_labels.json")
    with open(log_name, "w") as logf:
        json.dump(unmatched_counts, logf, indent=2)
    print(f"Unmatched label log written to {log_name}")





