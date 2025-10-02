#!/usr/bin/env python3
import os
import sys
import pandas as pd
from collections import defaultdict

# --- Configuration ---
if len(sys.argv) < 4:
    print("Usage: binary_table.py <output_table> <label_column> <file1> [<file2> ...]")
    sys.exit(1)

output_table = sys.argv[1]
label_column = sys.argv[2]
file_list = sys.argv[3:]

# Use a set to collect all unique labels across all files
all_labels = set()
# Store per-file label counts in a dict of dicts
file_label_counts = {}

for file_path in file_list:
    try:
        # Read only the label column
        df = pd.read_csv(file_path, sep="\t", usecols=[label_column], low_memory=True)
    except ValueError:
        print(f"Skipping {file_path}: '{label_column}' column not found.")
        file_label_counts[os.path.basename(file_path)] = {}
        continue
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        file_label_counts[os.path.basename(file_path)] = {}
        continue

    # Split and explode efficiently
    all_values = df[label_column].dropna().astype(str).str.split('|')
    flat_labels = (val.strip() for sublist in all_values for val in sublist)
    counts = defaultdict(int)
    for label in flat_labels:
        counts[label] += 1
        all_labels.add(label)
    file_label_counts[os.path.basename(file_path)] = counts

# Prepare sorted list of all labels for columns
all_labels = sorted(l for l in all_labels if l)  # Remove empty labels

# Write output incrementally
with open(output_table, 'w') as out_f:
    header = ['file'] + all_labels
    out_f.write('\t'.join(header) + '\n')
    for file_name, counts in file_label_counts.items():
        row = [file_name] + [str(counts.get(label, 0)) for label in all_labels]
        out_f.write('\t'.join(row) + '\n')

print(f"Per-label counts saved to '{output_table}'")


