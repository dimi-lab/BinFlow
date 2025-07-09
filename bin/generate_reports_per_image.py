#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import sys
import os
import re

merged_file = sys.argv[1]
image_id = sys.argv[2]
df = pd.read_csv(merged_file, sep='\t')
img_id = re.sub(r'_boxcox_mod\.tsv$', '', image_id)

plot_files = []

# Find prediction columns
prediction_cols = [col for col in df.columns if col.startswith("Prediction")]
x_col = "Centroid X µm"
y_col = "Centroid Y µm"

for pred_col in prediction_cols:
    plt.figure()
    # Use unique values in the prediction column for coloring
    unique_vals = df[pred_col].unique()
    for val in unique_vals:
        mask = df[pred_col] == val
        plt.scatter(df.loc[mask, x_col], df.loc[mask, y_col], label=str(val), alpha=0.7)
    plt.xlabel(x_col)
    plt.ylabel(y_col)
    plt.title(pred_col)
    plt.legend()
    plot_name = f"{img_id}_{pred_col}_scatter.png"
    plt.savefig(plot_name, bbox_inches='tight')
    plt.close()
    plot_files.append(plot_name)

# Generate a simple HTML report
with open(f"{img_id}_report.html", "w") as f:
    f.write(f"<html><head><title>Report for {img_id}</title></head><body>")
    f.write(f"<h1>Report for {img_id}</h1>")
    for plot in plot_files:
        f.write(f'<img src="{plot}" alt="{plot}"><br>')
    f.write("</body></html>")