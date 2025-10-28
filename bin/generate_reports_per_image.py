#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import sys
import os
import re
import glob
import seaborn as sns
from sklearn.metrics import roc_auc_score

# Function to modify column names
def clean_pred_columns(col):
    if col.startswith("Prediction"):
        parts = col.split('_')
        if len(parts) >= 3:
            return '_'.join(parts[:-1])  # Remove the last part
    return col  # Return as-is if not 3 parts

# Helper function to extract identifier
def get_identifier(filename, markers):
    for marker in markers:
        # Build regex to match exact marker with clear boundaries
        pattern = rf'(?:^|[_\.-]){marker}(?:[_\.-]|$)'
        if re.search(pattern, filename):
            return marker  # return first exact match
    return None

merged_file = sys.argv[1]
image_id = sys.argv[2]
qFile_path = sys.argv[3]
df = pd.read_csv(merged_file, sep='\t')
df.columns = [clean_pred_columns(col) for col in df.columns] # drop the "_##" at the end of the prediction columns
df['Inverted Centroid Y µm'] = df['Centroid Y µm'].max() - df['Centroid Y µm']
img_id = re.sub(r'_boxcox_mod\.tsv$', '', image_id)
binary_dir = os.path.dirname(os.path.realpath(merged_file))
print(binary_dir)
pFiles = glob.glob(os.path.join(binary_dir,'*_PRED.tsv'))
print('Found {} Prediction files'.format(len(pFiles)))

plot_files = []
curve_files = []
roc_files = []

### Step 1: Plot Scatterplot of Predictions ###
# Find prediction columns
prediction_cols = [col for col in df.columns if col.startswith("Prediction")]
x_col = "Centroid X µm"
y_col = "Inverted Centroid Y µm"

for pred_col in prediction_cols:
    plt.figure()
    label = pred_col.replace('Prediction_','')
    df[pred_col] = df[pred_col].map({0: f"{label}-", 1: f"{label}+"})
    # Use unique values in the prediction column for coloring
    unique_vals = sorted(
        df[pred_col].unique(),
        key=lambda x: (str(x).rstrip("+-"), str(x).endswith("+")) # ensure that "-" comes before "+"
        )
    for val in unique_vals:
        mask = df[pred_col] == val
        plt.scatter(df.loc[mask, x_col], df.loc[mask, y_col], label=str(val), alpha=0.7)
    plt.xlabel(x_col)
    plt.ylabel(y_col)
    plt.title('Spatial Plot of Binary Labels for ' + label)
    plt.legend()
    plot_name = f"{img_id}_{pred_col}_scatter.png"
    plt.savefig(plot_name, bbox_inches='tight')
    plt.close()
    plot_files.append(plot_name)

### Step 2: Plot Prediction Probabilities Curves ###
for pFile in pFiles:
    # Extract label from qFile
    match = re.search(r'predictions_(.*)\.pkl', pFile)
    if match:
        label = match.group(1)
    else:
        raise ValueError(f"Unexpected filename format: {pFile}")
    # Load the data
    prob_df = pd.read_csv(pFile, sep='\t', low_memory=False)
    # Replace Predictions column values: 0 → 'label-', 1 → 'label+'
    prob_df['Predictions'] = prob_df['Predictions'].map({0: f"{label}-", 1: f"{label}+"})
    image_name = prob_df['Image'].unique()
    if len(image_name) != 1:
        raise ValueError('Incorrect number of images captured in _PRED.tsv files: {}'.fortmat(len(image_name)))
    image_file = os.path.join(qFile_path, image_name[0] + '_LABELED.tsv')
    with open(image_file, 'r') as f:
        header = f.readline().strip().split('\t') # get only the header (column names)
    cols_to_keep = [col for col in header if (label in col and 'Median' in col)] # Find specific columns to load
    cols_to_keep += ['Centroid X µm', 'Centroid Y µm']
    image_df = pd.read_csv(image_file, usecols=cols_to_keep, sep='\t') # Read only those columns
    merge_df = prob_df.merge(image_df, on = ['Centroid X µm', 'Centroid Y µm'])
    hue_vals = [label + x for x in ['-', '+']]
    # Plot
    x_col = label + ': Cell: Median'
    y_col = 'Probabilities'
    if x_col not in merge_df.columns:

        print(f"[WARN] Skipping plot: column '{x_col}' not found in DataFrame.")
    else:
        tmp = sns.lmplot(
            x=x_col, y=y_col, data=merge_df, logistic=True,
            line_kws={'color': 'black'}, ci=None
        )
        # Generate new plt.figure()
        plt.figure()
        # Get the line data
        reg_line = tmp.ax.lines[0].get_data()
        x_fit, y_fit = reg_line  # x and y of the regression line
        plt.close()  # Close the temp plot

        g = sns.lmplot(
            x=x_col, y=y_col, hue='Predictions', hue_order=hue_vals, data=merge_df, logistic=False,
        scatter_kws={'alpha': 0.6}, y_jitter=0.025, legend=False, fit_reg=False # main data plot with hue (no regression lines)
    )
    ax = g.ax
    ax.plot(x_fit, y_fit, color="black", linewidth=2, label='Global Logistic Fit') # Add the extracted regression line to the current plot

    plot_name = f"{img_id}_{label}_probability-distribution.png"
    ax.legend() # add legend for the logistic curve
    plt.title('Probability & Intensity Distribution ' + label)
    plt.savefig(plot_name, bbox_inches='tight')
    plt.close()
    curve_files.append(plot_name)

### Match plots by Marker ###
marker_vals = [x.replace('Prediction_','') for x in prediction_cols]
# Index plots by identifier
plot_dict = {get_identifier(f, marker_vals): f for f in plot_files}
curve_dict = {get_identifier(f, marker_vals): f for f in curve_files}
# Combine identifiers
all_ids = sorted(set(plot_dict.keys()) | set(curve_dict.keys()))

# Generate a simple HTML report
with open(f"{img_id}_report.html", "w") as f:
    f.write(f"<html><head><title>Report for {img_id}</title></head><body>")
    f.write(f"<h1>Report for {img_id}</h1>")
    for id_ in all_ids:
        f.write(f"<h2>{id_}</h2>\n")
        f.write('<div style="display: flex; gap: 20px;">\n')
        if id_ in plot_dict:
            f.write(f'<div><img src="{plot_dict[id_]}" alt="{plot_dict[id_]}" height="400"></div>\n')
        if id_ in curve_dict:
            f.write(f'<div><img src="{curve_dict[id_]}" alt="{curve_dict[id_]}" height="400"></div>\n')
        f.write('</div><hr>\n')
    f.write("</body></html>")