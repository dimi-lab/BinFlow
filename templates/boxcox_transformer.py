#!/usr/bin/env python3

import os
import re
import time
import fnmatch
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from fpdf import FPDF
from scipy.stats import boxcox

# Static config
quantType = '${params.qupath_object_type}'
nucMark = '${params.nucleus_marker}'
plotFraction = 0.25
grouping_column = '${params.transformation_group_by_column}'
hasFOV = True

def create_title(title, pdf):
    pdf.set_font('Helvetica', 'b', 20)
    pdf.ln(40)
    pdf.write(5, title)
    pdf.ln(10)
    pdf.set_font('Helvetica', '', 14)
    pdf.set_text_color(r=128, g=128, b=128)
    today = time.strftime("%d/%m/%Y")
    pdf.write(4, f'{today}')
    pdf.ln(10)

def write_to_pdf(pdf, words):
    pdf.set_text_color(r=0, g=0, b=0)
    pdf.set_font('Helvetica', '', 12)
    pdf.write(5, words)

def get_max_value(df):
    values = df.values.flatten()
    filtered_values = values[np.isfinite(values)]
    return np.max(filtered_values) if filtered_values.size > 0 else 65535

def collect_and_transform(df, batchName):
    # Sample for plotting
    smTble = df.sample(frac=plotFraction, random_state=42) if len(df) > 1 else df.copy()
    # Melt for combined marker distribution (original)
    df_batching = smTble.filter(regex=f"(Mean|Median|{grouping_column})", axis=1)
    df_melted = pd.melt(df_batching, id_vars=[grouping_column])
    plt.figure(figsize=(20, 8))
    sns.boxplot(x=grouping_column, y='value', color="#CD7F32", data=df_melted, showfliers=False)
    plt.xticks(rotation=40, ha="right")
    plt.title('Combined Marker Distribution (original values)')
    plt.tight_layout()
    plt.savefig("original_marker_sample_boxplots.png")
    plt.close()

    # Select features for BoxCox
    if quantType == 'CellObject':
        df_batching2 = smTble.filter(regex='Cell: Mean', axis=1)
    else:
        df_batching2 = smTble.filter(regex='Mean', axis=1)
    df_batching2 = df_batching2.loc[:, df_batching2.nunique() > 1]

    # BoxCox transformation
    bcDf = df.copy(deep=True).fillna(0)
    metrics = []
    for fld in bcDf.filter(regex='(Min|Max|Median|Mean|StdDev)'):
        try:
            nArr, mxLambda = boxcox(bcDf[fld].add(1).values)
            bcDf[fld] = nArr
            mxLambda = f"{mxLambda:.3f}"
        except Exception:
            bcDf[fld] = 0
            mxLambda = 'Failed'
        metrics.append([
            fld,
            bcDf[fld].mean(),
            mxLambda,
            bcDf[fld].mean(),
            bcDf[fld].min(),
            bcDf[fld].max()
        ])
    bxcxMetrics = pd.DataFrame(metrics, columns=['Feature', 'Pre_Mean', 'Lambda', 'Post_Mean', 'Post_Min', 'Post_Max'])
    bxcxMetrics.to_csv("BoxCoxRecord.csv", index=False)

    # Pre vs Post mean scatter
    tmpPlot = bxcxMetrics[bxcxMetrics['Lambda'] != 'Failed']
    plt.figure(figsize=(10, 10))
    plt.scatter(tmpPlot['Pre_Mean'], tmpPlot['Post_Mean'])
    plt.title("Feature Avg Pre v. Post (BoxCox)")
    plt.xlabel("Pre_Mean")
    plt.ylabel("Post_Mean")
    plt.tight_layout()
    plt.savefig("boxcox_delta_values.png")
    plt.close()

    # Nucleus marker column
    myFields = df_batching2.columns.to_list()
    nuc_cols = [x for x in myFields if nucMark in x]
    if not nuc_cols:
        raise ValueError(f"No column found containing nucleus marker '{nucMark}' in columns: {myFields}")
    NucOnly = nuc_cols[0]

    # Density plots for each feature
    for idx, fld in enumerate(myFields):
        if fld == NucOnly:
            continue
        da = df_batching2[[NucOnly, fld]].add_suffix(' Original')
        dB = bcDf[[NucOnly, fld]].add_suffix(' Transformed')
        tmpMerge = pd.concat([da, dB], axis=0, ignore_index=True)
        maxX = get_max_value(tmpMerge)
        plt.figure(figsize=(8, 3))
        tmpMerge.plot.density(ax=plt.gca(), linewidth=3)
        plt.title(f"{fld} Distributions")
        plt.xlim(0, maxX)
        plt.tight_layout()
        plt.savefig(f"original_value_density_{idx}.png")
        plt.close()

    # Combined marker distribution (quantile values)
    df_batching = smTble.filter(regex=f"(Mean|Median|{grouping_column})", axis=1)
    df_melted = pd.melt(df_batching, id_vars=[grouping_column])
    plt.figure(figsize=(20, 8))
    sns.boxplot(x=grouping_column, y='value', color="#50C878", data=df_melted, showfliers=False)
    plt.xticks(rotation=40, ha="right")
    plt.title('Combined Marker Distribution (quantile values)')
    plt.tight_layout()
    plt.savefig("normlize_marker_sample_boxplots.png")
    plt.close()

    # QQ plots for each marker (4 per page)
    colNames = [x for x in df.columns if 'Mean' in x]
    nuc_cols = [x for x in colNames if nucMark in x]
    if not nuc_cols:
        raise ValueError(f"No column found containing nucleus marker '{nucMark}' in columns: {colNames}")
    NucOnly = nuc_cols[0]
    for i in range(0, len(colNames), 4):
        fig, axs = plt.subplots(2, 2, figsize=(8, 8))
        axs = axs.flatten()
        for j in range(4):
            if i + j < len(colNames):
                hd = colNames[i + j]
                nuc1 = pd.DataFrame({"Original_Value": df[NucOnly], "Transformed_Value": bcDf[NucOnly]})
                nuc1['Mark'] = nucMark
                mk2 = pd.DataFrame({"Original_Value": df[hd], "Transformed_Value": bcDf[hd]})
                mk2['Mark'] = hd.split(":")[0]
                qqDF = pd.concat([nuc1, mk2], ignore_index=True)
                ax2 = axs[j]
                sns.scatterplot(x='Original_Value', y='Transformed_Value', data=qqDF, hue="Mark", ax=ax2)
                ax2.set_title(f"BoxCox: {hd}")
                ax2.axline((0, 0), (nuc1['Original_Value'].max(), nuc1['Transformed_Value'].max()), linewidth=2, color='r')
            else:
                axs[j].axis('off')
        plt.tight_layout()
        plt.savefig(f"normlize_qrq_{i}.png")
        plt.close()
    bcDf.to_csv(f"{batchName}_boxcox_mod.tsv", sep="\t", index=False)

def generate_pdf_report(outfilename, batchName):
    WIDTH = 215.9
    pdf = FPDF()
    pdf.add_page()
    create_title(f"Log Transformation: {batchName}", pdf)
    pdf.image("${params.letterhead}", 0, 0, WIDTH)
    write_to_pdf(pdf, "Fig 1.a: Distribution of all markers combined summarized by biospecimen.")
    pdf.ln(5)
    pdf.image('original_marker_sample_boxplots.png', w=(WIDTH*0.95))
    pdf.ln(15)
    pdf.image('normlize_marker_sample_boxplots.png', w=(WIDTH*0.95))
    pdf.ln(15)
    pdf.add_page()
    write_to_pdf(pdf, "Fig 5: Transformation Plots.")
    pdf.ln(10)
    for file in sorted(fnmatch.filter(os.listdir('.'), "normlize_qrq_*")):
        pdf.image(file, w=WIDTH)
        pdf.ln(5)
    write_to_pdf(pdf, "Fig 3: Total cell population distributions.")
    pdf.ln(10)
    for file in sorted(fnmatch.filter(os.listdir('.'), "original_value_density_*")):
        pdf.image(file, w=WIDTH)
        pdf.ln(5)
    pdf.image('boxcox_delta_values.png', w=WIDTH)
    pdf.output(outfilename, 'F')

if __name__ == "__main__":
    myData = pd.read_csv("${quant_table}", sep="\t", low_memory=False)
    filename = os.path.basename("${quant_table}")

    if hasFOV:
        first_part = filename.split('_')[0]
        match = re.search(r'___(\\d{3})\\.ome', filename)
        if match:
            fov_part = match.group(1)
        else:
            match = re.search(r'(\\d{3})\\.ome', filename)
            fov_part = match.group(1) if match else "UNK"
        myFileIdx = f"{first_part}_{fov_part}"
    else:
        myFileIdx = filename.split('_')[0]
            
    collect_and_transform(myData, myFileIdx)
    generate_pdf_report(f"boxcox_report_{myFileIdx}.pdf", myFileIdx)




