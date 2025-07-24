#!/usr/bin/env python3

import os
import re
import time
import pandas as pd
import matplotlib.pyplot as plt
from fpdf import FPDF
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np


def load_selected_columns(file_path, chunk_size=40000):
    """
    Load specific columns from a large file in chunks and combine them into a single DataFrame.
    Excludes columns matching (Cytoplasm|Variance).
    """
    selected_columns = []
    all_chunks = []
    # Identify columns to keep
    for first_chunk in pd.read_csv(file_path, chunksize=1, low_memory=False, sep='\t'):
        columns = first_chunk.columns
        exclude_regex = re.compile(r"(Cytoplasm|Variance)", re.IGNORECASE)
        selected_columns = [col for col in columns if not exclude_regex.search(col)]
        break
    print(f"Selected columns: {selected_columns}")
    # Read in chunks
    with pd.read_csv(file_path, usecols=selected_columns, chunksize=chunk_size, low_memory=True, sep='\t') as reader:
        for i, chunk in enumerate(reader):
            all_chunks.append(chunk)
            print(f"Processed chunk {i + 1}")
    allLnData = pd.concat(all_chunks, axis=0, ignore_index=True)
    return allLnData


def process_large_file(file_path, pdf_pages, stats_text, chunk_size=40000):
    # Identify columns to keep
    for first_chunk in pd.read_csv(file_path, chunksize=1, low_memory=False, sep='\t'):
        columns = first_chunk.columns
        exclude_regex = re.compile(r"(Cytoplasm|Membrane|Nucleus|Min|Variance|Std\.Dev\.)", re.IGNORECASE)
        selected_columns = [col for col in columns if not exclude_regex.search(col)]
        break

    file_basename = os.path.basename(file_path).replace(".tsv", "")

    # Running stats: {marker: [count, sum, sumsq, min, max]}
    running_stats = {}
    hist_bins = 20
    hist_data = {}

    # Process each chunk
    for chunk in pd.read_csv(file_path, usecols=selected_columns, chunksize=chunk_size, low_memory=True, sep='\t'):
        numeric_chunk = chunk.select_dtypes(include=["number"])
        markers = [col for col in numeric_chunk.columns if "Median" in col and ("+" in col or "-" in col)]
        if not markers:
            continue

        # Update running stats and histograms
        for marker in markers:
            vals = numeric_chunk[marker].dropna().values
            if vals.size == 0:
                continue
            # Running stats
            if marker not in running_stats:
                running_stats[marker] = {
                    'count': 0, 'sum': 0.0, 'sumsq': 0.0, 'min': np.inf, 'max': -np.inf
                }
                hist_data[marker] = np.zeros(hist_bins)
            rs = running_stats[marker]
            rs['count'] += vals.size
            rs['sum'] += vals.sum()
            rs['sumsq'] += np.sum(vals ** 2)
            rs['min'] = min(rs['min'], np.min(vals))
            rs['max'] = max(rs['max'], np.max(vals))
            # Histogram (aggregate)
            hist, bin_edges = np.histogram(vals, bins=hist_bins, range=(rs['min'], rs['max']))
            hist_data[marker][:] += hist

    # Generate histograms and stats for each marker
    for marker in running_stats:
        # Plot histogram
        fig, ax = plt.subplots(figsize=(10, 6))
        bin_edges = np.linspace(running_stats[marker]['min'], running_stats[marker]['max'], hist_bins + 1)
        ax.bar((bin_edges[:-1] + bin_edges[1:]) / 2, hist_data[marker], width=(bin_edges[1] - bin_edges[0]), color="steelblue", edgecolor="black")
        ax.set_title(marker, fontsize=14)
        ax.set_xlabel("Value", fontsize=12)
        ax.set_ylabel("Count", fontsize=12)
        plt.tight_layout()
        pdf_pages.savefig(fig)
        plt.close(fig)

        # Compute statistics
        count = running_stats[marker]['count']
        mean = running_stats[marker]['sum'] / count if count else float('nan')
        var = (running_stats[marker]['sumsq'] / count - mean ** 2) if count else float('nan')
        std = np.sqrt(var) if var >= 0 else float('nan')
        minv = running_stats[marker]['min']
        maxv = running_stats[marker]['max']

        stats_text.append(f"Marker: {marker}")
        stats_text.append(f"  count: {count}")
        stats_text.append(f"  mean: {mean:.2f}")
        stats_text.append(f"  std: {std:.2f}")
        stats_text.append(f"  min: {minv:.2f}")
        stats_text.append(f"  max: {maxv:.2f}")
        stats_text.append("")


def generate_histograms_and_stats(tsv_files, output_pdf, letterhead):
    """
    Generates histograms for markers from input TSV files and appends statistics
    to a PDF report.
    """
    # Initialize PDF for combining plots
    pdf_pages = PdfPages(output_pdf)
    stats_text = []

    for file in tsv_files:
        try:
            file_size_mb = os.path.getsize(file) / (1024 * 1024)
            if file_size_mb > 800:
                print(f"File {file} is large ({file_size_mb:.1f} MB), using chunked reading.")
                process_large_file(file, pdf_pages, stats_text)
            else:
                df = pd.read_csv(file, sep="\t", low_memory=False)
                numeric_df = df.select_dtypes(include=["number"])

                file_basename = os.path.basename(file).replace(".tsv", "")
                markers = [col for col in numeric_df.columns if "Median" in col and ("+" in col or "-" in col)]

                if not markers:
                    print(f"No markers found in {file}.")
                    continue

                # Split markers into groups of three for plotting
                marker_groups = [markers[i:i + 3] for i in range(0, len(markers), 3)]

                # Generate histograms for each marker group
                for idx, group in enumerate(marker_groups):
                    fig, axes = plt.subplots(1, len(group), figsize=(15, 5))

                    if len(group) == 1:  # Ensure axes is always iterable
                        axes = [axes]

                    for ax, marker in zip(axes, group):
                        ax.hist(numeric_df[marker].dropna(), bins=20, color="steelblue", edgecolor="black")
                        ax.set_title(marker, fontsize=10)
                        ax.set_xlabel("Value", fontsize=8)
                        ax.set_ylabel("Count", fontsize=8)

                    plt.tight_layout()
                    pdf_pages.savefig(fig)  # Save the current figure to the PDF
                    plt.close(fig)

                # Collect statistics for the file
                stats_text.append(f"""Statistics for {file_basename}: 
                """)
                for marker in markers:
                    stats = numeric_df[marker].describe()
                    stats_text.append(f"Marker: {marker}")
                    for stat_name, value in stats.items():
                        stats_text.append(f"  {stat_name}: {value:.2f}")
                    stats_text.append("""
                     """)
        except Exception as e:
            print(f"Error processing {file}: {e}")
            continue

    pdf_pages.close()

    # Create a separate PDF with the text-based statistics
    generate_statistics_pdf(stats_text, len(stats_text), "Statistics_Report.pdf", letterhead)

    print(f"Histogram report saved as {output_pdf}")
    print("Statistics report saved as Statistics_Report.pdf")


def generate_statistics_pdf(stats_text, nMrks, output_pdf, letterhead):
    """
    Generates a PDF report for the collected statistics text.
    """
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    if letterhead and os.path.exists(letterhead):
        pdf.image(letterhead, 0, 0)
    pdf.set_font("Arial", size=12)
    
    # Add title
    pdf.set_font("Arial", size=16)
    pdf.cell(200, 10, txt="Marker Analysis Report", ln=True, align='C')
    # Add date of report
    pdf.set_font('Helvetica', '', 14)
    pdf.set_text_color(r=128,g=128,b=128)
    today = time.strftime("%d/%m/%Y")
    pdf.write(4, f'{today}')
    # Add line break
    pdf.ln(10) 
    
    pdf.set_font("Arial", size=12)
    pdf.cell(200, 10, txt=f"Total unique markers: {nMrks}", ln=True)

    for line in stats_text:
        pdf.cell(0, 10, txt=line, ln=True)

    pdf.output(output_pdf)
    print(f"Statistics PDF report saved as {output_pdf}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: analyze_panel_design.py <letterhead> <tsv1> [<tsv2> ...]")
        sys.exit(1)
    letterhead = sys.argv[1]
    tsv_files = sys.argv[2:]
    output_pdf = "Marker_Analysis_Report.pdf"
    generate_histograms_and_stats(tsv_files, output_pdf, letterhead)


