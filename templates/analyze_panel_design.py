#!/usr/bin/env python3

import os, time
import pandas as pd
import matplotlib.pyplot as plt
from fpdf import FPDF
from matplotlib.backends.backend_pdf import PdfPages


def generate_histograms_and_stats(tsv_files, output_pdf):
    """
    Generates histograms for markers from input TSV files and appends statistics
    to a PDF report.
    """
    # Initialize PDF for combining plots
    pdf_pages = PdfPages(output_pdf)
    stats_text = []

    for file in tsv_files:
        try:
            # Load TSV file and select numeric columns
            df = pd.read_csv(file, sep="\t", low_memory=False)
            numeric_df = df.select_dtypes(include=["number"])
        except Exception as e:
            print(f"Error reading {file}: {e}")
            continue

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

    pdf_pages.close()

    # Create a separate PDF with the text-based statistics
    generate_statistics_pdf(stats_text, len(markers), "Statistics_Report.pdf")

    print(f"Histogram report saved as {output_pdf}")
    print("Statistics report saved as Statistics_Report.pdf")


def generate_statistics_pdf(stats_text, nMrks, output_pdf):
    """
    Generates a PDF report for the collected statistics text.
    """
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.image("${params.letterhead}", 0, 0)     
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
    tsv_files = "${tables_collected}".split(' ')
    output_pdf = "Marker_Analysis_Report.pdf"
    generate_histograms_and_stats(tsv_files, output_pdf)

    
