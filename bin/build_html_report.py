#!/usr/bin/env python3
import argparse
import html
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def summarize_tsv(path: Path, out_dir: Path, idx: int):
    info = []
    figs = []
    try:
        df = pd.read_csv(path, sep='\t', low_memory=False)
        info.append(f"<p><b>Rows:</b> {len(df):,} &nbsp; <b>Columns:</b> {len(df.columns):,}</p>")
        info.append("<h4>Column preview</h4>" + pd.DataFrame({'column': df.columns}).head(40).to_html(index=False))
        info.append("<h4>Head (first 5 rows)</h4>" + df.head(5).to_html(index=False))
        num = df.select_dtypes(include='number')
        if not num.empty:
            info.append("<h4>Numeric describe</h4>" + num.describe().T.head(30).to_html())
            col = num.columns[0]
            fig_name = f"report_{idx}_{path.stem}_{col}_hist.png".replace('/', '_')
            fig_path = out_dir / fig_name
            plt.figure(figsize=(6, 3))
            num[col].dropna().hist(bins=40)
            plt.title(f"{path.name}: {col} distribution")
            plt.tight_layout()
            plt.savefig(fig_path, dpi=120)
            plt.close()
            figs.append(fig_name)
    except Exception as e:
        info.append(f"<p>Failed to parse TSV: {html.escape(str(e))}</p>")
    return "\n".join(info), figs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--title', required=True)
    ap.add_argument('--output', required=True)
    ap.add_argument('--inputs', nargs='*', default=[])
    ap.add_argument('--notes', default='')
    args = ap.parse_args()

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)

    sections = []
    sections.append(f"<h1>{html.escape(args.title)}</h1>")
    if args.notes:
        sections.append(f"<p>{html.escape(args.notes)}</p>")

    sections.append("<h2>Input files</h2><ul>" + ''.join([f"<li>{html.escape(str(p))}</li>" for p in args.inputs]) + "</ul>")

    for i, p in enumerate(args.inputs):
        pth = Path(p)
        sections.append(f"<hr><h3>{html.escape(pth.name)}</h3>")
        sections.append(f"<p><b>Exists:</b> {pth.exists()} &nbsp; <b>Size bytes:</b> {(pth.stat().st_size if pth.exists() else 0):,}</p>")
        if pth.suffix.lower() == '.tsv' and pth.exists():
            txt, figs = summarize_tsv(pth, out.parent, i)
            sections.append(txt)
            for f in figs:
                sections.append(f'<img src="{html.escape(f)}" style="max-width:900px;">')
        elif pth.suffix.lower() in ('.png', '.jpg', '.jpeg', '.svg') and pth.exists():
            sections.append(f'<img src="{html.escape(pth.name)}" style="max-width:900px;">')

    out.write_text("<html><body style='font-family:Arial'>" + '\n'.join(sections) + "</body></html>", encoding='utf-8')


if __name__ == '__main__':
    main()
