#!/usr/bin/env python3
import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from scipy.signal import argrelextrema
from scipy.stats import gaussian_kde
from sklearn.mixture import GaussianMixture
from sklearn.preprocessing import PowerTransformer

sns.set(style="whitegrid")

META_COL_PATTERNS = (
    'Centroid',
    'Classification',
    'Name',
    'Image',
    'ROI',
    'Binary',
)


def is_feature_col(colname: str) -> bool:
    cname = str(colname)
    return not any(pat.lower() in cname.lower() for pat in META_COL_PATTERNS)


def gmm_gate_column(values, random_state=0):
    x = np.asarray(values, dtype=float)
    x = np.where(np.isfinite(x), x, 0.0)
    X = x.reshape(-1, 1)

    gmm1 = GaussianMixture(n_components=1, random_state=random_state).fit(X)
    gmm2 = GaussianMixture(n_components=2, random_state=random_state).fit(X)
    best = gmm2 if gmm2.bic(X) < gmm1.bic(X) else gmm1

    means = best.means_.flatten()
    covs = np.array(best.covariances_).reshape(-1)
    stds = np.sqrt(np.maximum(covs, 1e-12))
    order = np.argsort(means)

    bg_mean = means[order[0]]
    bg_std = stds[order[0]] if stds[order[0]] > 0 else np.std(x) + 1e-8
    threshold = bg_mean + 2.0 * bg_std

    if np.nanmax(x) > np.nanmin(x):
        try:
            kde = gaussian_kde(x)
            grid = np.linspace(np.nanmin(x), np.nanmax(x), 1000)
            pdf = kde(grid)
            mins = argrelextrema(pdf, np.less)[0]
            if len(mins) > 0:
                threshold = min(threshold, float(grid[mins[0]]))
        except Exception:
            pass

    gated = np.where(x < threshold, threshold, x)
    return gated, float(threshold), int(best.n_components), means.tolist(), stds.tolist()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input_table')
    parser.add_argument('--output-table', required=True)
    parser.add_argument('--summary-csv', required=True)
    parser.add_argument('--summary-plot', required=True)
    parser.add_argument('--seed', type=int, default=421)
    parser.add_argument('--run-gmmgating', action='store_true')
    parser.add_argument('--run-powertransform', action='store_true')
    args = parser.parse_args()

    df = pd.read_csv(args.input_table, sep='\t', low_memory=False)
    out_df = df.copy()

    feature_cols = [c for c in df.columns if is_feature_col(c)]
    if not feature_cols:
        out_df.to_csv(args.output_table, sep='\t', index=False)
        pd.DataFrame(columns=['feature', 'threshold', 'percent_gated']).to_csv(args.summary_csv, index=False)
        Path(args.summary_plot).touch()
        return

    numeric_block = df[feature_cols].apply(pd.to_numeric, errors='coerce').fillna(0.0)
    gating_rows = []

    if args.run_gmmgating:
        for col in numeric_block.columns:
            pre = numeric_block[col].astype(float).values
            post, threshold, n_components, means, stds = gmm_gate_column(pre, random_state=args.seed)
            numeric_block[col] = post
            gating_rows.append({
                'feature': col,
                'threshold': threshold,
                'n_components': n_components,
                'gmm_means': means,
                'gmm_stds': stds,
                'pre_mean': float(np.mean(pre)),
                'post_mean': float(np.mean(post)),
                'delta_mean': float(np.mean(post - pre)),
                'cells_gated': int(np.sum(pre < threshold)),
                'percent_gated': float(np.mean(pre < threshold) * 100.0),
            })
    else:
        for col in numeric_block.columns:
            gating_rows.append({'feature': col, 'threshold': np.nan, 'n_components': 0, 'gmm_means': [], 'gmm_stds': [], 'pre_mean': float(numeric_block[col].mean()), 'post_mean': float(numeric_block[col].mean()), 'delta_mean': 0.0, 'cells_gated': 0, 'percent_gated': 0.0})

    if args.run_powertransform:
        pt = PowerTransformer(method='yeo-johnson', standardize=False)
        numeric_block = pd.DataFrame(pt.fit_transform(numeric_block), columns=numeric_block.columns, index=numeric_block.index)

    out_df.loc[:, feature_cols] = numeric_block
    out_df.to_csv(args.output_table, sep='\t', index=False)

    summary = pd.DataFrame(gating_rows)
    summary.to_csv(args.summary_csv, index=False)

    plt.figure(figsize=(8, 4))
    sns.histplot(summary['percent_gated'], bins=30, kde=True)
    plt.title('Distribution of percent gated across features')
    plt.tight_layout()
    plt.savefig(args.summary_plot, dpi=120)
    plt.close()


if __name__ == '__main__':
    main()
