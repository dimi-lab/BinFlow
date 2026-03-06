#!/usr/bin/env python3
import argparse
import json
import re
from datetime import datetime
from pathlib import Path

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from scipy.signal import argrelextrema
from scipy.stats import gaussian_kde, kurtosis, loguniform, randint, skew, uniform
from sklearn.cluster import KMeans
from sklearn.compose import ColumnTransformer
from sklearn.decomposition import PCA
from sklearn.ensemble import ExtraTreesClassifier, IsolationForest, RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.manifold import TSNE
from sklearn.metrics import (
    accuracy_score,
    adjusted_rand_score,
    average_precision_score,
    balanced_accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
    normalized_mutual_info_score,
    roc_auc_score,
    silhouette_score,
)
from sklearn.mixture import GaussianMixture
from sklearn.model_selection import GroupShuffleSplit, RandomizedSearchCV, StratifiedKFold, StratifiedGroupKFold
from sklearn.neighbors import LocalOutlierFactor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import PowerTransformer, RobustScaler, StandardScaler
from sklearn.svm import SVC

sns.set(style="whitegrid")


def parse_classification_value(classification_string, marker='NAK'):
    if pd.isna(classification_string):
        return np.nan
    token_map = {}
    for token in str(classification_string).split('|'):
        token = token.strip()
        m = re.match(r'^(.+?)([+-])$', token)
        if m:
            token_map[m.group(1)] = m.group(2)
    if marker not in token_map:
        return np.nan
    return 1 if token_map[marker] == '+' else 0


def find_marker_feature_columns(df, marker='NAK', exclude_component_patterns=None):
    marker_cols = [c for c in df.columns if str(c).startswith(marker)]
    if not exclude_component_patterns:
        return marker_cols
    return [c for c in marker_cols if not any(p in str(c) for p in exclude_component_patterns if p)]


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
    threshold = means[order[0]] + 2.0 * (stds[order[0]] if stds[order[0]] > 0 else np.std(x) + 1e-8)
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


def apply_gmm_gating_to_matrix(X_df, random_state=0):
    gated = X_df.copy()
    rows = []
    for col in gated.columns:
        pre = gated[col].astype(float).fillna(0.0).values
        post, threshold, n_components, means, stds = gmm_gate_column(pre, random_state=random_state)
        gated[col] = post
        rows.append({
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
    return gated, pd.DataFrame(rows)


def build_preprocessor(numeric_features, scaler='standard'):
    scaler_obj = StandardScaler() if scaler == 'standard' else RobustScaler()
    numeric_transformer = Pipeline(steps=[('imputer', SimpleImputer(strategy='median')), ('scaler', scaler_obj)])
    return ColumnTransformer(transformers=[('num', numeric_transformer, numeric_features)], remainder='drop')


def build_model_candidates(preprocessor, n_iter=30, cv=5, random_state=421):
    models = {}
    rf = Pipeline([('preprocessor', preprocessor), ('classifier', RandomForestClassifier(random_state=random_state, n_jobs=-1))])
    rf_grid = {
        'classifier__n_estimators': randint(150, 500),
        'classifier__max_depth': [None] + list(range(3, 14)),
        'classifier__min_samples_split': randint(2, 20),
        'classifier__min_samples_leaf': randint(1, 10),
        'classifier__max_features': ['sqrt', 'log2', None],
        'classifier__criterion': ['gini', 'entropy', 'log_loss'],
        'classifier__class_weight': [None, 'balanced', 'balanced_subsample'],
    }
    models['RandomForest'] = RandomizedSearchCV(rf, rf_grid, n_iter=n_iter, scoring='average_precision', cv=cv, random_state=random_state, n_jobs=-1)
    et = Pipeline([('preprocessor', preprocessor), ('classifier', ExtraTreesClassifier(random_state=random_state, n_jobs=-1))])
    models['ExtraTrees'] = RandomizedSearchCV(et, rf_grid.copy(), n_iter=n_iter, scoring='average_precision', cv=cv, random_state=random_state, n_jobs=-1)
    lr = Pipeline([('preprocessor', preprocessor), ('classifier', LogisticRegression(max_iter=5000, random_state=random_state))])
    lr_grid = {
        'classifier__C': loguniform(1e-4, 1e3),
        'classifier__penalty': ['l2', 'l1', 'elasticnet'],
        'classifier__solver': ['liblinear', 'saga'],
        'classifier__tol': loguniform(1e-6, 1e-2),
        'classifier__l1_ratio': uniform(0, 1),
        'classifier__class_weight': [None, 'balanced'],
    }
    models['LogisticRegression'] = RandomizedSearchCV(lr, lr_grid, n_iter=n_iter, scoring='average_precision', cv=cv, random_state=random_state, n_jobs=-1, error_score=np.nan)
    svm = Pipeline([('preprocessor', preprocessor), ('classifier', SVC(probability=True, random_state=random_state))])
    svm_grid = {
        'classifier__C': loguniform(1e-3, 1e3),
        'classifier__gamma': ['scale', 'auto'],
        'classifier__kernel': ['rbf', 'poly', 'sigmoid'],
        'classifier__class_weight': [None, 'balanced'],
    }
    models['SVC'] = RandomizedSearchCV(svm, svm_grid, n_iter=max(10, n_iter // 2), scoring='average_precision', cv=cv, random_state=random_state, n_jobs=-1, error_score=np.nan)
    return models


def evaluate_supervised_models(models, X_train, y_train, X_eval, y_eval, sample_weight_train=None, groups_train=None):
    records = []
    fitted = {}
    for name, search in models.items():
        if sample_weight_train is not None:
            search.fit(X_train, y_train, classifier__sample_weight=np.asarray(sample_weight_train), groups=groups_train)
        else:
            search.fit(X_train, y_train, groups=groups_train)
        y_pred = search.predict(X_eval)
        y_score = search.predict_proba(X_eval)[:, 1] if hasattr(search, 'predict_proba') else None
        records.append({
            'model': name,
            'accuracy': accuracy_score(y_eval, y_pred),
            'f1': f1_score(y_eval, y_pred, zero_division=0),
            'roc_auc': roc_auc_score(y_eval, y_score) if y_score is not None else np.nan,
            'pr_auc': average_precision_score(y_eval, y_score) if y_score is not None else np.nan,
            'balanced_accuracy': balanced_accuracy_score(y_eval, y_pred),
            'best_params': search.best_params_,
            'classification_report': classification_report(y_eval, y_pred, zero_division=0, output_dict=True),
            'confusion_matrix': confusion_matrix(y_eval, y_pred).tolist(),
        })
        fitted[name] = search
    leaderboard = pd.DataFrame([{k: v for k, v in r.items() if k not in ['best_params', 'classification_report', 'confusion_matrix']} for r in records])
    leaderboard = leaderboard.sort_values(['pr_auc', 'balanced_accuracy', 'f1'], ascending=False).reset_index(drop=True)
    return leaderboard, records, fitted


def main():
    p = argparse.ArgumentParser()
    p.add_argument('tables', nargs='+')
    p.add_argument('--marker', default='NAK')
    p.add_argument('--classification-col', default='Classification')
    p.add_argument('--output-dir', required=True)
    p.add_argument('--seed', type=int, default=421)
    p.add_argument('--test-size', type=float, default=0.3)
    p.add_argument('--cv-splits', type=int, default=5)
    p.add_argument('--n-iter-search', type=int, default=30)
    p.add_argument('--outlier-contamination', type=float, default=0.02)
    p.add_argument('--run-gmmgating', action='store_true')
    p.add_argument('--run-powertransform', action='store_true')
    p.add_argument('--exclude-component-patterns', default='')
    args = p.parse_args()

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    frames = []
    for fp in args.tables:
        d = pd.read_csv(fp, sep='\t', low_memory=False)
        d['source_file'] = Path(fp).name
        frames.append(d)
    df = pd.concat(frames, axis=0, ignore_index=True, sort=False)

    df[f'{args.marker}_label'] = df[args.classification_col].apply(lambda x: parse_classification_value(x, marker=args.marker))
    work_df = df[~df[f'{args.marker}_label'].isna()].copy()
    work_df[f'{args.marker}_label'] = work_df[f'{args.marker}_label'].astype(int)

    excl = [x.strip() for x in args.exclude_component_patterns.split(',') if x.strip()]
    marker_cols = find_marker_feature_columns(df, marker=args.marker, exclude_component_patterns=excl)
    X_all = df[marker_cols].apply(pd.to_numeric, errors='coerce').fillna(0.0)

    if args.run_gmmgating:
        X_all, gating = apply_gmm_gating_to_matrix(X_all, random_state=args.seed)
    else:
        gating = pd.DataFrame({'feature': marker_cols, 'threshold': np.nan, 'percent_gated': 0.0})
    gating.to_csv(out / f'{args.marker}_gmm_gating_summary.csv', index=False)

    plt.figure(figsize=(8, 4))
    sns.histplot(gating['percent_gated'], bins=30, kde=True)
    plt.title('Distribution of percent gated across features')
    plt.tight_layout()
    plt.savefig(out / f'{args.marker}_gating_percent_hist.png', dpi=120)
    plt.close()

    if args.run_powertransform:
        pt = PowerTransformer(method='yeo-johnson', standardize=False)
        X_all = pd.DataFrame(pt.fit_transform(X_all), columns=X_all.columns, index=X_all.index)
        joblib.dump(pt, out / f'{args.marker}_power_transformer.joblib')

    X = X_all.loc[work_df.index].copy()
    y = work_df[f'{args.marker}_label'].astype(int)

    imp = SimpleImputer(strategy='median')
    scaler = RobustScaler()
    X_scaled = scaler.fit_transform(imp.fit_transform(X))
    iso = IsolationForest(contamination=args.outlier_contamination, random_state=args.seed, n_jobs=-1)
    lof = LocalOutlierFactor(n_neighbors=35, contamination=args.outlier_contamination)
    is_outlier = (iso.fit_predict(X_scaled) == -1) | (lof.fit_predict(X_scaled) == -1)
    outlier_flag = pd.Series(is_outlier, index=X.index, name='is_outlier').astype(int)

    pca2 = PCA(n_components=2, random_state=args.seed)
    Z = pca2.fit_transform(X_scaled)
    plot_df = pd.DataFrame({'PC1': Z[:, 0], 'PC2': Z[:, 1], 'is_outlier': is_outlier, 'label': y.values})
    plt.figure(figsize=(7, 5))
    sns.scatterplot(data=plot_df, x='PC1', y='PC2', hue='is_outlier', style='label', alpha=0.6, s=20)
    plt.tight_layout()
    plt.savefig(out / f'{args.marker}_outlier_pca.png', dpi=120)
    plt.close()

    X_inlier = X.loc[~is_outlier]
    y_inlier = y.loc[~is_outlier]
    X_inlier_scaled = scaler.transform(imp.transform(X_inlier))

    cluster_records = []
    for k in [2, 3, 4, 5, 6]:
        km = KMeans(n_clusters=k, random_state=args.seed, n_init='auto')
        cl = km.fit_predict(X_inlier_scaled)
        cluster_records.append({'method': 'KMeans', 'k': k, 'ARI': adjusted_rand_score(y_inlier, cl), 'NMI': normalized_mutual_info_score(y_inlier, cl), 'Silhouette': silhouette_score(X_inlier_scaled, cl)})
    for k in [2, 3, 4, 5]:
        gm = GaussianMixture(n_components=k, random_state=args.seed)
        cl = gm.fit_predict(X_inlier_scaled)
        cluster_records.append({'method': 'GMM', 'k': k, 'ARI': adjusted_rand_score(y_inlier, cl), 'NMI': normalized_mutual_info_score(y_inlier, cl), 'Silhouette': silhouette_score(X_inlier_scaled, cl)})
    cluster_results = pd.DataFrame(cluster_records)
    cluster_results['composite_score'] = 0.45 * cluster_results['ARI'] + 0.35 * cluster_results['NMI'] + 0.20 * cluster_results['Silhouette']
    cluster_results = cluster_results.sort_values(['composite_score', 'ARI', 'NMI'], ascending=False).reset_index(drop=True)
    cluster_results.to_csv(out / f'{args.marker}_cluster_results.csv', index=False)

    sample_idx = np.arange(len(X_inlier_scaled))
    if len(sample_idx) > 5000:
        rng = np.random.default_rng(args.seed)
        sample_idx = rng.choice(sample_idx, size=5000, replace=False)
    emb = TSNE(n_components=2, random_state=args.seed, init='pca', learning_rate='auto').fit_transform(X_inlier_scaled[sample_idx])
    emb_df = pd.DataFrame({'t1': emb[:, 0], 't2': emb[:, 1], 'label': y_inlier.iloc[sample_idx].values})
    plt.figure(figsize=(6, 5))
    sns.scatterplot(data=emb_df, x='t1', y='t2', hue='label', s=14)
    plt.tight_layout()
    plt.savefig(out / f'{args.marker}_tsne_labels.png', dpi=120)
    plt.close()

    groups = work_df.loc[X.index, 'source_file'].astype(str)
    sample_weight = np.where(y.values == 1, 1.0 / max(np.mean(y.values == 1), 1e-6), 1.0 / max(np.mean(y.values == 0), 1e-6))
    gss1 = GroupShuffleSplit(n_splits=1, test_size=args.test_size, random_state=args.seed)
    idx_trainval, idx_test = next(gss1.split(X, y, groups=groups))
    X_trainval, y_trainval = X.iloc[idx_trainval], y.iloc[idx_trainval]
    X_test, y_test = X.iloc[idx_test], y.iloc[idx_test]
    w_trainval = sample_weight[idx_trainval]
    groups_trainval = groups.iloc[idx_trainval]

    gss2 = GroupShuffleSplit(n_splits=1, test_size=0.25, random_state=args.seed + 1)
    idx_train, idx_val = next(gss2.split(X_trainval, y_trainval, groups=groups_trainval))
    X_train, y_train = X_trainval.iloc[idx_train], y_trainval.iloc[idx_train]
    X_val, y_val = X_trainval.iloc[idx_val], y_trainval.iloc[idx_val]
    w_train = w_trainval[idx_train]
    groups_train = groups_trainval.iloc[idx_train]

    n_groups = groups_train.nunique()
    cv = StratifiedGroupKFold(n_splits=min(args.cv_splits, n_groups), shuffle=True, random_state=args.seed) if n_groups >= 3 else StratifiedKFold(n_splits=3, shuffle=True, random_state=args.seed)

    preprocessor = build_preprocessor(X_train.columns.tolist(), scaler='standard')
    models = build_model_candidates(preprocessor, n_iter=args.n_iter_search, cv=cv, random_state=args.seed)
    leaderboard, detailed_records, fitted_models = evaluate_supervised_models(models, X_train, y_train, X_val, y_val, sample_weight_train=w_train, groups_train=groups_train)
    leaderboard.to_csv(out / f'{args.marker}_supervised_leaderboard.csv', index=False)

    plot_df = leaderboard.melt(id_vars='model', value_vars=['accuracy', 'balanced_accuracy', 'f1', 'roc_auc', 'pr_auc'], var_name='metric', value_name='value')
    plt.figure(figsize=(9, 4))
    sns.barplot(data=plot_df, x='model', y='value', hue='metric')
    plt.ylim(0, 1)
    plt.xticks(rotation=20)
    plt.tight_layout()
    plt.savefig(out / f'{args.marker}_model_leaderboard.png', dpi=120)
    plt.close()

    best_name = leaderboard.iloc[0]['model']
    best_search = fitted_models[best_name]
    all_prob = best_search.predict_proba(X_all)[:, 1] if hasattr(best_search, 'predict_proba') else best_search.predict(X_all)
    all_pred = (all_prob >= 0.5).astype(int)
    all_lbl = pd.Series(all_pred).map({1: f'{args.marker}+', 0: f'{args.marker}-'})
    all_pred_df = pd.DataFrame({'source_file': df['source_file'], 'predicted_label': all_lbl})
    all_pred_df.to_csv(out / f'{args.marker}_all_rows_predictions.tsv', sep='\t', index=False)

    pred_counts = pd.crosstab(all_pred_df['source_file'], all_pred_df['predicted_label'])
    ax = pred_counts.plot(kind='bar', stacked=True, figsize=(10, 5), colormap='tab20')
    ax.set_title(f'Supervised predicted {args.marker} label abundance by input file')
    plt.xticks(rotation=25, ha='right')
    plt.tight_layout()
    plt.savefig(out / f'{args.marker}_all_rows_abundance.png', dpi=120)
    plt.close()

    per_file = out / 'per_file_reports'
    per_file.mkdir(parents=True, exist_ok=True)
    report_df = df.copy()
    report_df['predicted_label'] = all_lbl.values
    if {'Centroid X µm', 'Centroid Y µm'}.issubset(report_df.columns):
        for src_file, grp in report_df.groupby('source_file'):
            fig, ax = plt.subplots(figsize=(6, 5))
            grp = grp.dropna(subset=['Centroid X µm', 'Centroid Y µm']).copy()
            if grp.empty:
                continue
            grp['Inverted Centroid Y µm'] = grp['Centroid Y µm'].max() - grp['Centroid Y µm']
            sns.scatterplot(data=grp, x='Centroid X µm', y='Inverted Centroid Y µm', hue='predicted_label', s=6, alpha=0.7, ax=ax)
            plt.tight_layout()
            fig.savefig(per_file / f'{Path(src_file).stem}_spatial_predictions.png', dpi=120)
            plt.close(fig)

    summary = {
        'marker': args.marker,
        'input_files': [str(Path(t).name) for t in args.tables],
        'n_rows_total': int(len(df)),
        'n_rows_parseable_marker': int(len(work_df)),
        'n_rows_inlier_for_modeling': int((~is_outlier).sum()),
        'rows_per_file': df['source_file'].value_counts().to_dict(),
        'selected_model': best_name,
        'timestamp_utc': datetime.utcnow().isoformat() + 'Z',
    }
    (out / f'{args.marker}_modeling_summary.json').write_text(json.dumps(summary, indent=2))
    joblib.dump(best_search.best_estimator_, out / f'{args.marker}_best_model_{best_name}.joblib')
    with open(out / f'{args.marker}_feature_columns.txt', 'w', encoding='utf-8') as f:
        f.write('\n'.join(X.columns.tolist()))


if __name__ == '__main__':
    main()
