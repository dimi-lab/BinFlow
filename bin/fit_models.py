#!/usr/bin/env python3

import pandas as pd
import numpy as np
import os, sys
import re
import pickle
from dask.distributed import Client
from joblib import parallel_backend
from sklearn.model_selection import train_test_split, RandomizedSearchCV, StratifiedShuffleSplit
from sklearn.ensemble import RandomForestClassifier, ExtraTreesClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix, f1_score
from sklearn.impute import SimpleImputer
from pprint import pprint
from dask.distributed import Client
from joblib import parallel_backend

def preprocess_data(df, label_column='key_label'):
    # Strip whitespace from column names
    df.columns = df.columns.str.strip()
    # Check for alternate column names
    if 'key_label' not in df.columns:
        raise KeyError(f"'key_label' column not found. Available columns: {list(df.columns)}")
    # Preprocesses the DataFrame for machine learning.
    df[label_column] = df[label_column].fillna("Unknown")
    df[label_column] = df[label_column].astype(str).str.strip()
    df = df[df.columns.drop(list(df.filter(regex='(Centroid|Binary|Classification|Name|Image|ROI)')))].fillna(0)
    X = df.drop(columns=[label_column])
    y = df[label_column]
    return X, y

def build_pipelines(numeric_features):
    numeric_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='mean')),
        ('scaler', StandardScaler())
    ])
    categorical_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='constant', fill_value='missing')),
        ('onehot', OneHotEncoder(handle_unknown='ignore'))
    ])
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numeric_transformer, numeric_features)
        ]
    )
    return preprocessor

def build_models(preprocessor, n_iter=100, cv=5):
    models = {}
    rf_pipeline = Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('classifier', RandomForestClassifier(random_state=421))
    ])
    rf_param_grid = {
        'classifier__criterion': ['gini','entropy','log_loss'],
        'classifier__n_estimators': [100, 200],
        'classifier__max_depth': [None] + list(range(5,30)),
        'classifier__min_samples_split': list(range(2,20)),
        'classifier__min_samples_leaf': list(range(1,10)),
        'classifier__max_features': ['sqrt','log2']
    }
    models['RandomForest'] = RandomizedSearchCV(
        estimator=rf_pipeline,
        param_distributions=rf_param_grid,
        n_iter=n_iter,
        cv=cv,
        scoring='f1',
        random_state=422,
        n_jobs=-1
    )
    et_pipeline = Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('classifier', ExtraTreesClassifier(random_state=421))
    ])
    et_param_grid = {
        'classifier__criterion': ['gini','entropy','log_loss'],
        'classifier__n_estimators': [100, 200],
        'classifier__max_depth': [None] + list(range(5,30)),
        'classifier__min_samples_split': list(range(2,20)),
        'classifier__min_samples_leaf': list(range(1,10)),
        'classifier__max_features': ['sqrt','log2']
    }
    models['ExtraTrees'] = RandomizedSearchCV(
        estimator=et_pipeline,
        param_distributions=et_param_grid,
        n_iter=n_iter,
        cv=cv,
        scoring='f1',
        random_state=422,
        n_jobs=-1
    )
    lr_pipeline = Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('classifier', LogisticRegression(max_iter=5000, random_state=421))
    ])
    # Specific parameters for LogisticRegression
    x = np.linspace(2,20,37)
    tolerances = 10 ** -x
    x = np.linspace(-10,10,21)
    Cs = 10 ** x
    lr_param_grid = {
        'classifier__C': Cs,
        'classifier__penalty': ['l1', 'l2','elasticnet'],
        'classifier__solver': ['liblinear', 'sag', 'saga', 'newton-cg', 'lbfgs'],
        'classifier__tol': tolerances,
        'classifier__l1_ratio': np.linspace(0,1,24).tolist()
    }
    models['LogisticRegression'] = RandomizedSearchCV(
        estimator=lr_pipeline,
        param_distributions=lr_param_grid,
        n_iter=n_iter,
        cv=cv,
        scoring='f1',
        random_state=422,
        n_jobs=-1,
        error_score=np.nan,
        verbose=0
    )
    # pprint(models)
    return models

def evaluate_models(models, X_train, X_test, y_train, y_test, nom):
    results = {}
    best_model_name = None
    best_f1 = 0.0
    # pos_lbl = nom + '+'
    y_train_bin = np.array([1 if str(label).endswith('+') else 0 for label in y_train])
    y_test_bin = np.array([1 if str(label).endswith('+') else 0 for label in y_test])
    pprint(models)

    best_model_obj = None
    for name, model in models.items():
        fit_success = True
        try:
            model.fit(X_train, y_train_bin)
        except Exception as e:
            print(f'Error during model fitting for {name}: {e}')
            fit_success = False
        if fit_success:
            try:
                y_pred = model.predict(X_test)
                accuracy = accuracy_score(y_test_bin, y_pred)
                f1 = f1_score(y_test_bin, y_pred, average='binary')
                results[name] = {
                    'model': model,
                    'best_params': model.best_params_,
                    'accuracy': accuracy,
                    'f1-score': f1,
                    'classification_report': classification_report(y_test_bin, y_pred, zero_division=0, output_dict=True),
                    'confusion_matrix': confusion_matrix(y_test_bin, y_pred)
                }
                if f1 > best_f1:
                    best_f1 = f1
                    best_model_name = name
                    best_model_obj = model.best_estimator_
            except Exception as e:
                print(f'Error during prediction for {name}: {e}')
                results[name] = {'model': model, 'error': str(e)}
        else:
            results[name] = {'model': model, 'error': 'Model fitting failed'}

    if best_model_obj is not None:
        with open(f"{best_model_name}_best_model_{nom}.pkl", 'wb') as f:
            pickle.dump(best_model_obj, f)

    return results

def extract_marker(filename):
    parts = filename.split("_")
    if len(parts) > 1 and parts[0] == "training":
        return parts[1].split(".")[0]  # Assuming extension is separated by "."
    else:
        return "NA"

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: fit_models.py <training_df>")
        sys.exit(1)
    training_df = sys.argv[1]
    df = pd.read_csv(training_df, sep="\t")
    lblName = extract_marker(os.path.basename(training_df))
    print(f"Label = {lblName}")
    X, y = preprocess_data(df)
    if len(y.unique()) < 2:
        print(f"Error: Target variable 'y' must contain at least two unique values. {y.unique()} Exiting.")
        sys.exit(0)
    if X.shape[1] == 0:
        print("Error: No features available for training after preprocessing. Exiting.")
        sys.exit(1)
    print(f"X Shape = {X.shape}")

    # Check and create output directory if needed
    out_dir = os.path.dirname(os.path.abspath(training_df))
    if not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42, stratify=y) # Added Stratification for split
    sss_cv = StratifiedShuffleSplit(n_splits = 5, random_state = 911)
    print(f"X_train Shape = {X_train.shape}")
    numeric_cols = X_train.columns.tolist()
    print(f"Features for Pipeline = {', '.join(numeric_cols)}")
    preprocessor = build_pipelines(numeric_cols)
    models = build_models(preprocessor, cv = sss_cv)

    ### This code is only for the SLURM??
    # Start Dask client and use Dask as parallel backend
    #client = Client()
    #print(f"Dask client started: {client}")
    #with parallel_backend('dask'):
    results = evaluate_models(models, X_train, X_test, y_train, y_test, lblName)

    pprint(results)


