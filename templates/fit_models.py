#!/usr/bin/env python3

import pandas as pd
import os, sys
import re
import pickle
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier, ExtraTreesClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix
from sklearn.impute import SimpleImputer
from fpdf import FPDF

def preprocess_data(df, label_column='key_label'):
    # Strip whitespace from column names
    df.columns = df.columns.str.strip()

    # Check for alternate column names
    if 'key_label' not in df.columns:
        raise KeyError(f"'key_label' column not found. Available columns: {list(df.columns)}")


    # Preprocesses the DataFrame for machine learning.
    df[label_column] = df[label_column].fillna("Unknown")
    df[label_column] = df[label_column].astype(str).str.strip()
    X = df.drop(columns=[label_column])
    y = df[label_column]
    return X, y

def build_pipelines():
    numeric_features = ['Centroid X µm', 'Centroid Y µm']
    categorical_features = ['Image', 'ROI']
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
            ('num', numeric_transformer, numeric_features),
            ('cat', categorical_transformer, categorical_features)
        ]
    )
    return preprocessor

def build_models(preprocessor):
    models = {
        'RandomForest': Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('classifier', RandomForestClassifier(random_state=42))
        ]),
        'ExtraTrees': Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('classifier', ExtraTreesClassifier(random_state=42))
        ]),
        'LogisticRegression': Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('classifier', LogisticRegression(max_iter=500, random_state=42))
        ])
    }
    return models

def evaluate_models(models, X_train, X_test, y_train, y_test, nom):
    results = {}
    best_model_name = None
    best_accuracy = 0.0

    for name, model in models.items():
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        results[name] = {
            'model': model,
            'accuracy': accuracy,
            'classification_report': classification_report(y_test, y_pred, zero_division=0, output_dict=True),
            'confusion_matrix': confusion_matrix(y_test, y_pred)
        }
        if accuracy > best_accuracy:
            best_accuracy = accuracy
            best_model_name = name

    if best_model_name:
        with open(f"{best_model_name}_best_model_{nom}.pkl", 'wb') as f:
            pickle.dump(results[best_model_name]['model'], f)

    return results

def extract_marker(filename):
    parts = filename.split("_")
    if len(parts) > 1 and parts[0] == "training":
        return parts[1].split(".")[0]  # Assuming extension is separated by "."
    else:
        return "NA"

if __name__ == "__main__":
    df = pd.read_csv("${training_df}", sep="\t")
    lblName = extract_marker("${training_df}")
    X, y = preprocess_data(df)
    if len(y.unique()) < 2:
        print(f"Error: Target variable 'y' must contain at least two unique values. {y.unique()} Exiting.")
        sys.exit(0)
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    preprocessor = build_pipelines()
    models = build_models(preprocessor)
    results = evaluate_models(models, X_train, X_test, y_train, y_test, lblName)

