#!/usr/bin/env python3

import pandas as pd
import os, re
import pickle
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.ensemble import RandomForestClassifier, ExtraTreesClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix
from sklearn.impute import SimpleImputer
from fpdf import FPDF

def preprocess_data(df, label_column='key_label'):
    """
    Preprocesses the DataFrame for machine learning:
    - Ensures labels are treated as single categorical strings.
    - Handles missing or invalid labels appropriately.

    Parameters:
        df (pd.DataFrame): Input DataFrame.
        label_column (str): The name of the column containing the label.

    Returns:
        X (pd.DataFrame): Features.
        y (pd.Series): Processed labels.
    """
    # Handle missing labels by filling them with a placeholder (optional)
    df[label_column] = df[label_column].fillna("Unknown")

    # Ensure the labels are strings and strip whitespace
    df[label_column] = df[label_column].astype(str).str.strip()

    # Separate features and labels
    X = df.drop(columns=[label_column])
    y = df[label_column]

    return X, y



# Define function to build pipelines
def build_pipelines():
    numeric_features = ['Centroid X µm', 'Centroid Y µm']  # Adjust based on actual numeric columns
    categorical_features = ['Image', 'ROI']  # Adjust based on actual categorical columns

    # Preprocessing pipelines
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

# Define models to evaluate
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

# Evaluate models and save results
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix
import pickle

def evaluate_models(models, X_train, X_test, y_train, y_test, nom):
    results = {}
    best_model_name = None
    best_accuracy = 0.0

    for name, model in models.items():
        print(f"Training {name}...")
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        
        # Evaluate the model
        accuracy = accuracy_score(y_test, y_pred)
        report = classification_report(y_test, y_pred, zero_division=0, output_dict=True)
        confusion = confusion_matrix(y_test, y_pred)

        results[name] = {
            'model': model,
            'accuracy': accuracy,
            'classification_report': report,
            'confusion_matrix': confusion
        }
        
        # Update the best model if the current one is better
        if accuracy > best_accuracy:
            best_accuracy = accuracy
            best_model_name = name

    # Save the best model as a pickle file
    if best_model_name:
        best_model = results[best_model_name]['model']
        with open(f"{best_model_name}_best_model_{nom}.pkl", 'wb') as f:
            pickle.dump(best_model, f)
        print(f"Best model '{best_model_name}' saved to pickle file.")

    return results



# Generate PDF report
def generate_report(results):
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    pdf.set_font("Arial", size=16)
    pdf.cell(200, 10, txt="Model Comparison Report", ln=True, align='C')
    pdf.ln(10)

    best_model = None
    best_accuracy = 0
    for name, result in results.items():
        accuracy = result['accuracy']
        if accuracy > best_accuracy:
            best_accuracy = accuracy
            best_model = name

        pdf.set_font("Arial", size=12)
        pdf.cell(200, 10, txt=f"Model: {name}", ln=True)
        pdf.cell(200, 10, txt=f"Accuracy: {accuracy:.2f}", ln=True)
        pdf.ln(5)

        pdf.cell(200, 10, txt="Classification Report:", ln=True)
        for label, metrics in result['classification_report'].items():
            if isinstance(metrics, dict):
                pdf.cell(200, 10, txt=f"  Label: {label}", ln=True)
                for metric, value in metrics.items():
                    pdf.cell(200, 10, txt=f"    {metric}: {value:.2f}", ln=True)
        pdf.ln(10)

    # Highlight the best model
    pdf.set_font("Arial", size=14)
    pdf.cell(200, 10, txt=f"Best Performing Model: {best_model}", ln=True)
    pdf.cell(200, 10, txt=f"Best Accuracy: {best_accuracy:.2f}", ln=True)
    pdf.output("Model_Comparison_Report.pdf")
    print("PDF report generated: Model_Comparison_Report.pdf")


def extract_marker(filename):
  """Extracts the marker string between underscores from a filename.
  Args:
    filename: The input filename string, expected to be in the format "training_*.tsv".

  Returns:
    The extracted marker string, or None if not found or the filename doesn't match the expected format.
  """
  match = re.match(r"training_(\w+)\.tsv", filename)
  if match:
    return match.group(1)
  else:
    return "NA"

# Main workflow
if __name__ == "__main__":
    # Load data
    #df = pd.read_csv("${training_df}")
    df = pd.read_csv("training_CD68.tsv", sep="\t")
    lblName = extract_marker("training_CD68.tsv")

    # Preprocess data
    X, y = preprocess_data(df)

    # Split data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

    # Build pipelines and models
    preprocessor = build_pipelines()
    models = build_models(preprocessor)

    # Evaluate models
    results = evaluate_models(models, X_train, X_test, y_train, y_test, lblName)

    # Generate report
    generate_report(results)

