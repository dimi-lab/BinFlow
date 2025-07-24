#!/usr/bin/env python3
import os, sys
import pandas as pd
import pickle
from sklearn.pipeline import Pipeline

# Load the best model
def load_model(model_path):
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    return model

# Prepare the data for prediction
def prepare_data(df, model):
    if isinstance(model, Pipeline):
        preprocessor = model.named_steps['preprocessor']
        feature_columns = sum([list(t[2]) for t in preprocessor.transformers_ if len(t) > 2], [])
    else:
        raise ValueError("Model is not a Pipeline with a preprocessor.")
    print("Expected columns:", feature_columns)
    missing = [col for col in feature_columns if col not in df.columns]
    print("Input columns:", list(df.columns))
    print("Missing columns:", missing)
    if len(missing) > 1:
        print("Too many missing columns, returning null DataFrame for prediction.")
        return None
    return df[feature_columns]

# Make predictions
def make_predictions(model, data):
    predictions = model.predict(data)
    return predictions

# Save the output with only 'Centroid' columns and predictions
def save_predictions(df, predictions, output_path):
    centroid_and_image_columns = [col for col in df.columns if "centroid" in col.lower() or "image" in col.lower()]
    if not centroid_and_image_columns:
        raise ValueError(f"No centroid or image columns found in input DataFrame. Columns present: {list(df.columns)}")
    df_output = df[centroid_and_image_columns].copy()
    if predictions is None:
        df_output['Predictions'] = ["NA"] * len(df_output)
    else:
        df_output['Predictions'] = predictions
    df_output.to_csv(output_path, sep='\t', index=False)
    print(f"Predictions saved to {output_path}")

# Main workflow
def main(model_path, input_data_path, output_path):
    model = load_model(model_path)
    print(f"Loaded model from {model_path}")
    df = pd.read_csv(input_data_path, sep='\t')
    print(f"Loaded data from {input_data_path}")
    data_for_prediction = prepare_data(df, model)
    if data_for_prediction is None:
        save_predictions(df, None, output_path)
    else:
        predictions = make_predictions(model, data_for_prediction)
        save_predictions(df, predictions, output_path)

def extract_marker(filename):
    parts = filename.split("_")
    if len(parts) >= 4 and parts[1] == "best":
        return parts[3].split("\\.")[0]  # Assuming marker is between "best_model" and extension
    else:
        return "NA"
    
# Run script
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: best_model_predictions.py <best_model.pkl> <original_df.tsv>")
        sys.exit(1)
    model_path = sys.argv[1]
    input_data_path = sys.argv[2]

    preFh = os.path.basename(input_data_path)
    lblName = extract_marker(model_path)
    print(f"On Marker: {lblName}")
    output_path = f"{preFh}_predictions_{lblName}_PRED.tsv"

    main(model_path, input_data_path, output_path)

