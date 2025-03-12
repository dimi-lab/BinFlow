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
        # Extract the preprocessor from the pipeline
        preprocessor = model.named_steps['preprocessor']
        feature_columns = preprocessor.transformers_[0][2] + preprocessor.transformers_[1][2]
    else:
        raise ValueError("Model is not a Pipeline with a preprocessor.")
    
    # Select only the required columns
    df = df[feature_columns]
    return df

# Make predictions
def make_predictions(model, data):
    predictions = model.predict(data)
    return predictions

# Save the output with only 'Centroid' columns and predictions
def save_predictions(df, predictions, output_path):
    # Select columns containing "Centroid"
    centroid_and_image_columns = [col for col in df.columns if "centroid" in col.lower() or "image" in col.lower()]
    df_output = df[centroid_and_image_columns].copy()
    
    # Add predictions as a new column
    df_output['Predictions'] = predictions
    
    # Save the output
    df_output.to_csv(output_path, sep='\t', index=False)
    print(f"Predictions saved to {output_path}")

# Main workflow
def main(model_path, input_data_path, output_path):
    # Load the best model
    model = load_model(model_path)
    print(f"Loaded model from {model_path}")
    
    # Load the new dataset
    df = pd.read_csv(input_data_path, sep='\t')
    print(f"Loaded data from {input_data_path}")
    
    # Prepare the data for prediction
    data_for_prediction = prepare_data(df, model)
    
    # Make predictions
    predictions = make_predictions(model, data_for_prediction)
    
    # Save predictions with Centroid columns
    save_predictions(df, predictions, output_path)

def extract_marker(filename):
    parts = filename.split("_")
    if len(parts) >= 4 and parts[1] == "best":
        return parts[3].split(".")[0]  # Assuming marker is between "best_model" and extension
    else:
        return "NA"
    
# Run script
if __name__ == "__main__":
    # File paths
    model_path = "${best_model}"  # Update with your best model file
    input_data_path = "${original_df}"  # Path to the new dataset
    preFh = os.path.basename(input_data_path)
    lblName = extract_marker("${best_model}")
    output_path = f"{preFh}_predictions_{lblName}_PRED.tsv"  # Path to save predictions
    

    
    main(model_path, input_data_path, output_path)

