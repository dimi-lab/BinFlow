import os
import pandas as pd

def get_user_input(prompt, default_value=""):
    """
    Gets user input from the console with a default value.

    Args:
        prompt: The prompt to display to the user.
        default_value: The default value to use if no input is provided.

    Returns:
        The user's input or the default value.
    """
    user_input = input(f"{prompt} [{default_value}]: ")
    return user_input or default_value

def main():
    """
    Main function to execute the script.
    """
    try:
        # Check for existing 'binflow_input_data.tsv'
        if os.path.exists('binflow_input_data.tsv'):
            append_data = input("Found 'binflow_input_data.tsv'. Append data? (y/n): ").lower() == 'y'
            if append_data:
                df = pd.read_csv('binflow_input_data.tsv', sep='\t')
            else:
                df = pd.DataFrame(columns=['key', 'ome_path', 'segmask_path', 'quant_path'])
        else:
            df = pd.DataFrame(columns=['key', 'ome_path', 'segmask_path', 'quant_path'])

        # Get OMETIFF directory path
        ometiff_dir = get_user_input("Enter OMETIFF directory path: ")
        sample_name_in_dir = input("Does the OMETIFF directory contain sample names? (y/n): ").lower() == 'y'

        # Get OMETIFF file paths
        ometiff_files = [os.path.join(root, file) 
                        for root, _, files in os.walk(ometiff_dir) 
                        for file in files 
                        if file.lower().endswith(('.ome.tiff', '.ome.tif'))]

        # Generate 'key' column based on sample name presence
        df['ome_path'] = ometiff_files
        df['key'] = df['ome_path'].apply(lambda x: os.path.basename(x) if not sample_name_in_dir 
                                                else os.path.join(os.path.basename(os.path.dirname(x)), os.path.basename(x)))

        # Get SEGMASK directory path
        segmask_dir = get_user_input("Enter SEGMASK directory path: ")

        # Get QUANT directory path
        quant_dir = get_user_input("Enter QUANT directory path: ")

        # Match and populate 'segmask_path' and 'quant_path'
        for index, row in df.iterrows():
            ome_basename = os.path.splitext(os.path.basename(row['ome_path']))[0]
            for root, _, files in os.walk(segmask_dir):
                for segmask_file in files:
                    if ome_basename in segmask_file:
                        df.loc[index, 'segmask_path'] = os.path.join(root, segmask_file)
                        break
            for root, _, files in os.walk(quant_dir):
                for quant_file in files:
                    if ome_basename in quant_file:
                        df.loc[index, 'quant_path'] = os.path.join(root, quant_file)
                        break

        print(f"Number of rows in the dataframe: {len(df)}")

        # Write dataframe to 'binflow_input_data.tsv'
        df.to_csv('binflow_input_data.tsv', sep='\t', index=False)
        print("Data written to 'binflow_input_data.tsv'")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
