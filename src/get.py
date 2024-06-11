from utils import *
import pandas as pd
import requests
import json
import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

WB_URL = "https://microdata.worldbank.org/index.php/metadata/export/"
WB_INPUT_FILE = WB_DATA_PATH + "metadata.csv"
WB_OUTPUT_FILE = WB_DATA_PATH + "datasets.csv"

UNHCR_URL = "https://microdata.unhcr.org/index.php/metadata/export/{}/json"
UNHCR_INPUT_FILE = UNHCR_DATA_PATH + "metadata.csv"
UNHCR_OUTPUT_FILE = UNHCR_DATA_PATH + "datasets.csv"

def process_datasets(input_file, output_file):
    """
    Processes the dataset by normalizing nested JSON fields, renaming columns, 
    and removing unnecessary columns.
    
    Args:
    - input_file (str): Path to the input CSV file.
    - output_file (str): Path to save the processed CSV file.
    
    Returns:
    None
    """
    try:
        df = pd.read_csv(input_file)
        
        # Set patterns to be removed from column names
        patterns_to_remove = ["study_desc.", "doc_desc.", "study_info.", "method."]
        df.columns = df.columns.str.replace("|".join(patterns_to_remove), "", regex=True)
        df.columns = df.columns.str.replace("data_collection.", "method_")

        # Normalize nested JSON columns
        for col in find_list_columns(df):
            data = df[col].apply(merge_dicts)
            data_normalized = pd.json_normalize(data)
            # Add prefix of column name to the new data
            data_normalized.columns = [f"{col}_{c}" for c in data_normalized.columns]
            df = pd.concat([df, data_normalized], axis=1)
            df.drop(col, axis=1, inplace=True)

        # Drop columns with all NaN values and the 'schematype' column
        df.dropna(axis=1, how='all', inplace=True)
        if 'schematype' in df.columns:
            df.drop('schematype', axis=1, inplace=True)

        # Save the processed dataset
        df.to_csv(output_file, index=False)
        print(f"Flattened dataset with shape {df.shape} saved to {output_file}")

    except Exception as e:
        print(f"Error processing file {input_file}: {e}")

MAX_WORKERS = 20

def fetch_data(url, id):
    """Fetch data from the given API for a specific ID."""
    response = requests.get(url.format(id))
    response.raise_for_status()  # Raises an error for bad responses
    return response.json()

def fetch_wb_data(id):
    return fetch_data(WB_URL + "{}", id)

def fetch_unhcr_data(id):
    data = fetch_data(UNHCR_URL, id)
    data["id"] = id
    return data

def process_meta(input_file, fetch_function):
    # Read input CSV file
    df = pd.read_csv(input_file)
    records = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_function, id): id for id in df["id"]}

        for future in tqdm.tqdm(as_completed(futures), total=len(futures)):
            try:
                data = future.result()
                records.append(data)
            except Exception as e:
                print(f"An error occurred for ID {futures[future]}: {e}")

    normalized_df = pd.json_normalize(records)
    return normalized_df

def process_datasets(df, output_file):
    """
    Processes the dataset by normalizing nested JSON fields, renaming columns, 
    and removing unnecessary columns.
    
    Args:
    - df (DataFrame): Metadata DataFrame.
    - output_file (str): Path to save the processed CSV file.
    
    Returns:
    None
    """
    try:

        # Define patterns to be removed from column names
        patterns_to_remove = ["study_desc.", "doc_desc.", "study_info.", "method."]
        df.columns = df.columns.str.replace("|".join(patterns_to_remove), "", regex=True)
        df.columns = df.columns.str.replace("data_collection.", "method_")

        # Normalize nested JSON columns
        for col in find_list_columns(df):
            data = df[col].apply(merge_dicts)
            data_normalized = pd.json_normalize(data)
            # Add prefix of column name to the new data
            data_normalized.columns = [f"{col}_{c}" for c in data_normalized.columns]
            df = pd.concat([df, data_normalized], axis=1)
            df.drop(col, axis=1, inplace=True)

        # Drop columns with all NaN values and the 'schematype' column
        df.dropna(axis=1, how='all', inplace=True)
        if 'schematype' in df.columns:
            df.drop('schematype', axis=1, inplace=True)

        # Save the processed dataset
        df.to_csv(output_file, index=False)
        print(f"Flattened dataset with shape {df.shape} saved to {output_file}")

    except Exception as e:
        print(f"Error processing dataframe: {e}")


def main():
    try:
        print(f"Fetching datasets from the World Bank MDL")
        rawdf = process_meta(WB_INPUT_FILE, fetch_wb_data)
        process_datasets(rawdf, WB_OUTPUT_FILE)        
    except Exception as e:
        print(f"An error occurred: {e}")
    try:
        print(f"Fetching datasets from the UNHCR MDL")
        rawdf = process_meta(UNHCR_INPUT_FILE, fetch_unhcr_data)
        process_datasets(rawdf, UNHCR_OUTPUT_FILE)
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    print("----")
    main()
    print("----")
