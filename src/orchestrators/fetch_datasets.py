import os
import pandas as pd
import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from sources import unhcr, worldbank
from utils import UNHCR_DATA_PATH, WB_DATA_PATH
from schemas.column_mappings import apply_prefix_mapping, enforce_schema, get_schema_for_source

MAX_WORKERS = 20

def process_meta(input_file, output_file, fetch_function, source_name):
    """
    Process metadata by fetching detailed data for each ID in parallel.
    Only fetches NEW IDs not present in existing datasets.csv.

    Parameters:
    - input_file (str): Path to the metadata CSV file.
    - output_file (str): Path to the output datasets CSV file.
    - fetch_function (callable): Function to fetch data for a single ID.
    - source_name (str): 'worldbank' or 'unhcr'

    Returns:
    - pd.DataFrame: Normalized DataFrame with all fetched data.
    """
    df_meta = pd.read_csv(input_file)

    existing_df = None
    existing_ids = set()
    if os.path.exists(output_file):
        existing_df = pd.read_csv(output_file)
        existing_ids = set(existing_df['id'])

    new_ids = [id for id in df_meta["id"] if id not in existing_ids]

    if not new_ids:
        print(f"No new datasets to fetch")
        return existing_df if existing_df is not None else pd.DataFrame()

    print(f"Fetching {len(new_ids)} new datasets out of {len(df_meta)} total")

    records = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_function, id): id for id in new_ids}

        for future in tqdm.tqdm(as_completed(futures), total=len(futures), disable=True):
            try:
                data = future.result()
                # Attach the ID from metadata to ensure downstream processing has a key.
                if isinstance(data, dict) and "id" not in data:
                    data["id"] = futures[future]
                records.append(data)
            except Exception as e:
                print(f"An error occurred for ID {futures[future]}: {e}")

    new_df = pd.json_normalize(records)

    schema = get_schema_for_source(source_name)
    new_df = apply_prefix_mapping(new_df)
    new_df = enforce_schema(new_df, schema)

    if existing_df is not None:
        existing_df = enforce_schema(existing_df, schema)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df

    if "id" in combined_df.columns:
        combined_df = combined_df.sort_values('id').reset_index(drop=True)

    return combined_df

def process_datasets(df, output_file):
    """
    Saves the processed dataset to CSV.

    Args:
    - df (DataFrame): Metadata DataFrame (already schema-enforced).
    - output_file (str): Path to save the processed CSV file.

    Returns:
    None
    """
    if df is None or df.empty:
        print(f"No datasets to save for {output_file}")
        return

    if "id" not in df.columns:
        print(f"Dataset DataFrame missing 'id' column; skipping save for {output_file}")
        return

    try:
        df = df.sort_values('id')
        df.to_csv(output_file, index=False)
        print(f"Dataset with shape {df.shape} saved to {output_file}")

    except Exception as e:
        print(f"Error saving dataframe: {e}")

def run():
    """Orchestrate fetching detailed datasets from all sources."""
    try:
        print(f"Fetching datasets from the World Bank MDL")
        input_file = WB_DATA_PATH + "metadata.csv"
        output_file = WB_DATA_PATH + "datasets.csv"
        rawdf = process_meta(input_file, output_file, worldbank.fetch_dataset, 'worldbank')
        process_datasets(rawdf, output_file)
    except Exception as e:
        print(f"An error occurred with World Bank: {e}")

    try:
        print(f"Fetching datasets from the UNHCR MDL")
        input_file = UNHCR_DATA_PATH + "metadata.csv"
        output_file = UNHCR_DATA_PATH + "datasets.csv"
        rawdf = process_meta(input_file, output_file, unhcr.fetch_dataset, 'unhcr')
        process_datasets(rawdf, output_file)
    except Exception as e:
        print(f"An error occurred with UNHCR: {e}")
