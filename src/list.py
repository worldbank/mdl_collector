import json, os
import requests
import logging
from utils import *

UNHCR_OUTPUT_FILE = UNHCR_DATA_PATH + "metadata.csv"
WB_OUTPUT_FILE = WB_DATA_PATH + "metadata.csv"

UNHCR_URL = "https://microdata.unhcr.org/index.php/api/catalog/search?ps=9999999&sort_by=created&sort_order=desc"
WB_URL = "https://microdata.worldbank.org/index.php/api/catalog/list_idno/survey"

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_metadata(url, data_keys):
    """
    Fetch metadata from a given API URL.

    Parameters:
    - url (str): The API endpoint.
    - data_key (str): The key to access the required data in the JSON response.

    Returns:
    - pd.DataFrame: The metadata as a pandas DataFrame.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        for key in data_keys:
            data = data[key]
        return pd.DataFrame(data)
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        raise
    except (ValueError, KeyError) as e:
        logging.error(f"Failed to process JSON response: {e}")
        raise

def save_to_csv(df, output_file):
    """
    Save a DataFrame to a CSV file.

    Parameters:
    - df (pd.DataFrame): The DataFrame to save.
    - output_file (str): The file path to save the CSV to.
    """
    try:
        df.to_csv(output_file, index=False)
        logging.info(f"Data successfully saved to {output_file}")
    except IOError as e:
        logging.error(f"Failed to save CSV: {e}")
        raise

def main():
    os.makedirs(UNHCR_DATA_PATH, exist_ok=True)
    os.makedirs(WB_DATA_PATH, exist_ok=True)
    try:
        # Fetch and save UNHCR metadata
        print(f"Fetching metadata from the UNHCR MDL")
        df_unhcr = fetch_metadata(UNHCR_URL, ["result", "rows"])
        save_to_csv(df_unhcr, UNHCR_OUTPUT_FILE)
        logging.info(f"UNHCR Metadata {df_unhcr.shape} saved to {UNHCR_OUTPUT_FILE}")
    
    except Exception as e:
        logging.error(f"An error occurred: {e}")

    try:
        # Fetch and save World Bank metadata
        print(f"Fetching metadata from the World Bank MDL")
        df_wb = fetch_metadata(WB_URL, ["records"])
        save_to_csv(df_wb, WB_OUTPUT_FILE)
        logging.info(f"World Bank Metadata {df_wb.shape} saved to {WB_OUTPUT_FILE}")

    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()