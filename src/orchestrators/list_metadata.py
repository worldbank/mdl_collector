import os
import logging
from sources import unhcr, worldbank
from utils import UNHCR_DATA_PATH, WB_DATA_PATH

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def save_to_csv(df, output_file):
    """
    Save a DataFrame to a CSV file.

    Parameters:
    - df (pd.DataFrame): The DataFrame to save.
    - output_file (str): The file path to save the CSV to.
    """
    try:
        df = df.sort_values('id')
        df.to_csv(output_file, index=False)
        logging.info(f"Data successfully saved to {output_file}")
    except IOError as e:
        logging.error(f"Failed to save CSV: {e}")
        raise

def run():
    """Orchestrate fetching metadata lists from all sources."""
    os.makedirs(UNHCR_DATA_PATH, exist_ok=True)
    os.makedirs(WB_DATA_PATH, exist_ok=True)

    try:
        print(f"Fetching metadata from the UNHCR MDL")
        df_unhcr = unhcr.fetch_metadata_list()
        output_file = UNHCR_DATA_PATH + "metadata.csv"
        save_to_csv(df_unhcr, output_file)
        logging.info(f"UNHCR Metadata {df_unhcr.shape} saved to {output_file}")
    except Exception as e:
        logging.error(f"An error occurred with UNHCR: {e}")

    try:
        print(f"Fetching metadata from the World Bank MDL")
        df_wb = worldbank.fetch_metadata_list()
        output_file = WB_DATA_PATH + "metadata.csv"
        save_to_csv(df_wb, output_file)
        logging.info(f"World Bank Metadata {df_wb.shape} saved to {output_file}")
    except Exception as e:
        logging.error(f"An error occurred with World Bank: {e}")
