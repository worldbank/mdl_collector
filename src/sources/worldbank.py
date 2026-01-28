import requests
import pandas as pd
import logging
from utils import WB_DATA_PATH

METADATA_LIST_URL = "https://microdata.worldbank.org/index.php/api/catalog/list_idno/survey"
DATASET_EXPORT_URL = "https://microdata.worldbank.org/index.php/metadata/export/{}"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_metadata_list():
    """
    Fetch metadata list from World Bank API.

    Returns:
    - pd.DataFrame: The metadata as a pandas DataFrame.
    """
    try:
        response = requests.get(METADATA_LIST_URL)
        response.raise_for_status()
        data = response.json()
        data = data["records"]
        return pd.DataFrame(data)
    except requests.RequestException as e:
        logging.error(f"World Bank Request failed: {e}")
        raise
    except (ValueError, KeyError) as e:
        logging.error(f"World Bank Failed to process JSON response: {e}")
        raise

def fetch_dataset(id):
    """
    Fetch detailed dataset data from World Bank API for a specific ID.

    Parameters:
    - id: Dataset ID

    Returns:
    - dict: Dataset information
    """
    response = requests.get(DATASET_EXPORT_URL.format(id))
    response.raise_for_status()
    return response.json()
