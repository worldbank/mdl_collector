import pandas as pd
import ast
from typing import List

UNHCR_DATA_PATH = "data/unhcr/"
WB_DATA_PATH = "data/world_bank/"


def find_list_columns(df: pd.DataFrame) -> List[str]:
    """
    Identifies which columns in a DataFrame contain strings that represent lists.

    Parameters:
    df (pd.DataFrame): The DataFrame to check.

    Returns:
    List[str]: A list of column names where at least one entry is a string that can be evaluated as a list.
    """
    list_columns = []

    for column in df.columns:
            for item in df[column]:
                try:
                    if isinstance(item, str) and isinstance(ast.literal_eval(item), list) and item.startswith("[{"):
                        list_columns.append(column)
                        break  # Break after finding the first list in the column
                except:
                    # If literal_eval fails, the string is not a list
                    continue

    return list_columns

def merge_dicts(input,sep=';'):
    if (isinstance(input,float) and pd.isna(input)) | (str(input) == ",[]"):
      return ""

    input = ast.literal_eval(input)

    if isinstance(input,dict):
      return input

    merged_dict = {}
    #print(input)
    # Iterate through each dictionary in the list
    for d in input:
      #print(input)
      if isinstance(d,list) and input == [[]]:
        continue
      if isinstance(d,dict):
        # Iterate through each key-value pair in the dictionary
        for key, value in d.items():
            # If the key is not in the merged_dict, add it with the current value
            if key not in merged_dict:
                merged_dict[key] = value
            # If the key is already in the merged_dict, append the current value separated by a semicolon
            else:
                # Only add a semicolon if the previous value is not empty
                if merged_dict[key]:
                    merged_dict[key] += sep
                merged_dict[key] += value

        return merged_dict
