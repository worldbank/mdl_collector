# UNHCR + World Bank microdata library scraper

Python functions to scrape the microdata libraries (MDL) from the [UNHCR](https://microdata.unhcr.org/) and the [World Bank](https://microdata.worldbank.org).

The repository uses Github Action to run weekly. 

Created by the [Joint Data Center on Forced Displacement](https://www.jointdatacenter.org/).

# Quick access

Right-click and "Save link as" to download the files.

- [UNHCR microdata library](data/unhcr/datasets.csv)
  
- [WorldBank microdata library](data/world_bank/datasets.csv)

# Data 

Each microdata library has its own subfolder. The records are saved in the following format:

- `metadata.csv`: list of all datasets available

- `datasets.csv`: all information about the datasets in both microdata libraries

# Code

`list.py`: Lists all datasets in the MDLs and creates a CSV file with the metadata

`get.py`: Loop through all rows in `metadata.csv` to get data from the datasets and generate the `datasets.csv` file.

