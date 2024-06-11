# UNHCR + World Bank microdata library scraper

Python functions to scrape the microdata libraries (MDL) from the [UNHCR](https://microdata.unhcr.org/) and the [World Bank](https://microdata.worldbank.org).

The repository uses Github Action to run weekly. Approximate runtime: <5min

Created by the [Joint Data Center on Forced Displacement](https://www.jointdatacenter.org/).

# Data folder

Each microdata library has its own subfolder. The records are saved in the following format:

- `metadata.csv`: list of all datasets available in the MDL and their identification
- `datasets.csv`: all information about the datasets available in the MDL (some columns contain dictionaries)
- `datasets_flat.csv`: same as `datasets.csv` but with dictionaries expanded into multiple columns

# Code

`list.py`: List all datasets in the MDLs and creates `metadata.csv`

`get.py`: Loop through all rows in `metadata.csv` to get data from the datasets and produce the file `datasets.csv`

`clean.py`: Unnest columns with dictionaries and clean the dataset to generate `datasets_flat.csv`

