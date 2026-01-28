# UNHCR & World Bank microdata library collector

This repository automates data collection and archives records from the [UNHCR](https://microdata.unhcr.org/) and the [World Bank](https://microdata.worldbank.org) microdata libraries. Both are built on the [NADA (National Data Archive)](https://github.com/ihsn/nada) system.

# Key Features

**Automated Collection** - GitHub Actions workflow runs periodically to mirror data from both UNHCR and World Bank microdata libraries in a unified workflow.

**Git-Based Versioning** - Every update is committed, providing full historical tracking of all metadata changes over time.

**Fixed Schema Management** - Prevents schema drift by enforcing predefined columns.

# Quick Start

## Get the Data

Basic information from both microdata libraries can be fully exported as CSV files using the following endpoints: [https://microdata.unhcr.org/index.php/catalog/export/csv](https://microdata.unhcr.org/index.php/catalog/export/csv) and [https://microdata.worldbank.org/index.php/catalog/export/csv](https://microdata.worldbank.org/index.php/catalog/export/csv)

The archived files, including all metadata, are available below. Right-click and "Save link as" to download the files.

- [UNHCR microdata library](data/unhcr/datasets.csv)
  
- [WorldBank microdata library](data/world_bank/datasets.csv)

Be aware that archived data might be outdated.

## Run the Code

Dependencies are managed with [uv](https://docs.astral.sh/uv/).

1. Install uv (for example, `curl -LsSf https://astral.sh/uv/install.sh | sh`).
2. Sync dependencies: `uv sync --locked`.
3. Run the scraper: `uv run python src/main.py`.

# Data 

Each microdata library has its own subfolder. The records are saved in the following format:

- `metadata.csv`: list of all datasets available

- `datasets.csv`: all information about the datasets in both microdata libraries

**Note on UNHCR metadata updates**: The UNHCR API returns live statistics (`total_views`, `total_downloads`) that change frequently. This means the `metadata.csv` file for UNHCR will show changes on most runs even when no new datasets are added, as view counts are updated.

## Schema Management

API responses contain nested JSON that gets flattened. To avoid column name collisions, we use these prefix conventions:

- `study_desc.*` → `study.*` (study-level metadata)
- `doc_desc.*` → `doc.*` (documentation-level metadata)
- `study_info.*` → `info.*` (study information fields)
- `method.*` → `method.*` (methodology fields)
- `data_collection.*` → `method.*` (data collection details)

To prevent schema drift and maintain data integrity, this project uses fixed schemas. Definitions are in `src/schemas/column_mappings.py`.

If the API introduces new fields that should be tracked:

1. Edit `src/schemas/column_mappings.py`
2. Add the field to the appropriate schema dict (`WORLD_BANK_SCHEMA` or `UNHCR_SCHEMA`)
3. Re-run the scraper - new field will be populated in existing rows with NaN

Fields not in the schema are automatically dropped during collection.

# Changelog

**January 2026**: code refactoring, added incremental updates and fixed schema.

**July 2024**: web scraping implementation.

# Credits

Implemented by the [World Bank-UNHCR Joint Data Center on Forced Displacement](https://www.jointdatacenter.org/).