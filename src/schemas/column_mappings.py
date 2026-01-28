"""
Column mapping and schema enforcement for microdata collector.

This module provides:
1. Smart prefix mapping to prevent column name collisions
2. Fixed schema definitions for predictable column sets
3. Schema enforcement to align dataframes before merging
"""

import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


PREFIX_MAPPINGS = {
    'study_desc.': 'study.',
    'doc_desc.': 'doc.',
    'study_info.': 'info.',
    'method.': 'method.',
    'data_collection.': 'method.',
}


WORLD_BANK_SCHEMA = {
    'id': 'Int64',
    'title': 'str',
    'abstract': 'str',
    'tags': 'str',
    'study.title_statement.idno': 'str',
    'study.title_statement.title': 'str',
    'study.title_statement.sub_title': 'str',
    'study.title_statement.alternate_title': 'str',
    'study.title_statement.alt_title': 'str',
    'study.title_statement.translated_title': 'str',
    'study.title_statement.identifiers': 'str',
    'authoring_entity': 'str',
    'study.production_statement.producers': 'str',
    'study.production_statement.prod_date': 'str',
    'study.production_statement.prod_place': 'str',
    'study.production_statement.copyright': 'str',
    'study.production_statement.funding_agencies': 'str',
    'study.production_statement.grant_no': 'str',
    'study.distribution_statement.distributors': 'str',
    'study.distribution_statement.contact': 'str',
    'study.distribution_statement.depositor': 'str',
    'study.distribution_statement.deposit_date': 'str',
    'study.distribution_statement.distribution_date': 'str',
    'study.series_statement.series_name': 'str',
    'study.series_statement.series_info': 'str',
    'study.version_statement.version': 'str',
    'study.version_statement.version_date': 'str',
    'study.version_statement.version_resp': 'str',
    'study.version_statement.version_notes': 'str',
    'doc.version_statement.version': 'str',
    'doc.version_statement.version_date': 'str',
    'doc.version_statement.version_resp': 'str',
    'doc.version_statement.version_notes': 'str',
    'keywords': 'str',
    'topics': 'str',
    'info.notes': 'str',
    'coll_dates': 'str',
    'nation': 'str',
    'geog_coverage': 'str',
    'geog_coverage_notes': 'str',
    'geog_unit': 'str',
    'analysis_unit': 'str',
    'universe': 'str',
    'data_kind': 'str',
    'study_scope': 'str',
    'time_periods': 'str',
    'time_method': 'str',
    'data_collectors': 'str',
    'method.sampling_procedure': 'str',
    'method.sampling_deviation': 'str',
    'method.coll_mode': 'str',
    'method.research_instrument': 'str',
    'method.coll_situation': 'str',
    'method.act_min': 'str',
    'method.weight': 'str',
    'method.cleaning_operations': 'str',
    'method.notes': 'str',
    'method.data_processing': 'str',
    'method.coding_instructions': 'str',
    'method.instru_development': 'str',
    'method.collector_training': 'str',
    'method.collector_training.type': 'str',
    'method.collector_training.training': 'str',
    'method.control_operations': 'str',
    'analysis_info.response_rate': 'str',
    'analysis_info.sampling_error_estimates': 'str',
    'analysis_info.data_appraisal': 'str',
    'study_authorization.date': 'str',
    'study_authorization.agency': 'str',
    'study_authorization.authorization_statement': 'str',
    'sample_frame.name': 'str',
    'sample_frame.custodian': 'str',
    'sample_frame.universe': 'str',
    'sample_frame.frame_unit.unit_type': 'str',
    'sample_frame.frame_unit.is_primary': 'str',
    'data_access.dataset_availability.access_place': 'str',
    'data_access.dataset_availability.access_place_uri': 'str',
    'data_access.dataset_availability.access_place_url': 'str',
    'data_access.dataset_availability.original_archive': 'str',
    'data_access.dataset_availability.status': 'str',
    'data_access.dataset_availability.complete': 'str',
    'data_access.dataset_availability.file_quantity': 'str',
    'data_access.dataset_availability.notes': 'str',
    'data_access.dataset_use.contact': 'str',
    'data_access.dataset_use.cit_req': 'str',
    'data_access.dataset_use.conditions': 'str',
    'data_access.dataset_use.conf_dec': 'str',
    'data_access.dataset_use.disclaimer': 'str',
    'data_access.dataset_use.deposit_req': 'str',
    'data_access.dataset_use.restrictions': 'str',
    'data_access.dataset_use.spec_perm': 'str',
    'quality_statement.other_quality_statement': 'str',
    'quality_statement.compliance_description': 'str',
    'study_development': 'str',
    'study_notes': 'str',
    'sources': 'str',
    'sources.data_source': 'str',
    'holdings': 'str',
    'frequency': 'str',
    'bib_citation': 'str',
    'bib_citation_format': 'str',
    'additional.ticker_description': 'str',
    'additional.ticker_info': 'str',
    'idno': 'str',
    'oth_id': 'str',
    'producers': 'str',
    'prod_date': 'str',
    'production_statement': 'str',
    'distribution_statement': 'str',
    'series_statement': 'str',
}


UNHCR_SCHEMA = {
    'id': 'Int64',
    'title': 'str',
    'producers': 'str',
    'prod_date': 'str',
    'title_statement.idno': 'str',
    'title_statement.title': 'str',
    'title_statement.alt_title': 'str',
    'title_statement.sub_title': 'str',
    'title_statement.alternate_title': 'str',
    'title_statement.translated_title': 'str',
    'authoring_entity': 'str',
    'production_statement.producers': 'str',
    'production_statement.prod_date': 'str',
    'production_statement.copyright': 'str',
    'production_statement.funding_agencies': 'str',
    'production_statement.grant_no': 'str',
    'production_statement': 'str',
    'distribution_statement.contact': 'str',
    'distribution_statement.depositor': 'str',
    'distribution_statement.distributors': 'str',
    'distribution_statement': 'str',
    'series_statement.series_name': 'str',
    'series_statement.series_info': 'str',
    'series_statement': 'str',
    'version_statement.version': 'str',
    'version_statement.version_date': 'str',
    'version_statement.version_notes': 'str',
    'version_statement': 'str',
    'keywords': 'str',
    'topics': 'str',
    'abstract': 'str',
    'coll_dates': 'str',
    'nation': 'str',
    'geog_coverage': 'str',
    'geog_unit': 'str',
    'analysis_unit': 'str',
    'universe': 'str',
    'data_kind': 'str',
    'study_scope': 'str',
    'notes': 'str',
    'time_periods': 'str',
    'time_method': 'str',
    'data_collectors': 'str',
    'sampling_procedure': 'str',
    'sampling_deviation': 'str',
    'coll_mode': 'str',
    'research_instrument': 'str',
    'coll_situation': 'str',
    'weight': 'str',
    'cleaning_operations': 'str',
    'collector_training': 'str',
    'act_min': 'str',
    'analysis_info.response_rate': 'str',
    'analysis_info.sampling_error_estimates': 'str',
    'analysis_info.data_appraisal': 'str',
    'sample_frame.frame_unit': 'str',
    'data_access.dataset_availability': 'str',
    'data_access.dataset_availability.access_place': 'str',
    'data_access.dataset_availability.access_place_uri': 'str',
    'data_access.dataset_availability.original_archive': 'str',
    'data_access.dataset_use': 'str',
    'data_access.dataset_use.contact': 'str',
    'data_access.dataset_use.cit_req': 'str',
    'data_access.dataset_use.conditions': 'str',
    'data_access.dataset_use.conf_dec': 'str',
    'data_access.dataset_use.disclaimer': 'str',
    'ex_post_evaluation': 'str',
    'holdings': 'str',
    'idno': 'str',
    'oth_id': 'str',
}


def apply_prefix_mapping(df):
    """
    Apply smart prefix mapping to dataframe columns.

    Replaces long prefixes (study_desc., doc_desc., etc.) with shorter ones
    (study., doc., etc.) to maintain readability while preventing collisions.

    Args:
        df: DataFrame with columns from pd.json_normalize()

    Returns:
        DataFrame with renamed columns
    """
    new_columns = {}

    for col in df.columns:
        new_col = col
        for old_prefix, new_prefix in PREFIX_MAPPINGS.items():
            if new_col.startswith(old_prefix):
                new_col = new_col.replace(old_prefix, new_prefix, 1)
                break
        new_columns[col] = new_col

    df = df.rename(columns=new_columns)

    duplicates = set([col for col in df.columns if df.columns.tolist().count(col) > 1])
    if duplicates:
        logger.warning(f"Duplicate columns detected after prefix mapping: {duplicates}")

    return df


def enforce_schema(df, schema):
    """
    Enforce fixed schema on a dataframe.

    Aligns the dataframe to match the schema by:
    - Adding missing columns (filled with NaN)
    - Dropping extra columns not in schema
    - Reordering columns to match schema order

    Args:
        df: DataFrame to align
        schema: Dict mapping column names to types

    Returns:
        DataFrame aligned to schema
    """
    extra_cols = set(df.columns) - set(schema.keys())
    if extra_cols:
        logger.info(f"Dropping {len(extra_cols)} extra columns not in schema")
        logger.debug(f"Extra columns: {sorted(extra_cols)}")
        df = df.drop(columns=list(extra_cols))

    missing_cols = set(schema.keys()) - set(df.columns)
    if missing_cols:
        logger.info(f"Adding {len(missing_cols)} missing columns from schema")
        for col in missing_cols:
            df[col] = pd.NA

    df = df[list(schema.keys())]

    return df


def get_schema_for_source(source_name):
    """
    Get the schema for a given data source.

    Args:
        source_name: 'worldbank' or 'unhcr'

    Returns:
        Schema dictionary
    """
    if source_name == 'worldbank':
        return WORLD_BANK_SCHEMA
    elif source_name == 'unhcr':
        return UNHCR_SCHEMA
    else:
        raise ValueError(f"Unknown source: {source_name}. Must be 'worldbank' or 'unhcr'")
