"""
Schema management module for microdata collector.

Provides fixed schemas and column mapping utilities to prevent schema drift.
"""

from .column_mappings import (
    apply_prefix_mapping,
    enforce_schema,
    get_schema_for_source,
    WORLD_BANK_SCHEMA,
    UNHCR_SCHEMA,
)

__all__ = [
    'apply_prefix_mapping',
    'enforce_schema',
    'get_schema_for_source',
    'WORLD_BANK_SCHEMA',
    'UNHCR_SCHEMA',
]
