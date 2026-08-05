from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from pathlib import Path
import tempfile
from typing import Any

import pandas as pd
import requests

from config import SOURCES, SourceConfig
from schemas import apply_prefix_mapping, enforce_schema


logger = logging.getLogger(__name__)

MAX_WORKERS = 20
REQUEST_TIMEOUT = 60


def fetch_json(
    url: str, headers: dict[str, str] | None = None
) -> dict[str, Any]:
    response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        raise ValueError(f"Expected a JSON object from {url}")
    return data


def fetch_metadata(source: SourceConfig) -> pd.DataFrame:
    data: Any = fetch_json(source.metadata_url, source.headers)
    try:
        for key in source.metadata_keys:
            data = data[key]
    except (KeyError, TypeError) as exc:
        path = ".".join(source.metadata_keys)
        raise ValueError(
            f"{source.label} metadata response is missing {path}"
        ) from exc

    metadata = pd.DataFrame(data)
    if metadata.empty or "id" not in metadata.columns:
        raise ValueError(f"{source.label} returned no usable metadata records")
    return metadata.sort_values("id").reset_index(drop=True)


def fetch_dataset(source: SourceConfig, dataset_id: Any) -> dict[str, Any]:
    data = fetch_json(source.dataset_url.format(dataset_id), source.headers)
    data["id"] = dataset_id
    return data


def write_csv(df: pd.DataFrame, output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            dir=output_file.parent,
            prefix=f".{output_file.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary_file:
            temporary_path = Path(temporary_file.name)
            df.to_csv(temporary_file, index=False)
        temporary_path.replace(output_file)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()


def update_dataset_archive(source: SourceConfig, metadata: pd.DataFrame) -> None:
    existing = None
    existing_ids: set[Any] = set()
    if source.datasets_file.exists():
        existing = pd.read_csv(source.datasets_file)
        if "id" not in existing.columns:
            raise ValueError(f"{source.datasets_file} is missing the id column")
        existing_ids = set(existing["id"].dropna())

    new_ids = [
        dataset_id
        for dataset_id in metadata["id"].dropna().unique()
        if dataset_id not in existing_ids
    ]
    if not new_ids:
        logger.info("%s: no new detailed records", source.label)
        return

    logger.info(
        "%s: fetching %d new detailed records out of %d",
        source.label,
        len(new_ids),
        len(metadata),
    )
    records: list[dict[str, Any]] = []
    failures: list[tuple[Any, Exception]] = []
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(new_ids))) as executor:
        futures = {
            executor.submit(fetch_dataset, source, dataset_id): dataset_id
            for dataset_id in new_ids
        }
        for future in as_completed(futures):
            dataset_id = futures[future]
            try:
                records.append(future.result())
            except Exception as exc:
                failures.append((dataset_id, exc))
                logger.error(
                    "%s: failed to fetch dataset %s: %s",
                    source.label,
                    dataset_id,
                    exc,
                )

    if failures:
        failed_ids = ", ".join(str(dataset_id) for dataset_id, _ in failures)
        raise RuntimeError(
            f"{source.label} failed to fetch {len(failures)} datasets: {failed_ids}"
        ) from failures[0][1]

    new_records = enforce_schema(
        apply_prefix_mapping(pd.json_normalize(records)), source.schema
    )
    if existing is not None:
        existing = enforce_schema(existing, source.schema)
        datasets = pd.concat([existing, new_records], ignore_index=True)
    else:
        datasets = new_records

    datasets = datasets.sort_values("id").reset_index(drop=True)
    write_csv(datasets, source.datasets_file)
    logger.info(
        "%s: saved %d detailed records to %s",
        source.label,
        len(datasets),
        source.datasets_file,
    )


def collect_source(source: SourceConfig) -> None:
    logger.info("%s: fetching catalog metadata", source.label)
    metadata = fetch_metadata(source)
    write_csv(metadata, source.metadata_file)
    logger.info(
        "%s: saved %d metadata records to %s",
        source.label,
        len(metadata),
        source.metadata_file,
    )
    update_dataset_archive(source, metadata)


def run() -> None:
    failures: list[tuple[SourceConfig, Exception]] = []
    for source in SOURCES:
        try:
            collect_source(source)
        except Exception as exc:
            failures.append((source, exc))
            logger.exception("%s collection failed", source.label)

    if failures:
        failed_sources = ", ".join(source.label for source, _ in failures)
        raise RuntimeError(f"Collection failed for: {failed_sources}") from failures[0][1]
