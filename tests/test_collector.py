import logging
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

import collector
from config import SourceConfig


class CollectorTests(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.source = SourceConfig(
            name="test",
            label="Test source",
            data_dir=Path(self.temporary_directory.name),
            metadata_url="https://example.test/metadata",
            metadata_keys=("records",),
            dataset_url="https://example.test/datasets/{}",
            schema={"id": "Int64", "title": "str"},
        )

    def tearDown(self):
        self.temporary_directory.cleanup()
        logging.disable(logging.NOTSET)

    def test_collect_source_writes_sorted_metadata_and_details(self):
        metadata = pd.DataFrame({"id": [1, 2], "type": ["survey", "survey"]})

        with (
            patch.object(collector, "fetch_metadata", return_value=metadata),
            patch.object(
                collector,
                "fetch_dataset",
                side_effect=lambda source, dataset_id: {
                    "id": dataset_id,
                    "title": f"Dataset {dataset_id}",
                },
            ),
        ):
            collector.collect_source(self.source)

        saved_metadata = pd.read_csv(self.source.metadata_file)
        saved_datasets = pd.read_csv(self.source.datasets_file)
        self.assertEqual(saved_metadata["id"].tolist(), [1, 2])
        self.assertEqual(saved_datasets["id"].tolist(), [1, 2])

    def test_failed_detail_fetch_does_not_write_partial_archive(self):
        metadata = pd.DataFrame({"id": [1, 2]})

        def fetch(source, dataset_id):
            if dataset_id == 2:
                raise RuntimeError("API unavailable")
            return {"id": dataset_id, "title": "Dataset 1"}

        with patch.object(collector, "fetch_dataset", side_effect=fetch):
            with self.assertRaisesRegex(RuntimeError, "failed to fetch 1 datasets"):
                collector.update_dataset_archive(self.source, metadata)

        self.assertFalse(self.source.datasets_file.exists())

    def test_run_attempts_all_sources_and_then_raises(self):
        other_source = SourceConfig(
            name="other",
            label="Other source",
            data_dir=Path(self.temporary_directory.name) / "other",
            metadata_url="https://example.test/other",
            metadata_keys=("records",),
            dataset_url="https://example.test/other/{}",
            schema={"id": "Int64"},
        )

        with (
            patch.object(collector, "SOURCES", (self.source, other_source)),
            patch.object(
                collector,
                "collect_source",
                side_effect=[RuntimeError("first failed"), None],
            ) as collect_source,
        ):
            with self.assertRaisesRegex(RuntimeError, "Test source"):
                collector.run()

        self.assertEqual(collect_source.call_count, 2)


if __name__ == "__main__":
    unittest.main()
