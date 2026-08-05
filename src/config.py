from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from schemas import UNHCR_SCHEMA, WORLD_BANK_SCHEMA


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = PROJECT_ROOT / "data"


@dataclass(frozen=True)
class SourceConfig:
    name: str
    label: str
    data_dir: Path
    metadata_url: str
    metadata_keys: tuple[str, ...]
    dataset_url: str
    schema: Mapping[str, str]
    headers: Mapping[str, str] | None = None

    @property
    def metadata_file(self) -> Path:
        return self.data_dir / "metadata.csv"

    @property
    def datasets_file(self) -> Path:
        return self.data_dir / "datasets.csv"


SOURCES = (
    SourceConfig(
        name="unhcr",
        label="UNHCR",
        data_dir=DATA_ROOT / "unhcr",
        metadata_url=(
            "https://microdata.unhcr.org/index.php/api/catalog/search"
            "?ps=9999999&sort_by=created&sort_order=desc"
        ),
        metadata_keys=("result", "rows"),
        dataset_url="https://microdata.unhcr.org/index.php/metadata/export/{}/json",
        schema=UNHCR_SCHEMA,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
        },
    ),
    SourceConfig(
        name="worldbank",
        label="World Bank",
        data_dir=DATA_ROOT / "world_bank",
        metadata_url=(
            "https://microdata.worldbank.org/index.php/api/catalog/list_idno/survey"
        ),
        metadata_keys=("records",),
        dataset_url="https://microdata.worldbank.org/index.php/metadata/export/{}",
        schema=WORLD_BANK_SCHEMA,
    ),
)
