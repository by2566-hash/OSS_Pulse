from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
CLEANED_DIR = DATA_DIR / "cleaned"
OUTPUT_DIR = PROJECT_ROOT / "output"
SAMPLES_DIR = OUTPUT_DIR / "samples"
PROFILING_DIR = OUTPUT_DIR / "profiling"
REPORT_DIR = PROJECT_ROOT / "report"

GHARCHIVE_RAW_DIR = RAW_DIR / "gharchive"
GHARCHIVE_CLEANED_DIR = CLEANED_DIR / "gharchive"


def ensure_parent_dir(path: str | Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    return target
