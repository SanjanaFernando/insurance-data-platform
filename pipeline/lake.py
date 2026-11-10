"""Local data-lake helpers: land files in raw/ and validate them before processing."""
import shutil
from pathlib import Path


def copy_file(src_dir: str, dest_dir: str, fname: str) -> str:
    """Copy one source file into the raw zone (the Airflow equivalent of an ADF Copy activity)."""
    src = Path(src_dir) / fname
    if not src.is_file():
        raise FileNotFoundError(f"source file not found: {src}")
    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)
    target = dest / fname
    shutil.copy2(src, target)
    return str(target)


def validate_files(paths) -> None:
    """Fail the run early if any landed file is missing, empty, or has no data rows."""
    problems = []
    for path in paths:
        p = Path(path)
        if not p.is_file():
            problems.append(f"missing: {p}")
        elif p.stat().st_size == 0:
            problems.append(f"empty: {p}")
        else:
            with p.open() as handle:
                lines = sum(1 for _ in handle)
            if lines < 2:                                  # header only
                problems.append(f"no data rows: {p}")
    if problems:
        raise ValueError("raw validation failed: " + "; ".join(problems))
