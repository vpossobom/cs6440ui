from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
from pandas.errors import EmptyDataError


SUPPORTED_EXCEL_SUFFIXES = {".xls", ".xlsx"}


def read_source_file(file_path: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Read a CSV/XLSX source file and return a cleaned dataframe plus metadata."""
    path = Path(file_path)
    suffix = path.suffix.lower()

    if suffix == ".csv":
        dataframe, encoding = _read_csv_with_fallback(path)
        source_format = "csv"
    elif suffix in SUPPORTED_EXCEL_SUFFIXES:
        dataframe = pd.read_excel(path)
        encoding = None
        source_format = "xlsx"
    else:
        raise ValueError(f"Unsupported file type: {suffix or 'unknown'}")

    json_blob_columns = _detect_json_blob_columns(dataframe)
    dataframe = _drop_empty_source_rows(dataframe, json_blob_columns)
    if dataframe.empty:
        raise ValueError("Uploaded file contains no data rows to translate.")

    metadata: dict[str, Any] = {
        "format": source_format,
        "encoding": encoding,
        "row_count": int(len(dataframe)),
        "column_count": int(len(dataframe.columns)),
        "columns": list(dataframe.columns),
        "json_blob_columns": json_blob_columns,
    }

    return dataframe, metadata


def ingest_node(state: dict[str, Any]) -> dict[str, Any]:
    dataframe, metadata = read_source_file(state["file_path"])

    return {
        "dataframe": dataframe,
        "metadata": metadata,
        "errors": state.get("errors", []),
    }


def _read_csv_with_fallback(path: Path) -> tuple[pd.DataFrame, str]:
    try:
        return pd.read_csv(path, encoding="utf-8"), "utf-8"
    except UnicodeDecodeError:
        try:
            return pd.read_csv(path, encoding="latin-1"), "latin-1"
        except EmptyDataError as exc:
            raise ValueError("Uploaded CSV is empty or has no columns.") from exc
    except EmptyDataError as exc:
        raise ValueError("Uploaded CSV is empty or has no columns.") from exc


def _detect_json_blob_columns(dataframe: pd.DataFrame) -> list[str]:
    json_blob_columns: list[str] = []

    for column in dataframe.columns:
        if str(column).endswith("_pack"):
            json_blob_columns.append(column)
            continue

        values = dataframe[column].dropna().head(20)
        if any(_looks_like_json_blob(value) for value in values):
            json_blob_columns.append(column)

    return json_blob_columns


def _drop_empty_source_rows(
    dataframe: pd.DataFrame,
    json_blob_columns: list[str],
) -> pd.DataFrame:
    source_columns = [column for column in dataframe.columns if column not in json_blob_columns]
    if not source_columns:
        return dataframe.dropna(how="all").reset_index(drop=True)
    return dataframe.dropna(how="all", subset=source_columns).reset_index(drop=True)


def _looks_like_json_blob(value: Any) -> bool:
    if not isinstance(value, str):
        return False

    stripped = value.strip()
    if not stripped or stripped[0] not in "[{":
        return False

    try:
        json.loads(stripped)
    except json.JSONDecodeError:
        return False

    return True
