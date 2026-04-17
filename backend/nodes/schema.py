from __future__ import annotations

import warnings
from typing import Any

import pandas as pd
from pandas.api.types import (
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
)


def analyze_schema_node(state: dict[str, Any]) -> dict[str, Any]:
    dataframe: pd.DataFrame = state["dataframe"]
    metadata = state.get("metadata", {})
    json_blob_columns = set(metadata.get("json_blob_columns", []))

    schema_summary = {
        "row_count": metadata.get("row_count", int(len(dataframe))),
        "column_count": metadata.get("column_count", int(len(dataframe.columns))),
        "columns": [
            _summarize_column(dataframe, column, column in json_blob_columns)
            for column in dataframe.columns
        ],
    }

    return {
        "schema_summary": schema_summary,
        "sample_rows": _sample_rows(dataframe),
    }


def _summarize_column(
    dataframe: pd.DataFrame,
    column: str,
    is_json_blob: bool,
) -> dict[str, Any]:
    series = dataframe[column]
    non_null = series.dropna()
    null_percentage = 0.0
    if len(series) > 0:
        null_percentage = round(float(series.isna().mean() * 100), 2)

    if is_json_blob:
        inferred_type = "json_blob"
    else:
        inferred_type = _infer_type(series)

    return {
        "name": column,
        "type": inferred_type,
        "null_percentage": null_percentage,
        "unique_count": int(non_null.nunique(dropna=True)),
        "sample_values": [_json_safe(value) for value in non_null.head(3).tolist()],
    }


def _infer_type(series: pd.Series) -> str:
    non_null = series.dropna().astype(str)
    if non_null.empty:
        return "empty"

    if is_bool_dtype(series):
        return "boolean"
    if is_numeric_dtype(series):
        return "number"
    if is_datetime64_any_dtype(series):
        return "datetime"

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        datetime_parse_rate = pd.to_datetime(
            non_null.head(50),
            errors="coerce",
        ).notna().mean()
    if datetime_parse_rate >= 0.8:
        return "datetime"

    numeric_parse_rate = pd.to_numeric(non_null.head(50), errors="coerce").notna().mean()
    if numeric_parse_rate >= 0.8:
        return "number"

    return "string"


def _sample_rows(dataframe: pd.DataFrame) -> list[dict[str, Any]]:
    records = dataframe.head(3).where(pd.notna(dataframe.head(3)), None).to_dict("records")
    return [{key: _json_safe(value) for key, value in row.items()} for row in records]


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if pd.isna(value):
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value
