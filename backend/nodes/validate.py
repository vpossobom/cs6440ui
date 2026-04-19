from __future__ import annotations

from typing import Any


def validate_node(state: dict[str, Any]) -> dict[str, Any]:
    mapping = state.get("fhir_mapping", {})
    metadata = state.get("metadata", {})
    validation_report = dict(state.get("validation_report", {}))

    mapped_columns = {
        entry.get("source_column")
        for entry in mapping.get("mappings", [])
        if entry.get("source_column")
    }
    json_blob_columns = set(metadata.get("json_blob_columns", []))
    source_columns = set(metadata.get("columns", []))

    validation_report["unmapped_fields"] = sorted(source_columns - mapped_columns - json_blob_columns)
    validation_report["json_blob_fields_skipped"] = sorted(json_blob_columns)
    validation_report["bundle_type"] = "collection"

    return {"validation_report": validation_report}
