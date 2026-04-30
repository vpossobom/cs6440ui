from __future__ import annotations

import json
import os
import re
from typing import Any

from anthropic import Anthropic, AnthropicError


DEFAULT_MODEL = "claude-sonnet-4-5"

SUPPORTED_RESOURCE_TYPES = {"Patient", "Practitioner", "Appointment"}

FHIR_PATH_ROOTS = {
    "Patient": {
        "active",
        "address",
        "birthDate",
        "gender",
        "id",
        "identifier",
        "name",
        "telecom",
    },
    "Practitioner": {
        "active",
        "address",
        "birthDate",
        "gender",
        "id",
        "identifier",
        "name",
        "telecom",
    },
    "Appointment": {
        "appointmentType",
        "cancelationReason",
        "created",
        "description",
        "end",
        "id",
        "identifier",
        "participant",
        "reasonCode",
        "serviceCategory",
        "serviceType",
        "start",
        "status",
    },
}


SYSTEM_PROMPT = """You map legacy EHR exports to FHIR R4 resources.

Return only valid JSON with this exact shape:
{
  "resource_type": "Patient",
  "mappings": [
    {
      "source_column": "name",
      "fhir_path": "name[0].text",
      "transform": null
    }
  ]
}

Rules:
- Use only source columns that appear in the provided schema.
- Skip columns with type "json_blob".
- Prefer common FHIR R4 paths for Patient, Practitioner, and Appointment.
- Use transform only when a value clearly needs normalization, such as date, gender, phone, or status.
- If no columns map cleanly, return an empty mappings array.
"""


def generate_mapping_node(state: dict[str, Any]) -> dict[str, Any]:
    resource_type = state.get("target_resource_type") or infer_resource_type(
        state.get("file_path", "")
    )
    mapping = generate_fhir_mapping(
        schema_summary=state["schema_summary"],
        sample_rows=state.get("sample_rows", []),
        resource_type=resource_type,
    )

    return {"fhir_mapping": mapping}


def generate_fhir_mapping(
    schema_summary: dict[str, Any],
    sample_rows: list[dict[str, Any]],
    resource_type: str,
) -> dict[str, Any]:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY is required to generate FHIR mappings.")

    client = Anthropic(api_key=api_key)
    model = os.getenv("ANTHROPIC_MODEL", DEFAULT_MODEL)
    try:
        response = _create_mapping_message(
            client=client,
            model=model,
            schema_summary=schema_summary,
            sample_rows=sample_rows,
            resource_type=resource_type,
        )
    except AnthropicError as exc:
        if model == DEFAULT_MODEL or not _is_model_not_found_error(exc):
            raise RuntimeError(_anthropic_error_message(exc)) from exc
        try:
            response = _create_mapping_message(
                client=client,
                model=DEFAULT_MODEL,
                schema_summary=schema_summary,
                sample_rows=sample_rows,
                resource_type=resource_type,
            )
        except AnthropicError as fallback_exc:
            raise RuntimeError(_anthropic_error_message(fallback_exc)) from fallback_exc

    response_text = _message_text(response)
    try:
        mapping = parse_mapping_response(response_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Claude returned a mapping that was not valid JSON.") from exc

    return sanitize_mapping(mapping, schema_summary, resource_type)


def _is_model_not_found_error(exc: AnthropicError) -> bool:
    return getattr(exc, "status_code", None) == 404 and "model:" in str(exc)


def _anthropic_error_message(exc: AnthropicError) -> str:
    status_code = getattr(exc, "status_code", None)
    error_name = exc.__class__.__name__.lower()

    if status_code == 429 or "ratelimit" in error_name or "rate_limit" in error_name:
        return "Claude mapping request was rate limited. Please wait a minute and retry."
    if "timeout" in error_name:
        return "Claude mapping request timed out. Please retry the translation."
    return f"Claude mapping request failed: {exc}"


def _create_mapping_message(
    client: Anthropic,
    model: str,
    schema_summary: dict[str, Any],
    sample_rows: list[dict[str, Any]],
    resource_type: str,
) -> Any:
    return client.messages.create(
        model=model,
        max_tokens=1600,
        temperature=0,
        system=SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": _build_user_prompt(schema_summary, sample_rows, resource_type),
            }
        ],
    )


def parse_mapping_response(response_text: str) -> dict[str, Any]:
    cleaned = _strip_code_fence(response_text.strip())

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        json_match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
        if not json_match:
            raise
        return json.loads(json_match.group(0))


def sanitize_mapping(
    mapping: dict[str, Any],
    schema_summary: dict[str, Any],
    requested_resource_type: str,
) -> dict[str, Any]:
    if not isinstance(mapping, dict):
        raise RuntimeError("Claude returned a mapping with an invalid shape.")

    resource_type = mapping.get("resource_type") or requested_resource_type
    if resource_type not in SUPPORTED_RESOURCE_TYPES:
        resource_type = requested_resource_type
    if resource_type not in SUPPORTED_RESOURCE_TYPES:
        raise ValueError(f"Unsupported FHIR resource type: {resource_type}")

    source_columns = _schema_source_columns(schema_summary)
    json_blob_columns = _schema_json_blob_columns(schema_summary)
    cleaned_entries = []
    warnings = []

    raw_entries = mapping.get("mappings", [])
    if not isinstance(raw_entries, list):
        raw_entries = []
        warnings.append("Claude returned mappings in an invalid format.")

    for entry in raw_entries:
        cleaned_entry, reason = _clean_mapping_entry(
            entry,
            resource_type,
            source_columns,
            json_blob_columns,
        )
        if cleaned_entry:
            cleaned_entries.append(cleaned_entry)
        elif reason:
            warnings.append(reason)

    return {
        "resource_type": resource_type,
        "mappings": cleaned_entries,
        "mapping_warnings": warnings,
    }


def _clean_mapping_entry(
    entry: Any,
    resource_type: str,
    source_columns: set[str],
    json_blob_columns: set[str],
) -> tuple[dict[str, Any] | None, str | None]:
    if not isinstance(entry, dict):
        return None, "Skipped mapping entry because it was not an object."

    source_column = entry.get("source_column")
    fhir_path = entry.get("fhir_path")
    if source_column not in source_columns:
        return None, f"Skipped mapping for unknown source column: {source_column!r}."
    if source_column in json_blob_columns:
        return None, f"Skipped JSON blob source column: {source_column}."
    if not _is_supported_fhir_path(resource_type, fhir_path):
        return None, f"Skipped unsupported FHIR path: {fhir_path!r}."

    return {
        "source_column": source_column,
        "fhir_path": fhir_path,
        "transform": entry.get("transform"),
    }, None


def _schema_source_columns(schema_summary: dict[str, Any]) -> set[str]:
    return {
        column.get("name")
        for column in schema_summary.get("columns", [])
        if isinstance(column, dict) and column.get("name")
    }


def _schema_json_blob_columns(schema_summary: dict[str, Any]) -> set[str]:
    return {
        column.get("name")
        for column in schema_summary.get("columns", [])
        if isinstance(column, dict)
        and column.get("name")
        and column.get("type") == "json_blob"
    }


def _is_supported_fhir_path(resource_type: str, fhir_path: Any) -> bool:
    if not isinstance(fhir_path, str) or not fhir_path.strip():
        return False

    parts = fhir_path.split(".")
    root = _path_key(parts[0])
    if root not in FHIR_PATH_ROOTS[resource_type]:
        return False

    return all(_path_key(part) for part in parts)


def _path_key(path_part: str) -> str | None:
    match = re.fullmatch(r"([A-Za-z][A-Za-z0-9]*)(?:\[\d+])?", path_part)
    return match.group(1) if match else None


def infer_resource_type(file_path: str) -> str:
    lowered = file_path.lower()
    if "dentist" in lowered or "physician" in lowered:
        return "Practitioner"
    if "appointment" in lowered or "scheduling" in lowered or "event" in lowered:
        return "Appointment"
    return "Patient"


def _build_user_prompt(
    schema_summary: dict[str, Any],
    sample_rows: list[dict[str, Any]],
    resource_type: str,
) -> str:
    return "\n\n".join(
        [
            f"Map this to a FHIR R4 {resource_type} resource.",
            "Schema summary:",
            json.dumps(schema_summary, ensure_ascii=False, indent=2),
            "Three sample rows from the source data:",
            json.dumps(sample_rows, ensure_ascii=False, indent=2),
        ]
    )


def _message_text(response: Any) -> str:
    text_parts: list[str] = []
    for block in response.content:
        text = getattr(block, "text", None)
        if text:
            text_parts.append(text)
    return "\n".join(text_parts)


def _strip_code_fence(text: str) -> str:
    fence_match = re.fullmatch(r"```(?:json)?\s*(.*?)\s*```", text, flags=re.DOTALL)
    if fence_match:
        return fence_match.group(1).strip()
    return text
