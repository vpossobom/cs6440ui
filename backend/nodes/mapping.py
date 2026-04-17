from __future__ import annotations

import json
import os
import re
from typing import Any

from anthropic import Anthropic, AnthropicError


DEFAULT_MODEL = "claude-sonnet-4-5"


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
            raise RuntimeError(f"Claude mapping request failed: {exc}") from exc
        try:
            response = _create_mapping_message(
                client=client,
                model=DEFAULT_MODEL,
                schema_summary=schema_summary,
                sample_rows=sample_rows,
                resource_type=resource_type,
            )
        except AnthropicError as fallback_exc:
            raise RuntimeError(f"Claude mapping request failed: {fallback_exc}") from fallback_exc

    response_text = _message_text(response)
    mapping = parse_mapping_response(response_text)
    mapping.setdefault("resource_type", resource_type)
    mapping.setdefault("mappings", [])

    return mapping


def _is_model_not_found_error(exc: AnthropicError) -> bool:
    return getattr(exc, "status_code", None) == 404 and "model:" in str(exc)


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
