from __future__ import annotations

import json
import math
import re
from typing import Any, Callable

import pandas as pd
from fhir.resources.appointment import Appointment
from fhir.resources.bundle import Bundle
from fhir.resources.patient import Patient
from fhir.resources.practitioner import Practitioner


RESOURCE_BUILDERS: dict[str, Callable[[dict[str, Any], list[dict[str, Any]]], Any]] = {}

DEFAULT_SOURCE_COLUMNS = {
    "Patient": {
        "address",
        "birthdate",
        "city",
        "cpf",
        "email",
        "gender",
        "mobilephone",
        "name",
        "otherdocumentid",
        "patientname",
        "phone",
        "sex",
        "state",
        "telephone",
        "zipcode",
        "zip_code",
    },
    "Practitioner": {
        "birthdate",
        "cpf",
        "dentistname",
        "email",
        "mobilephone",
        "name",
        "otherdocumentid",
        "phone",
        "physicianname",
        "physician_name",
    },
    "Appointment": {
        "canceled",
        "cancelled",
        "date",
        "dentistid",
        "endtime",
        "fromtime",
        "patientid",
        "patient_id",
        "physicianid",
        "physician_id",
        "starttime",
        "status",
        "totime",
    },
}

GENDER_MAP = {
    "m": "male",
    "male": "male",
    "masculino": "male",
    "f": "female",
    "female": "female",
    "feminino": "female",
    "other": "other",
    "unknown": "unknown",
}

APPOINTMENT_STATUS_MAP = {
    "cp": "fulfilled",
    "completed": "fulfilled",
    "complete": "fulfilled",
    "fulfilled": "fulfilled",
    "booked": "booked",
    "scheduled": "booked",
    "pending": "pending",
    "cancelled": "cancelled",
    "canceled": "cancelled",
    "x": "cancelled",
    "noshow": "noshow",
    "no-show": "noshow",
}


def transform_node(state: dict[str, Any]) -> dict[str, Any]:
    dataframe: pd.DataFrame = state["dataframe"]
    mapping = state.get("fhir_mapping", {})
    resource_type = mapping.get("resource_type") or state.get("target_resource_type") or "Patient"
    mapping_entries = mapping.get("mappings", [])
    mapping_warnings = list(mapping.get("mapping_warnings", []))
    builder = RESOURCE_BUILDERS.get(resource_type)

    if builder is None:
        raise ValueError(f"Unsupported FHIR resource type: {resource_type}")

    if not mapping_entries and not _has_default_source_columns(dataframe, resource_type):
        validation_error = {
            "row": None,
            "reason": "No usable FHIR mappings were generated for this file.",
        }
        bundle = _build_bundle([])
        return {
            "fhir_bundle": _resource_to_dict(bundle),
            "validation_report": {
                "resource_type": resource_type,
                "total_rows_processed": int(len(dataframe)),
                "rows_skipped_deleted": 0,
                "resources_created": 0,
                "validation_errors": [validation_error],
                "mapping_warnings": mapping_warnings,
                "error_count": 1,
            },
            "errors": state.get("errors", []) + [validation_error],
        }

    resources = []
    validation_errors = []
    seen_ids: set[str] = set()
    skipped_deleted = 0

    for row_number, row in enumerate(dataframe.to_dict("records"), start=1):
        if _is_deleted(row):
            skipped_deleted += 1
            continue

        if resource_type == "Practitioner":
            dedupe_id = _safe_id(
                _first_present(row, "physician_id", "DentistId", "dentist_id", "id")
            )
            if dedupe_id and dedupe_id in seen_ids:
                continue
            if dedupe_id:
                seen_ids.add(dedupe_id)

        try:
            resources.append(builder(row, mapping_entries))
        except Exception as exc:  # fhir.resources raises validation-specific pydantic errors.
            validation_errors.append({"row": row_number, "reason": str(exc)})

    bundle = _build_bundle(resources)
    validation_report = {
        "resource_type": resource_type,
        "total_rows_processed": int(len(dataframe)),
        "rows_skipped_deleted": skipped_deleted,
        "resources_created": len(resources),
        "validation_errors": validation_errors,
        "mapping_warnings": mapping_warnings,
        "error_count": len(validation_errors),
    }

    return {
        "fhir_bundle": _resource_to_dict(bundle),
        "validation_report": validation_report,
        "errors": state.get("errors", []) + validation_errors,
    }


def build_patient(row: dict[str, Any], mapping: list[dict[str, Any]]) -> Patient:
    resource = _base_resource("Patient", row, "patient_id", "PatientId", "id")
    _apply_mapping(resource, row, mapping)
    _apply_patient_defaults(resource, row)
    return Patient(**resource)


def build_practitioner(row: dict[str, Any], mapping: list[dict[str, Any]]) -> Practitioner:
    resource = _base_resource("Practitioner", row, "physician_id", "DentistId", "dentist_id", "id")
    _apply_mapping(resource, row, mapping)
    _apply_practitioner_defaults(resource, row)
    return Practitioner(**resource)


def build_appointment(row: dict[str, Any], mapping: list[dict[str, Any]]) -> Appointment:
    resource = _base_resource("Appointment", row, "pk", "appointment_id", "id")
    _apply_mapping(resource, row, mapping)
    _apply_appointment_defaults(resource, row)
    return Appointment(**resource)


def _base_resource(resource_type: str, row: dict[str, Any], *id_columns: str) -> dict[str, Any]:
    resource = {"resourceType": resource_type}
    resource_id = _safe_id(_first_present(row, *id_columns))
    if resource_id:
        resource["id"] = resource_id
    return resource


def _apply_mapping(
    resource: dict[str, Any],
    row: dict[str, Any],
    mapping: list[dict[str, Any]],
) -> None:
    for entry in mapping:
        source_column = entry.get("source_column")
        fhir_path = entry.get("fhir_path")
        if not source_column or not fhir_path or source_column not in row:
            continue

        value = row[source_column]
        if _is_empty(value):
            continue

        normalized = _normalize_value(value, entry.get("transform"), source_column, fhir_path)
        if not _is_empty(normalized):
            _set_fhir_path(resource, fhir_path, normalized)


def _apply_patient_defaults(resource: dict[str, Any], row: dict[str, Any]) -> None:
    name = _first_present(row, "name", "Name", "patient_name", "PatientName")
    if name and "name" not in resource:
        resource["name"] = [{"text": str(name)}]

    birth_date = _normalize_date(_first_present(row, "birthdate", "BirthDate", "birth_date"))
    if birth_date and "birthDate" not in resource:
        resource["birthDate"] = birth_date

    gender = _normalize_gender(_first_present(row, "gender", "Sex", "sex"))
    if gender and "gender" not in resource:
        resource["gender"] = gender

    identifier = _first_present(row, "cpf", "CPF", "OtherDocumentId", "document", "document_id")
    if identifier and "identifier" not in resource:
        resource["identifier"] = [{"system": "urn:legacy:document", "value": str(identifier)}]

    _append_telecom_defaults(resource, row)
    _append_address_defaults(resource, row)


def _apply_practitioner_defaults(resource: dict[str, Any], row: dict[str, Any]) -> None:
    name = _first_present(row, "physician_name", "DentistName", "Name", "name")
    if name and "name" not in resource:
        resource["name"] = [{"text": str(name)}]

    birth_date = _normalize_date(_first_present(row, "BirthDate", "birthdate", "birth_date"))
    if birth_date and "birthDate" not in resource:
        resource["birthDate"] = birth_date

    identifier = _first_present(row, "OtherDocumentId", "cpf", "CPF", "document", "document_id")
    if identifier and "identifier" not in resource:
        resource["identifier"] = [{"system": "urn:legacy:document", "value": str(identifier)}]

    _append_telecom_defaults(resource, row)


def _apply_appointment_defaults(resource: dict[str, Any], row: dict[str, Any]) -> None:
    resource.pop("serviceCategory", None)
    resource.pop("serviceType", None)

    status = _normalize_appointment_status(
        _first_present(row, "status", "Status", "Canceled", "cancelled", "canceled")
        or _pack_value(row, "extra_pack", "status")
    )
    resource.setdefault("status", status or "booked")

    start = _combine_date_time(row, "start_time", "fromTime", "from_time")
    end = _combine_date_time(row, "end_time", "toTime", "to_time")
    if start:
        resource["start"] = start
    if end:
        resource["end"] = end

    participants = []
    patient_id = _safe_id(_first_present(row, "patient_id", "PatientId", "patientId"))
    practitioner_id = _safe_id(_first_present(row, "physician_id", "DentistId", "dentist_id"))

    if patient_id:
        participants.append(
            {"actor": {"reference": f"Patient/{patient_id}"}, "status": "accepted"}
        )
    if practitioner_id:
        participants.append(
            {"actor": {"reference": f"Practitioner/{practitioner_id}"}, "status": "accepted"}
        )

    if not participants:
        participants.append({"status": "accepted"})
    resource["participant"] = participants


def _append_telecom_defaults(resource: dict[str, Any], row: dict[str, Any]) -> None:
    telecom = resource.setdefault("telecom", [])
    phone = _normalize_phone(
        _first_present(row, "mobile_phone", "MobilePhone", "phone", "Phone", "telephone")
    )
    email = _first_present(row, "email", "Email")

    if phone:
        telecom.append({"system": "phone", "value": phone, "use": "mobile"})
    if email:
        telecom.append({"system": "email", "value": str(email)})
    if not telecom:
        resource.pop("telecom", None)


def _append_address_defaults(resource: dict[str, Any], row: dict[str, Any]) -> None:
    address_line = _first_present(row, "address", "Address")
    city = _first_present(row, "city", "City")
    state = _first_present(row, "state", "State")
    postal_code = _first_present(row, "zip_code", "zipcode", "ZipCode", "PostalCode")

    if not any([address_line, city, state, postal_code]) or "address" in resource:
        return

    address: dict[str, Any] = {"country": "BR"}
    if address_line:
        address["line"] = [str(address_line)]
    if city:
        address["city"] = str(city)
    if state:
        address["state"] = str(state)
    if postal_code:
        address["postalCode"] = str(postal_code)
    resource["address"] = [address]


def _build_bundle(resources: list[Any]) -> Bundle:
    entries = [{"resource": _resource_to_dict(resource)} for resource in resources]
    return Bundle(**{"resourceType": "Bundle", "type": "collection", "entry": entries})


def _has_default_source_columns(dataframe: pd.DataFrame, resource_type: str) -> bool:
    normalized_columns = {_normalize_column_name(column) for column in dataframe.columns}
    default_columns = {
        _normalize_column_name(column)
        for column in DEFAULT_SOURCE_COLUMNS.get(resource_type, set())
    }
    return bool(normalized_columns & default_columns)


def _resource_to_dict(resource: Any) -> dict[str, Any]:
    if hasattr(resource, "model_dump"):
        return resource.model_dump(mode="json", exclude_none=True)
    if hasattr(resource, "dict"):
        return resource.dict(exclude_none=True)
    return dict(resource)


def _normalize_value(value: Any, transform: Any, source_column: str, fhir_path: str) -> Any:
    transform_text = str(transform or "").lower()
    column_text = source_column.lower()
    path_text = fhir_path.lower()

    if path_text == "id":
        return _safe_id(value)
    if path_text == "active":
        return _normalize_boolean(value)
    if transform_text.endswith("_reference") or path_text.endswith(".reference"):
        return _reference_value(value, transform_text)
    if path_text.endswith(".system"):
        return _system_value_for_path(source_column, fhir_path)
    if path_text.endswith(".value") or path_text.endswith(".text") or path_text.endswith(".display"):
        return _string_value(value)
    if path_text in {"start", "end"} and _looks_like_time_only(value):
        return None
    if "datetime" in transform_text or path_text in {"start", "end", "created", "cancelationdate"}:
        return _normalize_datetime(value)
    if "date" in transform_text or path_text.endswith("birthdate"):
        return _normalize_date(value)
    if "gender" in transform_text or path_text == "gender" or column_text in {"sex", "gender"}:
        return _normalize_gender(value)
    if "phone" in transform_text or "phone" in column_text:
        return _normalize_phone(value)
    if "status" in transform_text or path_text == "status":
        return _normalize_appointment_status(value)
    if path_text in {"start", "end"}:
        return _normalize_datetime(value)
    return _json_safe_scalar(value)


def _normalize_date(value: Any) -> str | None:
    if _is_empty(value):
        return None

    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date().isoformat()


def _normalize_datetime(value: Any) -> str | None:
    if _is_empty(value):
        return None

    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    if getattr(parsed, "tzinfo", None):
        return parsed.isoformat()
    return f"{parsed.to_pydatetime().isoformat()}Z"


def _combine_date_time(row: dict[str, Any], *time_columns: str) -> str | None:
    date_value = _first_present(row, "date", "Date", "appointment_date")
    time_value = _first_present(row, *time_columns)

    if _is_empty(date_value):
        return None

    if _is_empty(time_value):
        return _normalize_datetime(date_value)

    date_part = _normalize_date(date_value)
    if not date_part:
        return None

    parsed_time = pd.to_datetime(time_value, errors="coerce")
    if pd.isna(parsed_time):
        time_text = str(time_value).strip()
        if re.fullmatch(r"\d{1,2}:\d{2}", time_text):
            time_text = f"{time_text}:00"
        return f"{date_part}T{time_text}Z"

    return f"{date_part}T{parsed_time.time().isoformat()}Z"


def _normalize_gender(value: Any) -> str | None:
    if _is_empty(value):
        return None
    return GENDER_MAP.get(str(value).strip().lower(), "unknown")


def _normalize_phone(value: Any) -> str | None:
    if _is_empty(value):
        return None
    digits = re.sub(r"\D", "", str(value))
    return digits or None


def _normalize_appointment_status(value: Any) -> str | None:
    if _is_empty(value):
        return None
    return APPOINTMENT_STATUS_MAP.get(str(value).strip().lower(), "booked")


def _reference_value(value: Any, transform_text: str) -> str | None:
    resource_id = _safe_id(value)
    if not resource_id:
        return None
    if "patient" in transform_text:
        return f"Patient/{resource_id}"
    if "practitioner" in transform_text or "dentist" in transform_text or "physician" in transform_text:
        return f"Practitioner/{resource_id}"
    return resource_id


def _looks_like_time_only(value: Any) -> bool:
    if _is_empty(value):
        return False
    return bool(re.fullmatch(r"\d{1,2}:\d{2}(?::\d{2})?", str(value).strip()))


def _normalize_boolean(value: Any) -> bool | None:
    if _is_empty(value):
        return None
    if isinstance(value, bool):
        return value

    text = str(value).strip().lower()
    if text in {"x", "yes", "y", "true", "1", "active"}:
        return True
    if text in {"no", "n", "false", "0", "inactive"}:
        return False
    return None


def _system_value_for_path(source_column: str, fhir_path: str) -> str | None:
    path_text = fhir_path.lower()
    column_text = source_column.lower()

    if "telecom" in path_text:
        if "email" in column_text:
            return "email"
        if "phone" in column_text:
            return "phone"
    if "identifier" in path_text:
        return f"urn:legacy:{_normalize_column_name(source_column)}"
    return None


def _string_value(value: Any) -> str | None:
    if _is_empty(value):
        return None
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _set_fhir_path(resource: dict[str, Any], path: str, value: Any) -> None:
    current: Any = resource
    parts = path.split(".")

    for index, part in enumerate(parts):
        key, list_index = _parse_path_part(part)
        is_last = index == len(parts) - 1

        if list_index is None:
            if is_last:
                current[key] = value
            else:
                current = current.setdefault(key, {})
            continue

        items = current.setdefault(key, [])
        while len(items) <= list_index:
            items.append({} if not is_last else None)

        if is_last:
            items[list_index] = value
        else:
            if items[list_index] is None:
                items[list_index] = {}
            current = items[list_index]


def _parse_path_part(part: str) -> tuple[str, int | None]:
    match = re.fullmatch(r"([A-Za-z][A-Za-z0-9]*)(?:\[(\d+)])?", part)
    if not match:
        raise ValueError(f"Unsupported FHIR path segment: {part}")
    index = int(match.group(2)) if match.group(2) is not None else None
    return match.group(1), index


def _first_present(row: dict[str, Any], *columns: str) -> Any:
    normalized_columns = {_normalize_column_name(key): key for key in row}
    for column in columns:
        actual_column = normalized_columns.get(_normalize_column_name(column))
        if actual_column is not None and not _is_empty(row[actual_column]):
            return row[actual_column]
    return None


def _is_deleted(row: dict[str, Any]) -> bool:
    deleted = _first_present(row, "Deleted", "deleted")
    return str(deleted).strip().lower() == "x" if deleted is not None else False


def _normalize_column_name(column: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(column).lower())


def _safe_id(value: Any) -> str | None:
    if _is_empty(value):
        return None
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    safe = re.sub(r"[^A-Za-z0-9\-.]", "-", str(value).strip())
    safe = re.sub(r"-+", "-", safe).strip("-")
    return safe or None


def _pack_value(row: dict[str, Any], column: str, key: str) -> Any:
    raw_value = _first_present(row, column)
    if _is_empty(raw_value):
        return None

    text = str(raw_value).strip()
    if text.startswith("json::"):
        text = text.removeprefix("json::")

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None

    if isinstance(payload, dict):
        return payload.get(key)
    return None


def _json_safe_scalar(value: Any) -> Any:
    if _is_empty(value):
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


RESOURCE_BUILDERS.update(
    {
        "Patient": build_patient,
        "Practitioner": build_practitioner,
        "Appointment": build_appointment,
    }
)
