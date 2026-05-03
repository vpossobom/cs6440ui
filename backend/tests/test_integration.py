from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

import main  # noqa: E402
from nodes.ingest import read_source_file  # noqa: E402
from nodes.schema import analyze_schema_node  # noqa: E402
from nodes.transform import transform_node  # noqa: E402
from nodes.validate import validate_node  # noqa: E402


client = TestClient(main.app)


def test_health_check_returns_ok() -> None:
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_translate_upload_returns_bundle_shape(monkeypatch) -> None:
    def fake_run_pipeline(file_path: str, target_resource_type: str) -> dict:
        assert target_resource_type == "Patient"
        assert Path(file_path).read_text() == "id,name,gender\np-1,Ana Silva,F\n"
        return {
            "fhir_bundle": {
                "resourceType": "Bundle",
                "type": "collection",
                "entry": [
                    {
                        "resource": {
                            "resourceType": "Patient",
                            "id": "p-1",
                            "name": [{"text": "Ana Silva"}],
                        }
                    }
                ],
            },
            "validation_report": {
                "total_rows_processed": 1,
                "resources_created": 1,
                "error_count": 0,
            },
        }

    monkeypatch.setattr(main, "run_pipeline", fake_run_pipeline)

    response = client.post(
        "/translate",
        data={"resource_type": "Patient"},
        files={"file": ("patients.csv", b"id,name,gender\np-1,Ana Silva,F\n", "text/csv")},
    )

    payload = response.json()
    assert response.status_code == 200
    assert payload["bundle"]["resourceType"] == "Bundle"
    assert payload["bundle"]["entry"][0]["resource"]["resourceType"] == "Patient"
    assert payload["stats"] == {
        "rows_processed": 1,
        "resources_created": 1,
        "error_count": 0,
    }


def test_translate_empty_file_returns_400() -> None:
    response = client.post(
        "/translate",
        files={"file": ("empty.csv", b"", "text/csv")},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "Uploaded file is empty."


def test_translate_unsupported_file_type_returns_400() -> None:
    response = client.post(
        "/translate",
        files={"file": ("patients.txt", b"id,name\np-1,Ana Silva\n", "text/plain")},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "Unsupported file type: .txt"


def test_csv_patient_legacy_format_builds_fhir_bundle(tmp_path: Path) -> None:
    csv_path = tmp_path / "patients.csv"
    csv_path.write_text(
        "\n".join(
            [
                "patient_id,name,birthdate,gender,email,legacy_pack",
                'p-1,Ana Silva,1985-04-12,F,ana@example.com,"{""ignored"": true}"',
            ]
        )
    )

    bundle, report = _run_local_pipeline(
        csv_path,
        resource_type="Patient",
        mappings=[
            {"source_column": "patient_id", "fhir_path": "id", "transform": None},
            {"source_column": "name", "fhir_path": "name[0].text", "transform": None},
            {"source_column": "gender", "fhir_path": "gender", "transform": "gender"},
        ],
    )

    resource = bundle["entry"][0]["resource"]
    assert bundle["resourceType"] == "Bundle"
    assert bundle["type"] == "collection"
    assert resource["resourceType"] == "Patient"
    assert resource["id"] == "p-1"
    assert resource["gender"] == "female"
    assert resource["birthDate"] == "1985-04-12"
    assert report["resources_created"] == 1
    assert report["json_blob_fields_skipped"] == ["legacy_pack"]


def test_xlsx_appointment_legacy_format_skips_deleted_rows(tmp_path: Path) -> None:
    xlsx_path = tmp_path / "appointments.xlsx"
    pd.DataFrame(
        [
            {
                "pk": "a-1",
                "patient_id": "p-1",
                "physician_id": "d-1",
                "date": "2026-05-02",
                "fromTime": "09:00",
                "toTime": "09:30",
                "status": "scheduled",
                "Deleted": "",
            },
            {
                "pk": "a-2",
                "patient_id": "p-2",
                "physician_id": "d-2",
                "date": "2026-05-03",
                "fromTime": "10:00",
                "toTime": "10:30",
                "status": "scheduled",
                "Deleted": "x",
            },
        ]
    ).to_excel(xlsx_path, index=False)

    bundle, report = _run_local_pipeline(
        xlsx_path,
        resource_type="Appointment",
        mappings=[
            {"source_column": "pk", "fhir_path": "id", "transform": None},
            {"source_column": "status", "fhir_path": "status", "transform": "status"},
        ],
    )

    assert report["total_rows_processed"] == 2
    assert report["rows_skipped_deleted"] == 1
    assert report["resources_created"] == 1

    resource = bundle["entry"][0]["resource"]
    assert resource["resourceType"] == "Appointment"
    assert resource["id"] == "a-1"
    assert resource["status"] == "booked"
    assert resource["start"] == "2026-05-02T09:00:00Z"
    assert resource["end"] == "2026-05-02T09:30:00Z"
    assert resource["participant"][0]["actor"]["reference"] == "Patient/p-1"
    assert resource["participant"][1]["actor"]["reference"] == "Practitioner/d-1"


def _run_local_pipeline(
    file_path: Path,
    *,
    resource_type: str,
    mappings: list[dict],
) -> tuple[dict, dict]:
    dataframe, metadata = read_source_file(str(file_path))
    state = {
        "file_path": str(file_path),
        "target_resource_type": resource_type,
        "dataframe": dataframe,
        "metadata": metadata,
        "errors": [],
    }
    state.update(analyze_schema_node(state))
    state["fhir_mapping"] = {
        "resource_type": resource_type,
        "mappings": mappings,
        "mapping_warnings": [],
    }
    state.update(transform_node(state))
    state.update(validate_node(state))
    return state["fhir_bundle"], state["validation_report"]
