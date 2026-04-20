from __future__ import annotations

import tempfile
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, UploadFile

from pipeline import run_pipeline


app = FastAPI(title="CS6440 EHR to FHIR Backend")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/translate")
async def translate(
    file: UploadFile = File(...),
    resource_type: str = Form(default="Patient"),
) -> dict:
    if not file.filename:
        raise HTTPException(status_code=400, detail="Uploaded file must have a filename.")

    suffix = Path(file.filename).suffix
    temp_path = None

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
            temp_path = temp_file.name
            temp_file.write(await file.read())

        result = run_pipeline(temp_path, target_resource_type=resource_type)
        validation_report = result["validation_report"]
        return {
            "bundle": result["fhir_bundle"],
            "validation_report": validation_report,
            "stats": _build_stats(validation_report),
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        if temp_path:
            Path(temp_path).unlink(missing_ok=True)


def _build_stats(validation_report: dict) -> dict[str, int]:
    return {
        "rows_processed": int(validation_report.get("total_rows_processed", 0)),
        "resources_created": int(validation_report.get("resources_created", 0)),
        "error_count": int(validation_report.get("error_count", 0)),
    }
