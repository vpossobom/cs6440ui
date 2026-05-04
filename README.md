# CS6440 EHR to FHIR Project

This is an app for taking legacy EHR export files and converting them into FHIR R4 JSON bundles. It has a Streamlit frontend and a FastAPI backend. The backend uses Claude to help map columns from the uploaded file to FHIR fields.

The project was built for CS6440 as a basic EHR-to-FHIR translation workflow.

## What's In Here

- `cs6440ui/app/` - the Streamlit web app
- `cs6440ui/backend/` - the FastAPI backend and translation pipeline
- `iClinic_data/` - sample legacy CSV exports I included for testing
- `fhir_bundle_test.json` and `fhir_bundle (1).json` - example FHIR bundle output files

The sample CSV files are from the iClinic-style export data and include patient, contact, event record, and scheduling data. The JSON files are example output bundles so it is easier to see what the app is supposed to create.

## Running It Locally

Run the backend and frontend in separate terminal sessions.

### 1. Start the backend

```bash
cd cs6440ui/backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export ANTHROPIC_API_KEY="your-api-key-here"
uvicorn main:app --reload
```

The backend runs at:

```text
http://localhost:8000
```

You can check it with:

```bash
curl http://localhost:8000/health
```

### 2. Start the frontend

In another terminal:

```bash
cd cs6440ui/app
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export BACKEND_URL="http://localhost:8000"
streamlit run app.py
```

Streamlit opens in the browser at:

```text
http://localhost:8501
```

## How To Use It

1. Start the backend.
2. Start the Streamlit app.
3. Upload one of the CSV files from `iClinic_data/`.
4. Pick the resource type if needed.
5. Click translate.
6. Download or view the generated FHIR bundle.

The app currently supports CSV and Excel uploads. The main resource types it handles are Patient, Practitioner, and Appointment.

## Notes

- The backend needs `ANTHROPIC_API_KEY` or the mapping step will fail.
- The frontend defaults to the deployed backend unless `BACKEND_URL` is set locally.
- The sample JSON bundles are not input files for the upload flow, they are just examples of generated output.
- This is a course project and is intended to demonstrate the basic translation workflow.
