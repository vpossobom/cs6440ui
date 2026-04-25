# EHR → FHIR Translation Tool

A provider-facing Streamlit web application for the legacy EHR to FHIR R4 translation pipeline. Upload a legacy EHR file (CSV or Excel), send it to the FastAPI/LangGraph backend, then download the generated FHIR R4 Bundle.

---

## Project Structure

```
app/
├── app.py            # Main Streamlit application
├── requirements.txt  # Python dependencies
└── README.md         # This file
```

---

## Running Locally

### Prerequisites

- Python 3.9 or later
- `pip`

### Steps

```bash
# 1. Navigate to the app directory
cd app

# 2. (Recommended) Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate        # macOS / Linux
# .venv\Scripts\activate         # Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Launch the app
streamlit run app.py
```

The app will open automatically at `http://localhost:8501`. By default it calls `https://cs6440ui.onrender.com`; set `BACKEND_URL` in the environment or Streamlit secrets to point at a different backend.

---

## Deploying to Streamlit Community Cloud

1. Push this repository to GitHub (the `app/` directory must be at the repo root or you can point Streamlit Cloud to a subdirectory).

2. Go to [share.streamlit.io](https://share.streamlit.io) and sign in with your GitHub account.

3. Click **New app** and fill in:
   - **Repository**: your GitHub repo
   - **Branch**: `main` (or your default branch)
   - **Main file path**: `app/app.py`

4. Click **Deploy**. Streamlit Cloud will automatically install dependencies from `requirements.txt`.

The app will be live at a URL like `https://<your-app-name>.streamlit.app`.

---

## Usage

1. Open the app in your browser.
2. Drag and drop (or click to browse) a legacy EHR file — `.csv` or `.xlsx`.
3. Click **Translate**.
4. Watch each pipeline step complete in sequence:
   - Schema Analyzed
   - FHIR Mapping Generated
   - Data Transformed
   - Bundle Validated
5. Once complete, expand **View Bundle** to inspect the FHIR R4 JSON, or click **Download Bundle** to save it locally.

---

## Tech Stack

| Layer | Technology |
|-------|------------|
| UI | [Streamlit](https://streamlit.io) >= 1.32 |
| FHIR standard | HL7 FHIR R4 |
| Backend | FastAPI |
| Agent | LangGraph + Claude |

---

*CareCode — EHR Migration Tool | Powered by Claude*
