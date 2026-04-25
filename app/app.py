"""
app.py
EHR → FHIR Translation Tool — Provider-facing Streamlit UI.

Runs a LangGraph agent pipeline that:
  1. Analyzes the uploaded EHR file schema
  2. Generates a FHIR mapping using an LLM
  3. Transforms the data
  4. Validates and packages the FHIR bundle
"""

import json
import os
import time

import requests
import streamlit as st

DEFAULT_BACKEND_URL = "https://cs6440ui.onrender.com"


def _get_backend_url() -> str:
    env_url = os.getenv("BACKEND_URL")
    if env_url:
        return env_url

    try:
        return st.secrets.get("BACKEND_URL") or DEFAULT_BACKEND_URL
    except Exception:
        return DEFAULT_BACKEND_URL

# ---------------------------------------------------------------------------
# Page configuration — must be the first Streamlit call
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="EHR → FHIR Translation Tool",
    page_icon="🏥",
    layout="centered",
    initial_sidebar_state="collapsed",
)

BACKEND_URL = _get_backend_url()

# ---------------------------------------------------------------------------
# Session state initialisation
# ---------------------------------------------------------------------------
if "stage" not in st.session_state:
    st.session_state.stage = "idle"          # idle | file_ready | translating | complete

if "uploaded_file_name" not in st.session_state:
    st.session_state.uploaded_file_name = None

if "uploaded_file_size" not in st.session_state:
    st.session_state.uploaded_file_size = 0

if "bundle_json" not in st.session_state:
    st.session_state.bundle_json = None

if "validation_report" not in st.session_state:
    st.session_state.validation_report = None

if "stats" not in st.session_state:
    st.session_state.stats = None

if "error_message" not in st.session_state:
    st.session_state.error_message = None

if "selected_resource_type" not in st.session_state:
    st.session_state.selected_resource_type = "Patient"

# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _fmt_size(size_bytes: int) -> str:
    """Return a human-readable file size string."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 ** 2:
        return f"{size_bytes / 1024:.1f} KB"
    else:
        return f"{size_bytes / (1024 ** 2):.2f} MB"


def _infer_resource_type(filename: str) -> str:
    """Use the filename to select a sensible default target resource."""
    lowered = filename.lower()
    if "dentist" in lowered or "physician" in lowered:
        return "Practitioner"
    if "appointment" in lowered or "scheduling" in lowered or "event" in lowered:
        return "Appointment"
    return "Patient"


def _post_translation(uploaded_file, resource_type: str) -> dict:
    files = {
        "file": (
            uploaded_file.name,
            uploaded_file.getvalue(),
            uploaded_file.type or "application/octet-stream",
        )
    }
    response = requests.post(
        f"{BACKEND_URL.rstrip('/')}/translate",
        files=files,
        data={"resource_type": resource_type},
        timeout=180,
    )

    if response.status_code != 200:
        try:
            detail = response.json().get("detail", response.text)
        except ValueError:
            detail = response.text
        raise RuntimeError(f"Backend returned {response.status_code}: {detail}")

    return response.json()


def _run_pipeline(uploaded_file, resource_type: str, step_placeholders: list) -> None:
    """
    Run the backend translation and map its response into Streamlit session state.
    The backend returns the final result in one response, so the UI shows a
    single in-progress line before marking the remaining steps complete.
    """
    steps = [
        "Schema Analyzed",
        "FHIR Mapping Generated",
        "Data Transformed",
        "Bundle Validated",
    ]

    step_placeholders[0].markdown(f"⏳ &nbsp; **{steps[0]}** — *backend running…*")

    result = _post_translation(uploaded_file, resource_type)

    for i, label in enumerate(steps):
        step_placeholders[i].markdown(f"✅ &nbsp; **{label}**")

    st.session_state.bundle_json = json.dumps(result["bundle"], indent=2, ensure_ascii=False)
    st.session_state.validation_report = result.get("validation_report", {})
    st.session_state.stats = result.get("stats", {})
    st.session_state.error_message = None
    st.session_state.stage = "complete"


def _resource_summary(bundle: dict | None) -> str:
    if not bundle:
        return "0 FHIR resources generated"

    entries = bundle.get("entry") or []
    resource_types = sorted(
        {
            entry.get("resource", {}).get("resourceType")
            for entry in entries
            if entry.get("resource", {}).get("resourceType")
        }
    )
    type_text = ", ".join(resource_types) if resource_types else "FHIR resources"
    return f"{len(entries)} FHIR resources generated ({type_text})"


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
col_title, col_menu = st.columns([9, 1])
with col_title:
    st.title("EHR → FHIR Translation Tool")
with col_menu:
    st.write("")          # vertical alignment nudge
    st.write("")
    st.write("☰")         # cosmetic hamburger icon

st.divider()

# ---------------------------------------------------------------------------
# File upload section
# ---------------------------------------------------------------------------
st.subheader("Upload Legacy EHR File")
st.write("Drag and drop a legacy EHR export to begin the FHIR translation pipeline.")

uploaded_file = st.file_uploader(
    label="Select a file",
    type=["csv", "xlsx"],
    label_visibility="collapsed",
    help="Supported formats: CSV, XLSX",
)

# When a new file arrives, update session state and reset to file_ready
if uploaded_file is not None:
    new_name = uploaded_file.name
    new_size = uploaded_file.size

    if (
        new_name != st.session_state.uploaded_file_name
        or st.session_state.stage == "idle"
    ):
        st.session_state.uploaded_file_name = new_name
        st.session_state.uploaded_file_size = new_size
        st.session_state.selected_resource_type = _infer_resource_type(new_name)
        st.session_state.validation_report = None
        st.session_state.stats = None
        st.session_state.error_message = None
        # Allow re-translation if a new file is dropped after a completed run
        if st.session_state.stage == "complete":
            st.session_state.stage = "file_ready"
            st.session_state.bundle_json = None
        elif st.session_state.stage == "idle":
            st.session_state.stage = "file_ready"

# Show file metadata once a file is present
if st.session_state.stage in ("file_ready", "translating", "complete"):
    name = st.session_state.uploaded_file_name
    size = _fmt_size(st.session_state.uploaded_file_size)
    st.info(f"📄 **{name}** — {size}")
    st.selectbox(
        "Target FHIR resource",
        ["Patient", "Practitioner", "Appointment"],
        key="selected_resource_type",
        disabled=(st.session_state.stage == "translating"),
    )

if st.session_state.error_message:
    st.error(st.session_state.error_message)

st.write("")

# ---------------------------------------------------------------------------
# Translate button
# ---------------------------------------------------------------------------
translate_clicked = st.button(
    "Translate",
    type="primary",
    disabled=(st.session_state.stage not in ("file_ready",)),
    use_container_width=True,
)

# ---------------------------------------------------------------------------
# Backend translation (runs synchronously inside this script execution)
# ---------------------------------------------------------------------------
if translate_clicked and st.session_state.stage == "file_ready":
    st.session_state.stage = "translating"
    st.session_state.error_message = None
    st.divider()
    st.subheader("Translation in Progress")

    # Build four persistent placeholder lines
    placeholders = [st.empty() for _ in range(4)]

    # Render all steps as pending before starting
    pending_labels = [
        "Schema Analyzed",
        "FHIR Mapping Generated",
        "Data Transformed",
        "Bundle Validated",
    ]
    for ph, lbl in zip(placeholders, pending_labels):
        ph.markdown(f"🔲 &nbsp; {lbl}")

    time.sleep(0.3)   # brief pause so the user sees the initial state

    try:
        _run_pipeline(
            uploaded_file,
            st.session_state.selected_resource_type,
            placeholders,
        )
    except (requests.RequestException, RuntimeError, KeyError, ValueError) as exc:
        placeholders[0].markdown("❌ &nbsp; **Translation Failed**")
        st.session_state.stage = "file_ready"
        st.session_state.error_message = (
            "The backend translation failed. "
            f"Check that FastAPI is running and try again. Details: {exc}"
        )
        st.error(st.session_state.error_message)
    # After _run_pipeline, stage == "complete"; fall through to output section

# ---------------------------------------------------------------------------
# Status panel — shown after translation completes (on a subsequent rerun)
# ---------------------------------------------------------------------------
if st.session_state.stage == "complete" and not translate_clicked:
    st.divider()
    st.subheader("Translation Complete")
    st.markdown("✅ &nbsp; **Schema Analyzed**")
    st.markdown("✅ &nbsp; **FHIR Mapping Generated**")
    st.markdown("✅ &nbsp; **Data Transformed**")
    st.markdown("✅ &nbsp; **Bundle Validated**")

# ---------------------------------------------------------------------------
# Output section
# ---------------------------------------------------------------------------
if st.session_state.stage == "complete":
    st.divider()
    st.subheader("Output")

    bundle = json.loads(st.session_state.bundle_json)
    stats = st.session_state.stats or {}

    st.success("FHIR Bundle generated")
    st.info(_resource_summary(bundle))
    st.metric("Rows processed", stats.get("rows_processed", 0))
    col_created, col_errors = st.columns(2)
    col_created.metric("Resources created", stats.get("resources_created", 0))
    col_errors.metric("Validation errors", stats.get("error_count", 0))

    with st.expander("View Validation Report", expanded=False):
        st.json(st.session_state.validation_report or {})

    with st.expander("View Bundle", expanded=False):
        st.code(st.session_state.bundle_json, language="json")

    st.download_button(
        label="⬇ Download Bundle",
        data=st.session_state.bundle_json,
        file_name="fhir_bundle.json",
        mime="application/json",
        use_container_width=True,
    )

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.write("")
st.write("")
st.divider()
st.caption("CareCode — EHR Migration Tool | Powered by Claude")
