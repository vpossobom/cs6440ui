"""
app.py
EHR → FHIR Translation Tool — Provider-facing Streamlit UI.

Simulates a LangGraph agent pipeline that:
  1. Analyzes the uploaded EHR file schema
  2. Generates a FHIR mapping using an LLM
  3. Transforms the data
  4. Validates and packages the FHIR bundle

No real backend is required — each pipeline step is mocked with realistic
delays and a pre-built FHIR R4 Bundle is returned as the final output.
"""

import time
import streamlit as st

from mock_bundle import get_mock_bundle_json

# ---------------------------------------------------------------------------
# Page configuration — must be the first Streamlit call
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="EHR → FHIR Translation Tool",
    page_icon="🏥",
    layout="centered",
    initial_sidebar_state="collapsed",
)

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


def _run_pipeline(step_placeholders: list) -> None:
    """
    Simulate the four-step agent pipeline.
    Each step renders a spinner while running, then a green checkmark on
    completion. Uses st.empty() placeholders so only the current line updates.
    """
    steps = [
        "Schema Analyzed",
        "FHIR Mapping Generated",
        "Data Transformed",
        "Bundle Validated",
    ]

    for i, label in enumerate(steps):
        # Mark current step as running
        step_placeholders[i].markdown(f"⏳ &nbsp; **{label}** — *running…*")

        # Simulate work — vary delays slightly for realism
        delay = 1.5 if i % 2 == 0 else 2.0
        time.sleep(delay)

        # Mark current step as done
        step_placeholders[i].markdown(f"✅ &nbsp; **{label}**")

    # Store the generated bundle and advance stage
    st.session_state.bundle_json = get_mock_bundle_json()
    st.session_state.stage = "complete"


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
    type=["csv", "json", "txt"],
    label_visibility="collapsed",
    help="Supported formats: CSV, JSON, TXT",
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
# Pipeline simulation (runs synchronously inside this script execution)
# ---------------------------------------------------------------------------
if translate_clicked and st.session_state.stage == "file_ready":
    st.session_state.stage = "translating"
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

    _run_pipeline(placeholders)
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

    st.success("Bundle saved to S3")
    st.info(
        "47 FHIR resources generated across 3 resource types  "
        "(**Patient**, **Observation**, **Condition**)"
    )

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
