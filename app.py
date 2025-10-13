import streamlit as st
import os
import time
import requests
from pathlib import Path
from glob import glob
from huggingface_hub import InferenceClient

# ----------------------------------------------------
# PAGE CONFIG
# ----------------------------------------------------
st.set_page_config(page_title="Lakebridge Automation Portal", layout="wide")
st.title("🌉 Lakebridge Automation Portal")
st.caption("End-to-End Informatica ➜ Databricks Migration (Analyzer • Transpiler • LLM Validation)")

# ----------------------------------------------------
# BACKEND CONFIG (FastAPI on Azure VM)
# ----------------------------------------------------
# You can override via Streamlit secrets or env var on Streamlit Cloud:
# st.secrets["BACKEND_URL"] OR os.environ["BACKEND_URL"]
BACKEND_URL = (
    st.secrets.get("BACKEND_URL")
    if hasattr(st, "secrets") and "BACKEND_URL" in st.secrets
    else os.getenv("BACKEND_URL", "http://98.70.26.8:8000")
)

# Small backend health badge
def render_backend_status():
    col1, col2 = st.columns([1, 4])
    with col1:
        st.write("🔌 Backend:")
    with col2:
        try:
            r = requests.get(f"{BACKEND_URL}/", timeout=5)
            if r.status_code == 200:
                st.success(f"Online – {BACKEND_URL}")
            else:
                st.warning(f"Unhealthy (HTTP {r.status_code}) – {BACKEND_URL}")
        except Exception as e:
            st.error(f"Offline – {BACKEND_URL} — {e}")

render_backend_status()

# ----------------------------------------------------
# SESSION + LOCAL PROJECT FOLDERS (for Streamlit Cloud safe temp use)
# ----------------------------------------------------
base_dir = Path(__file__).parent / "bridge" # local project folder
input_root = base_dir / "input"
tmp_root = base_dir / "tmp"
for d in [input_root, tmp_root]:
    d.mkdir(parents=True, exist_ok=True)

# session keys
if "uploaded_analyzer_paths" not in st.session_state:
    st.session_state.uploaded_analyzer_paths = []
if "last_analyzer_report" not in st.session_state:
    st.session_state.last_analyzer_report = None
if "last_transpiler_output" not in st.session_state:
    st.session_state.last_transpiler_output = None

# ----------------------------------------------------
# SOURCE MAPS (Analyzer vs Transpiler naming)
# ----------------------------------------------------
analyzer_sources = {
    "Informatica PowerCenter": "Informatica - PC",
    "Informatica Cloud": "Informatica Cloud",
    "Azure Data Factory (ADF)": "ADF",
    "IBM DataStage": "Datastage",
    "MS SQL Server": "MS SQL Server",
    "Oracle": "Oracle",
}

transpiler_sources = {
    "Informatica PowerCenter": "informatica (desktop edition)",
    "Informatica Cloud": "informatica cloud",
    "Azure Data Factory (ADF)": "synapse",
    "IBM DataStage": "datastage",
    "MS SQL Server": "mssql",
    "Oracle": "oracle",
}

# ----------------------------------------------------
# LLM VALIDATION (uses files uploaded to Streamlit app)
# ----------------------------------------------------
def llm_validate(xml_text: str, pyspark_text: str) -> str:
    """Run Hugging Face validation if token set; otherwise mock result."""
    try:
        hf_token = os.getenv("HUGGINGFACE_API_KEY")
        if not hf_token:
            raise ValueError("No Hugging Face API key found. Running mock mode.")
        client = InferenceClient(token=hf_token)

        prompt = f"""
You are a senior ETL migration validator specializing in Informatica-to-Databricks conversions.
Validate whether the PySpark code below fully replicates the logic in the Informatica XML.

Compare:
• Source & Target mapping alignment
• Transformations (lookup, expression, router, filters, joins)
• Load strategy (insert/update/merge)
• Parameter & variable usage

Identify any missing or mismatched logic and summarize in markdown.

--- Informatica XML (truncated) ---
{xml_text[:4000]}

--- PySpark Code (truncated) ---
{pyspark_text[:4000]}

Return sections:
1️⃣ ETL Summary
2️⃣ Key Matching Transformations
3️⃣ Missing / Deviated Logic
4️⃣ Final Verdict (Pass/Fail)
"""
        response = client.text_generation(
            prompt,
            model="HuggingFaceH4/zephyr-7b-beta",
            max_new_tokens=800,
            temperature=0.3,
        )
        return response

    except Exception as e:
        if "No Hugging Face API key" in str(e):
            return (
                "🧠 Mock Validation Mode (no HF token)\n\n"
                "✅ ETL Summary: The job reads source(s), applies transformations, and loads target(s).\n"
                "✅ Matching: Key logic appears to align.\n"
                "No critical mismatches detected.\n"
                "⚠️ Set HUGGINGFACE_API_KEY for real validation."
            )
        return f"❌ Error during LLM validation: {e}"

# ----------------------------------------------------
# UI TABS
# ----------------------------------------------------
tab1, tab2, tab3 = st.tabs(["🧩 Analyzer", "⚙️ Transpiler", "🤖 LLM Validation"])

# ====================================================
# TAB 1 – ANALYZER
# ====================================================
with tab1:
    st.header("Step 1️⃣ – Run Analyzer (via VM backend)")
    st.caption("Uploads XML to the VM and runs `databricks labs lakebridge analyze` there.")

    source_label = st.selectbox("Source Technology:", list(analyzer_sources.keys()))
    analyzer_source = analyzer_sources[source_label]

    uploaded = st.file_uploader("Upload XML file(s) for Analyzer", type=["xml"], accept_multiple_files=True)
    if uploaded:
        st.session_state.uploaded_analyzer_paths = []
        for f in uploaded:
            p = input_root / f.name
            with open(p, "wb") as out:
                out.write(f.read())
            st.session_state.uploaded_analyzer_paths.append(p)
        st.success(f"Uploaded {len(uploaded)} file(s) to session.")

    if st.button("▶️ Run Analyzer on VM"):
        if not st.session_state.uploaded_analyzer_paths:
            st.warning("Please upload at least one XML file first.")
        else:
            with st.spinner("Uploading XML & running Analyzer on backend..."):
                try:
                    # (Current backend accepts 1 file per call; use the first)
                    xml_path = st.session_state.uploaded_analyzer_paths[0]
                    files = {"file": open(xml_path, "rb")}
                    data = {"source_tech": analyzer_source}
                    r = requests.post(f"{BACKEND_URL}/run_analyzer", files=files, data=data, timeout=300)
                    if r.status_code == 200:
                        res = r.json()
                        if res.get("status") == "success":
                            st.success("✅ Analyzer completed successfully!")
                            st.session_state.last_analyzer_report = res["report_file"]
                            st.info(f"Report: {res['report_file']}")
                            # Download link directly from VM
                            dl_url = f"{BACKEND_URL}/download_file?filepath={res['report_file']}"
                            st.markdown(f"[⬇️ Download Analyzer Report]({dl_url})")
                        else:
                            st.error(res.get("message", "Analyzer failed"))
                    else:
                        st.error(f"Server error: {r.text}")
                except Exception as e:
                    st.error(f"Request failed: {e}")

# ====================================================
# TAB 2 – TRANSPILER
# ====================================================
with tab2:
    st.header("Step 2️⃣ – Run Transpiler (via VM backend)")
    st.caption("Runs `databricks labs lakebridge transpile` on the VM and returns generated files.")

    source_label2 = st.selectbox("Source (Transpiler):", list(transpiler_sources.keys()))
    transpiler_source = transpiler_sources[source_label2]

    run_mode = st.radio(
        "Choose input for Transpiler:",
        ["Use last Analyzer upload", "Upload a new XML here"]
    )

    new_xml = None
    new_xml_path = None
    if run_mode == "Upload a new XML here":
        new_xml = st.file_uploader("Upload XML for Transpiler", type=["xml"], accept_multiple_files=False)
        if new_xml:
            new_xml_path = tmp_root / new_xml.name
            with open(new_xml_path, "wb") as out:
                out.write(new_xml.read())
            st.success(f"Uploaded: {new_xml.name}")

    if st.button("▶️ Run Transpiler on VM"):
        with st.spinner("Running Transpiler on backend..."):
            try:
                if run_mode == "Upload a new XML here":
                    if not new_xml_path:
                        st.warning("Please upload an XML first.")
                        st.stop()
                    files = {"file": open(new_xml_path, "rb")}
                    data = {"dialect": transpiler_source}
                    r = requests.post(f"{BACKEND_URL}/run_transpiler", data=data, files=files, timeout=600)
                else:
                    # no file => backend uses latest run_* under /home/lakeops/bridge/input
                    data = {"dialect": transpiler_source}
                    r = requests.post(f"{BACKEND_URL}/run_transpiler", data=data, timeout=600)

                if r.status_code == 200:
                    res = r.json()
                    if res.get("status") == "success":
                        st.success("✅ Transpiler completed successfully!")
                        st.session_state.last_transpiler_output = res["output_folder"]
                        st.info(f"Output folder: {res['output_folder']}")
                        files = res.get("files", [])
                        if files:
                            st.subheader("📁 Generated Files")
                            for fname in files:
                                dl_url = f"{BACKEND_URL}/download_file?filepath={res['output_folder']}/{fname}"
                                st.markdown(f"[⬇️ {fname}]({dl_url})")
                        else:
                            st.info("No files returned by backend.")
                    else:
                        st.error(res.get("message", "Transpiler failed"))
                else:
                    st.error(f"Server error: {r.text}")
            except Exception as e:
                st.error(f"Request failed: {e}")

# ====================================================
# TAB 3 – LLM VALIDATION (LOCAL in Streamlit)
# ====================================================
with tab3:
    st.header("Step 3️⃣ – Validate with LLM (optional)")
    st.caption("Compares Informatica XML logic with generated PySpark code. Runs fully in Streamlit Cloud.")

    colA, colB = st.columns(2)
    with colA:
        xml_file = st.file_uploader("Upload Informatica XML for validation", type=["xml"], accept_multiple_files=False)
    with colB:
        pyspark_file = st.file_uploader("Upload generated PySpark file for validation", type=["py"], accept_multiple_files=False)

    if st.button("🧠 Run LLM Validation"):
        if not xml_file or not pyspark_file:
            st.warning("Please upload both the XML and the PySpark file.")
        else:
            xml_text = xml_file.read().decode("utf-8", errors="ignore")
            pyspark_text = pyspark_file.read().decode("utf-8", errors="ignore")
            with st.spinner("Analyzing with LLM..."):
                result = llm_validate(xml_text, pyspark_text)
            st.success("✅ Validation Completed")
            st.markdown("### 🔍 Validation Report")
            st.markdown(result)

st.caption("Tip: Set HUGGINGFACE_API_KEY (Streamlit Secrets) to enable real LLM validation.")
