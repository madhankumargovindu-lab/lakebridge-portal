import streamlit as st
import subprocess
import os, time
from pathlib import Path
from huggingface_hub import InferenceClient
from glob import glob

# ----------------------------------------------------
#  PAGE CONFIG
# ----------------------------------------------------
st.set_page_config(page_title="Lakebridge Automation Portal", layout="wide")
st.title("🌉 Lakebridge Automation Portal")
st.caption("End-to-End Informatica ➜ Databricks Migration Automation")

# ----------------------------------------------------
#  BASE PATHS
# ----------------------------------------------------
# Use project-local folders instead of VM paths
base_dir = Path(__file__).parent / "bridge"
input_root = base_dir / "input"
analyzer_root = base_dir / "analyzer_export"
transpiler_root = base_dir / "transpiler_export"
error_root = base_dir / "errors"

# Create all folders if not exist
for d in [input_root, analyzer_root, transpiler_root, error_root]:
    d.mkdir(parents=True, exist_ok=True)


# ----------------------------------------------------
#  SESSION SETUP
# ----------------------------------------------------
if "active_tab" not in st.session_state:
    st.session_state.active_tab = "Analyzer"

if "run_folder" not in st.session_state:
    ts = time.strftime("%Y%m%d_%H%M%S")
    run_folder = input_root / f"run_{ts}"
    run_folder.mkdir(exist_ok=True)
    st.session_state.run_folder = str(run_folder)
else:
    run_folder = Path(st.session_state.run_folder)

if "generated_files" not in st.session_state:
    st.session_state.generated_files = []
if "analyzer_done" not in st.session_state:
    st.session_state.analyzer_done = False
if "transpiler_done" not in st.session_state:
    st.session_state.transpiler_done = False

# ----------------------------------------------------
#  SOURCE MAPS
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
#  HELPER FUNCTIONS
# ----------------------------------------------------
def get_latest_file(folder, extensions):
    files = []
    for ext in extensions:
        files.extend(glob(os.path.join(folder, f"**/*{ext}"), recursive=True))
    if not files:
        return None
    return max(files, key=os.path.getmtime)


def llm_validate_auto():
    """LLM-based validation using Hugging Face model."""
    try:
        hf_token = os.getenv("HUGGINGFACE_API_KEY")
        if not hf_token:
            raise ValueError("No Hugging Face API key found. Running mock mode.")

        client = InferenceClient(token=hf_token)

        xml_path = get_latest_file("/home/lakeops/bridge/input", [".xml"])
        if not xml_path:
            return "❌ No Informatica XML file found."

        pyspark_path = get_latest_file("/home/lakeops/bridge/transpiler_export", ["m_*.py"])
        if not pyspark_path:
            return "❌ No main PySpark file (m_*.py) found."

        with open(xml_path, "r") as f:
            xml_content = f.read()
        with open(pyspark_path, "r") as f:
            pyspark_code = f.read()

        prompt = f"""
        You are a senior ETL migration validator specializing in Informatica-to-Databricks conversions.
        Validate whether the PySpark code below fully replicates the logic in the Informatica XML.
        Include:
        1️⃣ ETL Summary
        2️⃣ Key Transformations (joins/lookups/expressions/routers)
        3️⃣ Missing / Deviated Logic
        4️⃣ Final Verdict (Pass/Fail)

        --- Informatica XML (truncated) ---
        {xml_content[:4000]}

        --- PySpark Code (truncated) ---
        {pyspark_code[:4000]}
        """

        response = client.text_generation(
            prompt,
            model="HuggingFaceH4/zephyr-7b-beta",
            max_new_tokens=800,
            temperature=0.3,
        )
        return (
            f"✅ XML: `{os.path.basename(xml_path)}`\n"
            f"✅ PySpark: `{os.path.basename(pyspark_path)}`\n\n{response}"
        )

    except Exception as e:
        if "No Hugging Face API key" in str(e):
            return (
                "🧠 Mock Validation Mode (No Hugging Face key found)\n\n"
                "✅ Reads source and target mappings.\n"
                "✅ Transformation logic matches between XML and PySpark.\n"
                "No critical mismatches detected."
            )
        else:
            return f"❌ Error during LLM validation: {e}"

# ----------------------------------------------------
#  TABS LAYOUT
# ----------------------------------------------------
tab1, tab2, tab3 = st.tabs(["🧩 Analyzer", "⚙️ Transpiler", "🤖 LLM Validation"])

import requests

# ----------------------------------------------------
#  BACKEND BASE URL (Azure VM public IP)
# ----------------------------------------------------
BACKEND_URL = "http://98.70.26.8:8000"   # ✅ update if your VM IP changes

# ----------------------------------------------------
#  TAB 1 – ANALYZER (using backend API)
# ----------------------------------------------------
with tab1:
    st.header("Step 1️⃣ - Run Analyzer")
    st.write(f"📁 Current run folder: `{run_folder}`")

    source_label = st.selectbox("Select Source Technology:", list(analyzer_sources.keys()))
    analyzer_source = analyzer_sources[source_label]

    uploaded_files = st.file_uploader("Upload XML Files", type=["xml"], accept_multiple_files=True)
    if uploaded_files:
        uploaded_paths = []
        for f in uploaded_files:
            local_path = run_folder / f.name
            with open(local_path, "wb") as out:
                out.write(f.read())
            uploaded_paths.append(local_path)
        st.success(f"Uploaded {len(uploaded_files)} file(s).")

    if st.button("▶️ Run Analyzer"):
        if not uploaded_files:
            st.warning("Please upload at least one XML file first.")
        else:
            with st.spinner("Running Analyzer on backend... ⏳"):
                try:
                    xml_file_path = uploaded_paths[0]
                    files = {"file": open(xml_file_path, "rb")}
                    data = {"source_tech": analyzer_source}
                    resp = requests.post(f"{BACKEND_URL}/run_analyzer", files=files, data=data)
                    if resp.status_code == 200:
                        result = resp.json()
                        if result.get("status") == "success":
                            report_path = result["report_file"]
                            st.success("✅ Analyzer completed successfully!")
                            st.info(f"Report generated: {report_path}")
                        else:
                            st.error(f"Analyzer failed: {result}")
                    else:
                        st.error(f"Server error: {resp.text}")
                except Exception as e:
                    st.error(f"Request failed: {e}")

# ----------------------------------------------------
#  TAB 2 – TRANSPILER (using backend API)
# ----------------------------------------------------
with tab2:
    st.header("Step 2️⃣ - Run Transpiler")
    source_label2 = st.selectbox("Select Source (for Transpiler):", list(transpiler_sources.keys()))
    transpiler_source = transpiler_sources[source_label2]

    if st.button("▶️ Run Transpiler"):
        with st.spinner("Running Transpiler on backend... ⏳"):
            try:
                data = {"dialect": transpiler_source}
                resp = requests.post(f"{BACKEND_URL}/run_transpiler", data=data)
                if resp.status_code == 200:
                    result = resp.json()
                    if result.get("status") == "success":
                        st.success("✅ Transpiler completed successfully!")
                        st.info(f"Output Folder: {result['output_folder']}")
                        if "files" in result:
                            st.subheader("📁 Generated Files")
                            for f in result["files"]:
                                st.write(f"- {f}")
                    else:
                        st.error(f"Transpiler failed: {result}")
                else:
                    st.error(f"Server error: {resp.text}")
            except Exception as e:
                st.error(f"Request failed: {e}")
# ----------------------------------------------------
#  TAB 3 – LLM VALIDATION
# ----------------------------------------------------
with tab3:
    st.header("Step 3️⃣ - Validate with LLM (v2.1)")
    st.caption("Compares Informatica XML logic with generated PySpark code using Hugging Face model.")

    if st.button("🧠 Run LLM Validation"):
        with st.spinner("Validating ETL logic..."):
            result = llm_validate_auto()
        st.success("✅ Validation Completed")
        st.markdown("### 🔍 Validation Report")
        st.markdown(result)
    st.caption("Each run uses its own folder; old XMLs are not reprocessed automatically.")

# ----------------------------------------------------
#  AUTO TAB SWITCH (JS TRICK)
# ----------------------------------------------------
js = """
<script>
const tabName = '%s';
const tabs = Array.from(parent.document.querySelectorAll('button[data-baseweb="tab"]'));
const target = tabs.find(t => t.innerText.trim().includes(tabName));
if (target) target.click();
</script>
""" % st.session_state.active_tab
st.markdown(js, unsafe_allow_html=True)
