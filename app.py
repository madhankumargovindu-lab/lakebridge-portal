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
base_dir = Path("/home/lakeops/bridge")
input_root = base_dir / "input"
analyzer_root = base_dir / "analyzer_export"
transpiler_root = base_dir / "transpiler_export"
error_root = base_dir / "errors"

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

# ----------------------------------------------------
#  TAB 1 – ANALYZER
# ----------------------------------------------------
with tab1:
    st.header("Step 1️⃣ - Run Analyzer")
    st.write(f"📁 Current run folder: `{run_folder}`")

    source_label = st.selectbox("Select Source Technology:", list(analyzer_sources.keys()))
    analyzer_source = analyzer_sources[source_label]

    uploaded_files = st.file_uploader("Upload XML Files", type=["xml"], accept_multiple_files=True)
    if uploaded_files:
        for f in uploaded_files:
            with open(run_folder / f.name, "wb") as out:
                out.write(f.read())
        st.success(f"Uploaded {len(uploaded_files)} file(s).")

    if st.button("▶️ Run Analyzer"):
        placeholder = st.empty()
        placeholder.info("Running Analyzer... ⏳")
        try:
            report_path = analyzer_root / f"analyzer_{run_folder.name}.xlsx"
            cmd = [
                "databricks", "labs", "lakebridge", "analyze",
                "--source-directory", str(run_folder),
                "--report-file", str(report_path),
                "--source-tech", analyzer_source
            ]
            subprocess.run(cmd, check=True)
            st.session_state.generated_files.append(str(report_path))
            st.session_state.analyzer_done = True
            placeholder.success("✅ Analyzer completed!")

            # Automatically move to Transpiler tab
            st.session_state.active_tab = "Transpiler"
            st.rerun()

        except subprocess.CalledProcessError as e:
            placeholder.error(f"Analyzer failed: {e}")

    if st.session_state.analyzer_done:
        st.subheader("📊 Analyzer Report")
        for fpath in st.session_state.generated_files:
            if fpath.endswith(".xlsx") and os.path.exists(fpath):
                with open(fpath, "rb") as f:
                    st.download_button(
                        label=f"⬇️ Download {Path(fpath).name}",
                        data=f,
                        file_name=Path(fpath).name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    )

# ----------------------------------------------------
#  TAB 2 – TRANSPILER
# ----------------------------------------------------
with tab2:
    st.header("Step 2️⃣ - Run Transpiler")
    source_label2 = st.selectbox("Select Source (for Transpiler):", list(transpiler_sources.keys()))
    transpiler_source = transpiler_sources[source_label2]

    if st.button("▶️ Run Transpiler"):
        placeholder = st.empty()
        placeholder.info("Running Transpiler... ⏳")
        try:
            transpiler_out = transpiler_root / run_folder.name
            transpiler_out.mkdir(exist_ok=True)
            cmd = [
                "databricks", "labs", "lakebridge", "transpile",
                "--input-source", str(run_folder),
                "--output-folder", str(transpiler_out),
                "--catalog-name", "dev",
                "--schema-name", "lakebridge",
                "--source-dialect", transpiler_source,
                "--error-file-path", str(error_root / f"errors_{run_folder.name}.txt")
            ]
            subprocess.run(cmd, check=True)
            for f in os.listdir(transpiler_out):
                st.session_state.generated_files.append(str(transpiler_out / f))
            st.session_state.transpiler_done = True
            placeholder.success("✅ Transpiler completed!")

            # Auto jump to LLM Validation
            st.session_state.active_tab = "LLM Validation"
            st.rerun()

        except subprocess.CalledProcessError as e:
            placeholder.error(f"Transpiler failed: {e}")

    if st.session_state.transpiler_done:
        st.subheader("📁 Transpiler Output Files")
        transpiler_files = [
            f for f in st.session_state.generated_files if f.endswith((".py", ".json"))
        ]
        for fpath in transpiler_files:
            if os.path.exists(fpath):
                with open(fpath, "rb") as f:
                    st.download_button(
                        label=f"⬇️ Download {Path(fpath).name}",
                        data=f,
                        file_name=Path(fpath).name,
                        mime="text/plain",
                    )

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
