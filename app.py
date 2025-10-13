import streamlit as st
import os, time, requests
from pathlib import Path
from glob import glob
from huggingface_hub import InferenceClient

# ----------------------------------------------------
#  PAGE CONFIG
# ----------------------------------------------------
st.set_page_config(page_title="Lakebridge Automation Portal", layout="wide")
st.title("🌉 Lakebridge Automation Portal")
st.caption("End-to-End Informatica ➜ Databricks Migration Automation")

# ----------------------------------------------------
#  FOLDER SETUP (local Streamlit side)
# ----------------------------------------------------
base_dir = Path(__file__).parent / "bridge"
input_root = base_dir / "input"
analyzer_root = base_dir / "analyzer_export"
transpiler_root = base_dir / "transpiler_export"
error_root = base_dir / "errors"
for d in [input_root, analyzer_root, transpiler_root, error_root]:
    d.mkdir(parents=True, exist_ok=True)

# ----------------------------------------------------
#  SESSION STATE
# ----------------------------------------------------
if "active_tab" not in st.session_state:
    st.session_state.active_tab = "Analyzer"
if "run_folder" not in st.session_state:
    ts = time.strftime("%Y%m%d_%H%M%S")
    rf = input_root / f"run_{ts}"
    rf.mkdir(exist_ok=True)
    st.session_state.run_folder = str(rf)
run_folder = Path(st.session_state.run_folder)

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
#  BACKEND URL
# ----------------------------------------------------
BACKEND_URL = "http://98.70.26.8:8000"   # change if VM IP changes

# ----------------------------------------------------
#  TABS LAYOUT
# ----------------------------------------------------
tab1, tab2, tab3 = st.tabs(["🧩 Analyzer", "⚙️ Transpiler", "🤖 LLM Validation"])

# ----------------------------------------------------
#  TAB 1 – ANALYZER
# ----------------------------------------------------
with tab1:
    st.header("Step 1️⃣ – Run Analyzer")
    source_label = st.selectbox("Select Source Technology:", list(analyzer_sources.keys()))
    analyzer_source = analyzer_sources[source_label]

    uploaded_files = st.file_uploader("Upload XML Files", type=["xml"], accept_multiple_files=True)
    if uploaded_files:
        uploaded_paths = []
        for f in uploaded_files:
            p = run_folder / f.name
            with open(p, "wb") as out:
                out.write(f.read())
            uploaded_paths.append(p)
        st.success(f"Uploaded {len(uploaded_files)} file(s).")

    if st.button("▶️ Run Analyzer"):
        if not uploaded_files:
            st.warning("Please upload at least one XML file first.")
        else:
            with st.spinner("Running Analyzer on backend..."):
                try:
                    xml_path = uploaded_paths[0]
                    files = {"file": open(xml_path, "rb")}
                    data = {"source_tech": analyzer_source}
                    r = requests.post(f"{BACKEND_URL}/run_analyzer", files=files, data=data)
                    if r.status_code == 200:
                        res = r.json()
                        if res.get("status") == "success":
                            st.success("✅ Analyzer completed successfully!")
                            st.info(f"Report generated: {res['report_file']}")
                        else:
                            st.error(res.get("message"))
                    else:
                        st.error(f"Server error: {r.text}")
                except Exception as e:
                    st.error(f"Request failed: {e}")

# ----------------------------------------------------
#  TAB 2 – TRANSPILER
# ----------------------------------------------------
with tab2:
    st.header("Step 2️⃣ – Run Transpiler")
    source_label2 = st.selectbox("Select Source (for Transpiler):", list(transpiler_sources.keys()))
    transpiler_source = transpiler_sources[source_label2]

    if st.button("▶️ Run Transpiler"):
        with st.spinner("Running Transpiler on backend..."):
            try:
                data = {"dialect": transpiler_source}
                r = requests.post(f"{BACKEND_URL}/run_transpiler", data=data)
                if r.status_code == 200:
                    res = r.json()
                    if res.get("status") == "success":
                        st.success("✅ Transpiler completed successfully!")
                        st.info(f"Output Folder: {res['output_folder']}")
                        if "files" in res:
                            st.subheader("📁 Generated Files")
                            for f in res["files"]:
                                st.write(f"• {f}")
                    else:
                        st.error(res.get("message"))
                else:
                    st.error(f"Server error: {r.text}")
            except Exception as e:
                st.error(f"Request failed: {e}")

# ----------------------------------------------------
#  TAB 3 – LLM VALIDATION
# ----------------------------------------------------
def get_latest_file(folder, exts):
    files = []
    for e in exts:
        files.extend(glob(os.path.join(folder, f"**/*{e}"), recursive=True))
    return max(files, key=os.path.getmtime) if files else None

def llm_validate_auto():
    try:
        hf_token = os.getenv("HUGGINGFACE_API_KEY")
        if not hf_token:
            raise ValueError("No Hugging Face API key found – running mock mode.")
        client = InferenceClient(token=hf_token)
        xml = get_latest_file(str(input_root), [".xml"])
        pyspark = get_latest_file(str(transpiler_root), ["m_*.py"])
        if not xml or not pyspark:
            return "❌ Missing XML or PySpark file."
        with open(xml) as f1, open(pyspark) as f2:
            x, p = f1.read(), f2.read()
        prompt = f"""
        You are an ETL migration validator.
        Compare the Informatica XML and PySpark code.
        Summarize and state Pass/Fail.
        --- XML ---
        {x[:4000]}
        --- PySpark ---
        {p[:4000]}
        """
        res = client.text_generation(prompt, model="HuggingFaceH4/zephyr-7b-beta",
                                     max_new_tokens=800, temperature=0.3)
        return res
    except Exception as e:
        return f"🧠 Mock Validation: {e}"

with tab3:
    st.header("Step 3️⃣ – Validate with LLM (v2.1)")
    if st.button("🧠 Run LLM Validation"):
        with st.spinner("Validating ETL logic..."):
            r = llm_validate_auto()
        st.success("✅ Validation Completed")
        st.markdown("### 🔍 Validation Report")
        st.markdown(r)

# ----------------------------------------------------
#  AUTO TAB SWITCH
# ----------------------------------------------------
js = f"""
<script>
const tabName = '{st.session_state.active_tab}';
const tabs = Array.from(parent.document.querySelectorAll('button[data-baseweb="tab"]'));
const target = tabs.find(t => t.innerText.trim().includes(tabName));
if (target) target.click();
</script>
"""
st.markdown(js, unsafe_allow_html=True)
