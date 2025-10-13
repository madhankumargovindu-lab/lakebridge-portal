import streamlit as st
import os, time, requests
from pathlib import Path
from glob import glob
from huggingface_hub import InferenceClient

# ... (previous code remains same)

BACKEND_URL = "http://98.70.26.8:8000" # 🔗 backend API

tab1, tab2, tab3 = st.tabs(["🧩 Analyzer", "⚙️ Transpiler", "🤖 LLM Validation"])

# ----------------------------------------------------
# TAB 1 – ANALYZER
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
                            # ⬇️ Download button
                            download_url = f"{BACKEND_URL}/download_file?filepath={res['report_file']}"
                            st.markdown(f"[⬇️ Download Analyzer Report]({download_url})")
                        else:
                            st.error(res.get("message"))
                    else:
                        st.error(f"Server error: {r.text}")
                except Exception as e:
                    st.error(f"Request failed: {e}")

# ----------------------------------------------------
# TAB 2 – TRANSPILER
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
                                download_url = f"{BACKEND_URL}/download_file?filepath={res['output_folder']}/{f}"
                                st.markdown(f"[⬇️ {f}]({download_url})")
                    else:
                        st.error(res.get("message"))
                else:
                    st.error(f"Server error: {r.text}")
            except Exception as e:
                st.error(f"Request failed: {e}")
