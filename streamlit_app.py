import shutil
import streamlit as st
import numpy as np
import pandas as pd
import tempfile
from wisio import init_with_hydra

st.image("assets/logo.png", width=200)
st.title("Welcome to WisIO Live")
st.markdown(
    """
    Analyze, visualize, and understand I/O performance issues in HPC workloads.
    """
)

trace_files = st.file_uploader(
    "Upload a trace file",
    type=["darshan", "parquet", "pfw", "pfw.gz"],
    accept_multiple_files=True,
)

if st.button("Analyze", disabled=not trace_files):
    # Check if all trace files have the same type
    if not trace_files or len(trace_files) == 0:
        st.error("Please upload at least one trace file.")
        st.stop()
    if len(set(file.name.split('.')[-1] for file in trace_files)) > 1:
        st.error("All trace files must be of the same type.")
        st.stop()

    analyzer = 'darshan'
    if all(file.name.endswith('.parquet') for file in trace_files):
        analyzer = 'recorder'
    elif all(file.name.endswith('.pfw') or file.name.endswith('.pfw.gz') for file in trace_files):
        analyzer = 'dftracer'

    with st.status("Analyzing trace files", expanded=True) as status:
        st.write(f"Detected analyzer type: {analyzer.title()}")

        with tempfile.TemporaryDirectory() as temp_dir:
            st.write(f"Using temporary directory: {temp_dir}")

            for trace_file in trace_files:
                with open(f"{temp_dir}/{trace_file.name}", "wb") as temp_trace_file:
                    temp_trace_file.write(trace_file.getbuffer())

            wis = init_with_hydra(
                hydra_overrides=[
                    f"+analyzer={analyzer}",
                    f"analyzer.bottleneck_dir={temp_dir}",
                    f"analyzer.checkpoint={False}",
                    f"analyzer.time_granularity={5e6}",
                    f"hydra.run.dir={temp_dir}",
                    f"hydra.runtime.output_dir={temp_dir}",
                    f"logical_view_types={False}",
                    f"output.compact={True}",
                    f"output.group_behavior={True}",
                    f"output.max_bottlenecks={2}",
                    f"output.root_only={True}",
                    f"percentile={0.95}",
                    f"trace_path={temp_dir}",
                ]
            )
            st.write("Initialized WisIO analyzer.")

            st.write("Analyzing trace files...")
            result = wis.analyze_trace()
            st.write("Analysis complete.")

            try:
                st.write("Shutting down analyzer...")
                wis.client.close()
                wis.cluster.close()
                st.write("Analyzer shut down.")
            except Exception as e:
                st.error(f"Error shutting down analyzer: {e}")
                st.write("Please restart the application.")

            st.write("Cleaning up temporary directory...")
            shutil.rmtree(temp_dir, ignore_errors=True)
            st.write("Temporary directory cleaned up.")

            status.update(label="Analysis complete.", expanded=False, state="complete")

    st.subheader("Analysis Results")

    st.write(len(result.evaluated_views), "views evaluated.")

    