import streamlit as st
import pandas as pd
import os

query_params = st.query_params
scenarioId = query_params.get("id")

if scenarioId is None:
    st.write("Scenario ID is not provided.")
    st.stop()

# Make a parent directory that contains this REPO and RIC-BGO-Tool
file_path = r"../RIC-BGO-Tool/excelFiles/solved/" + scenarioId + ".xlsx"

if os.path.exists(file_path):
    xlsx = pd.ExcelFile(file_path)
    scenario_code = pd.read_excel(xlsx, "Model", index_col=None, usecols="B", header=3, nrows=0)
    st.write("Scenario code is: ")
    st.write(scenario_code)
else:
    st.write("File not found: " + file_path)