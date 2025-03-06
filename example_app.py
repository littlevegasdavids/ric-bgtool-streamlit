import streamlit as st
import psycopg2
import os
from dotenv import load_dotenv

st.set_page_config(
    page_title="RIC - Streamlit",
    layout="wide"  # Wide layout for better comparison view
)

# Load environment variables
load_dotenv()

# Connect to the database
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv('PGHOST'),
        database=os.getenv('PGDATABASE'),
        user=os.getenv('PGUSER'),
        password=os.getenv('PGPASSWORD')
    )

# Fetch available scenarios for dropdowns, sorted by ID descending
def fetch_scenario_list():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT id, scenario_code FROM public."Scenarios" WHERE scenario_status = 4 ORDER BY id DESC')  # Sorting by ID DESC
    scenarios = cur.fetchall()
    conn.close()
    return {str(id_): f"{id_}-{scenario_code}" for id_, scenario_code in scenarios}

# Fetch scenario details based on ID
def fetch_scenario_details(scenario_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        'SELECT scenario_code, input_filename, sku_type, demand FROM public."Scenarios" WHERE id = %s',
        (scenario_id,)
    )
    result = cur.fetchone()
    conn.close()
    return result

# Get scenarios list for dropdowns (sorted by ID DESC)
scenarios_dict = fetch_scenario_list()
scenario_ids = list(scenarios_dict.keys())  # Extract ordered scenario IDs

# Get scenario ID from URL parameters
query_params = st.query_params
scenario_id_from_url = query_params.get("id")

# Ensure valid scenario ID
default_scenario_id = scenario_id_from_url if scenario_id_from_url in scenarios_dict else scenario_ids[0]

# Sidebar section for scenario selection
st.sidebar.write("### Select Scenario")
scenario_1 = st.sidebar.selectbox("Scenario 1", options=scenario_ids, index=scenario_ids.index(default_scenario_id), format_func=lambda x: scenarios_dict[x])

# Retrieve first scenario details
scenario_1_details = fetch_scenario_details(scenario_1) if scenario_1 else None

# Button to enable second scenario comparison
if "show_comparison" not in st.session_state:
    st.session_state.show_comparison = False

if st.sidebar.button("➕ Add Comparison"):
    st.session_state.show_comparison = True

# Display scenarios
if not st.session_state.show_comparison:
    if scenario_1_details:
        st.write("### Scenario Details")
        st.write(f"**Scenario Code:** {scenario_1_details[0]}")
        st.write(f"**Input Filename:** {scenario_1_details[1]}")
        st.write(f"**SKU Type:** {scenario_1_details[2]}")
        st.write(f"**Demand:** {scenario_1_details[3]}")
else:
    # Show both scenarios side-by-side
    col1, col2 = st.columns(2)

    with col1:
        st.write("### Scenario 1 Details")
        if scenario_1_details:
            st.write(f"**Scenario Code:** {scenario_1_details[0]}")
            st.write(f"**Input Filename:** {scenario_1_details[1]}")
            st.write(f"**SKU Type:** {scenario_1_details[2]}")
            st.write(f"**Demand:** {scenario_1_details[3]}")

    with col2:
        st.sidebar.write("### Select Second Scenario")
        scenario_2 = st.sidebar.selectbox("Scenario 2", options=scenario_ids, format_func=lambda x: scenarios_dict[x])

        scenario_2_details = fetch_scenario_details(scenario_2) if scenario_2 else None

        if scenario_2_details:
            st.write("### Scenario 2 Details")
            st.write(f"**Scenario Code:** {scenario_2_details[0]}")
            st.write(f"**Input Filename:** {scenario_2_details[1]}")
            st.write(f"**SKU Type:** {scenario_2_details[2]}")
            st.write(f"**Demand:** {scenario_2_details[3]}")
