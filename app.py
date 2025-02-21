import streamlit as st
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
import streamlit as st

st.set_page_config(
    page_title="RIC - Streamlit",
    layout="centered"  # Options: "centered" or "wide"
)


# Load environment variables
load_dotenv()

# Get the ID from URL parameters
query_params = st.query_params
scenario_id = query_params.get("id")

# Ensure scenario_id is valid before querying
if scenario_id:
    try:
        scenario_id = int(scenario_id)  # Convert to integer
    except ValueError:
        st.error("Invalid ID format. Please provide a numeric ID.")
        st.stop()

    # Connect to the database
    db_conn = psycopg2.connect(
        host=os.getenv('PGHOST'),
        database=os.getenv('PGDATABASE'),
        user=os.getenv('PGUSER'),
        password=os.getenv('PGPASSWORD')
    )
    db_cur = db_conn.cursor()

    # Execute the query with the retrieved ID
    db_cur.execute(
        'SELECT scenario_code, input_filename, sku_type, demand FROM public."Scenarios" WHERE id = %s',
        (scenario_id,)
    )
    result = db_cur.fetchone()

    # Check if a result was returned
    if result:
        scenario_code, input_filename, sku_type, demand = result

        # Display the values in Streamlit
        st.write("### Scenario Details")
        st.write("**Scenario Code:**", scenario_code)
        st.write("**Input Filename:**", input_filename)
        st.write("**SKU Type:**", sku_type)
        st.write("**Demand:**", demand)
    else:
        st.error(f"No scenario found with ID {scenario_id}.")

    # Close the database connection
    db_cur.close()
    db_conn.close()