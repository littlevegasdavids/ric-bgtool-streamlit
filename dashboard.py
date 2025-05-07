# <editor-fold desc="Imports">
import streamlit as st
import plotly.express as px
import pandas as pd
import numpy as np
import os
import sys
import warnings
import pgeocode
import folium
from streamlit_folium import st_folium
import plotly.express as px
import plotly.graph_objects as go
import pydeck as pdk
from numerize import numerize
import ast
import sqlite3
from dotenv import load_dotenv
import psycopg2
# </editor-fold>

# <editor-fold desc="Setup Variables">
#filePath = r"C:\Users\AngeloGuerra\OneDrive - Rieger Industrial Consultants CC\SP_Link\RIC-530 MAB BGO Licensing\Tool Upgrades\Web Dashboards"
stylePath = r'style.css'
ricLogoPath = "https://www.ricgroup.net/wp-content/uploads/sites/1122/2020/02/RIC_logo_800px.png"
mabLogoPath = "https://markanthony.com/wp-content/uploads/mark-anthony-group-logo.png"
# </editor-fold>

# <editor-fold desc="DB Connection and Excel File">
load_dotenv()
dbConnection = psycopg2.connect(
    host=os.getenv('PGHOST'),
    database=os.getenv('PGDATABASE'),
    user=os.getenv('PGUSER'),
    password=os.getenv('PGPASSWORD')
)
db_cur = dbConnection.cursor()

query_params = st.query_params
scenario_id_from_url = query_params.get("id") #Getting Scenario ID from URL parameters
#scenario_id_from_url = 1618

if scenario_id_from_url is None:
    st.write("Scenario ID is not provided")
    st.stop()

solvedSc_file_path = r"../RIC-BGO-Tool/excelFiles/solved/" + scenario_id_from_url + ".xlsx"

if os.path.exists(solvedSc_file_path):
    solvedSc_excel_file = pd.ExcelFile(solvedSc_file_path)
else:
    st.write("File path not found: " + solvedSc_file_path)
# </editor-fold>

# <editor-fold desc="Configuration">
warnings.filterwarnings('ignore')
st.set_page_config(page_title="BGO Dashboard", page_icon=":chart_with_upwards_trend:", layout="wide")
# </editor-fold>

# <editor-fold desc="Import CSS Styles and Create Page Title">
with open(stylePath) as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
st.html(f'<div class= "pageTitle"><img class="logo" align ="left" src={ricLogoPath}>BGO Scenario Dashboard<img class="logo" align="right" src={mabLogoPath}></div')
st.divider()
# </editor-fold>

# <editor-fold desc="Import from DB">
#df_dbTest = pd.read_sql_query('SELECT * FROM public."Users" WHERE role_id = %s;', dbConnection, params=[1])
df_CmMaster = pd.read_excel(solvedSc_excel_file, sheet_name='CmMaster')
df_ZipMaster = pd.read_excel(solvedSc_excel_file, sheet_name='ZIPMaster')
df_ZipMaster.drop_duplicates(subset='BGO Code', keep='first', inplace=True)
df_Model = pd.read_excel(solvedSc_excel_file, sheet_name='Model')
df_Summary = pd.read_excel(solvedSc_excel_file, sheet_name='Summary')
# </editor-fold>

# <editor-fold desc="Get Scenario Information">
ScCode = df_Model.loc[df_Model['H1'] == 'Scenario Code', 'H2'].values[0]
ScDescription = df_Model.loc[df_Model['H1'] == 'Scenario Description', 'H2'].values[0]
ScGrp = "Group-Level Run" if df_Model.loc[df_Model['H1'] == 'Run Model on SKU Grp Level', 'H2'].values[0] == "Yes" else "SKU Level Run"

exPd = eval(df_Model.loc[df_Model['H1'] == 'Excluded Sites: Pd Site', 'H2'].values[0])
exPk = eval(df_Model.loc[df_Model['H1'] == 'Excluded Sites: Pk Site', 'H2'].values[0])
exRpk = eval(df_Model.loc[df_Model['H1'] == 'Excluded Sites: rPk Site', 'H2'].values[0])
exWip = eval(df_Model.loc[df_Model['H1'] == 'Excluded Sites: WIP Site', 'H2'].values[0])
exFg = eval(df_Model.loc[df_Model['H1'] == 'Excluded Sites: FG Site', 'H2'].values[0])
# </editor-fold>

def main_page():
    # <editor-fold desc="Show Scenario Info on Cards">
    colCode, colDescription, colGrp = st.columns(3)
    colCode.html(f'<div class="infoCard"><p class="infoCardHeading">Scenario Code</p><p class="infoCardText">{ScCode}</p></div>')
    colDescription.html(f'<div class="infoCard"><p class="infoCardHeading">Scenario Description</p><p class="infoCardText">{ScDescription}</p></div>')
    colGrp.html(f'<div class="infoCard"><p class="infoCardHeading">Run Type</p><p class="infoCardText">{ScGrp}</p></div>')
    # </editor-fold>
    # <editor-fold desc="Show Excl. Sites in a Table">
    exclSiteData = {
        "Pd": exPd,
        "Pk": exPk,
        "rPk": exRpk,
        "WIP": exWip,
        "FG": exFg
    }
    exclSiteDf = pd.DataFrame(dict([(key, pd.Series(value)) for key, value in exclSiteData.items()]))
    st.subheader("Excluded Sites:")
    st.table(exclSiteDf)
    # </editor-fold>
    # <editor-fold desc="Get Summary Stats/KPIs">
    df_Summary_Pens = df_Summary.loc[(df_Summary['H1'] == 'Cost') & (df_Summary['H3'] == 'Pk') & (df_Summary['H5'] == 'Total')]
    df_Summary_Utilization = df_Summary.loc[(df_Summary['H1'] == 'Hours') & (df_Summary['H5'] == 'Total') & (df_Summary['Total'] > 0)]
    df_Summary_Utilization = df_Summary_Utilization.drop(['Code', 'H1', 'H2','H5', 'UOM', 'Total', 'Report Total'], axis=1)
    df_Summary_Utilization = pd.melt(df_Summary_Utilization, id_vars=['H4', 'H3'], var_name='Period', value_name='Utilization')
    df_Summary_Utilization_Pd = df_Summary_Utilization.loc[df_Summary_Utilization['H3'] == 'Pd']
    df_Summary_Utilization_Pk = df_Summary_Utilization.loc[df_Summary_Utilization['H3'] == 'Pk']
    df_Summary_Utilization_Rpk = df_Summary_Utilization.loc[df_Summary_Utilization['H3'] == 'rPk']
    # </editor-fold>
    # <editor-fold desc="Show Summary Stats/KPI's">
    pens, util = st.columns(2)
    with pens:
        st.subheader("Penalties")
        st.bar_chart(df_Summary_Pens, x="H2", y="Total", color="H4", stack=False)
    with util:
        tab1, tab2, tab3 = st.tabs(["Pd", "Pk", "Rpk"])
        with tab1:
            st.subheader("Production Utilization")
            st.line_chart(df_Summary_Utilization_Pd, x="Period", y="Utilization", color="H4")
        with tab2:
            st.subheader("Packaging Utilization")
            st.line_chart(df_Summary_Utilization_Pk, x="Period", y="Utilization", color="H4")
        with tab3:
            st.subheader("Repacking Utilization")
            st.line_chart(df_Summary_Utilization_Rpk, x="Period", y="Utilization", color="H4")
    # </editor-fold>

def manufacturing_page():
    # <editor-fold desc="Select Manufacturing Type and Import Relevant Data">
    manuTypeSelect = st.selectbox("Choose Manufacturing Type: ",("GFB Production", "Packaging", "Repacking"),index=1)
    if manuTypeSelect == "GFB Production":
        #df_manuq = pd.read_excel("MABOutputFile_Template.xlsx", sheet_name='PdStQ')
        df_manuq = pd.read_sql_query('SELECT * FROM public."PdStQ" WHERE scenario_id = %s;', dbConnection, params=[scenario_id_from_url])
        unitType = "litres"
        siteType = "prod_site"
    elif manuTypeSelect == "Repacking":
        #df_manuq = pd.read_excel("MABOutputFile_Template.xlsx", sheet_name='rPkLnQ')
        df_manuq = pd.read_sql_query('SELECT * FROM public."rPkLnQ" WHERE scenario_id = %s;', dbConnection, params=[scenario_id_from_url])
        unitType = "cases"
        siteType = "repack_site"
    else:
        #df_manuq = pd.read_excel("MABOutputFile_Template.xlsx", sheet_name='PkLnQ')
        df_manuq = pd.read_sql_query('SELECT * FROM public."PkLnQ" WHERE scenario_id = %s;', dbConnection, params=[scenario_id_from_url])
        unitType = "cases"
        siteType = "pack_site"
    df_manuq['child_sku_group'] = df_manuq['child_sku_group'].replace(r'^\s+|\s+$', '', regex=True)
    # </editor-fold>
    # <editor-fold desc="Create Filters and Apply to Data">
    selCol1, selCol2, selCol3 = st.columns(3)
    with selCol1:
        period = st.multiselect("Select Period: ", df_manuq["period"].unique())
    with selCol2:
        parentSkuGroup = st.multiselect("Select Parent SKU Group: ", df_manuq["parent_sku_group"].unique())
    with selCol3:
        childSkuGroup = st.multiselect("Select Child SKU Group: ", df_manuq["child_sku_group"].unique())

    # Apply Filters to data
    if not period and not childSkuGroup and not parentSkuGroup:
        filtered_df_manuq = df_manuq
    elif not childSkuGroup and not parentSkuGroup:
        filtered_df_manuq = df_manuq[df_manuq["period"].isin(period)]
    elif not period and not parentSkuGroup:
        filtered_df_manuq = df_manuq[df_manuq["child_sku_group"].isin(childSkuGroup)]
    elif not childSkuGroup and not period:
        filtered_df_manuq = df_manuq[df_manuq["parent_sku_group"].isin(parentSkuGroup)]
    elif not childSkuGroup:
        filtered_df_manuq = df_manuq[df_manuq["period"].isin(period) & df_manuq["parent_sku_group"].isin(parentSkuGroup)]
    elif not parentSkuGroup:
        filtered_df_manuq = df_manuq[df_manuq["period"].isin(period) & df_manuq["child_sku_group"].isin(childSkuGroup)]
    elif not period:
        filtered_df_manuq = df_manuq[df_manuq["parent_sku_group"].isin(parentSkuGroup) & df_manuq["child_sku_group"].isin(childSkuGroup)]
    else:
        filtered_df_manuq = df_manuq[df_manuq["period"].isin(period) & df_manuq["child_sku_group"].isin(childSkuGroup) & df_manuq["parent_sku_group"].isin(parentSkuGroup)]

    category_df_manuq = filtered_df_manuq.groupby(by=[siteType], as_index=False)[unitType].sum()
    category_df_manuq2 = filtered_df_manuq.groupby(by=["period"], as_index=False)[unitType].sum()
    category_df_manuq3 = filtered_df_manuq.groupby(by=["child_sku_group"], as_index=False)[unitType].sum()
    category_df_manuq4 = filtered_df_manuq.pivot_table(index='sku_group', columns=siteType, values=unitType, aggfunc='sum')
    # </editor-fold>
    # <editor-fold desc="Display Graphs">
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Quantity per Site")
        fig = px.bar(category_df_manuq, x=siteType, y=unitType, template="seaborn",height=250)
        st.plotly_chart(fig,use_container_width=True)
    with col2:
        st.subheader("Quantity per Period")
        fig = px.line(category_df_manuq2, x="period", y=unitType, template="seaborn",height=250)
        st.plotly_chart(fig,use_container_width=True)

    col5, col6 = st.columns((2,4),gap='small')
    with col5:
        st.subheader("Quantity by Container Size")
        fig = px.line_polar(category_df_manuq3, r=unitType, theta="child_sku_group", line_close=True)
        st.plotly_chart(fig, use_container_width=True)
    with col6:
        st.subheader("Quantity by Site and SKU")
        st.dataframe(category_df_manuq4)
    # </editor-fold>

def distribution_page():
    if "selected_period" not in st.session_state:
        st.session_state.selected_period = []
    if "selected_sku_group" not in st.session_state:
        st.session_state.selected_sku_group = []

    # <editor-fold desc="Select Distribution Type and Import Relevant Data">
    #df_FgCm = pd.read_excel("MABOutputFile_Template.xlsx", sheet_name='FG_Cm')
    df_FgCm = pd.read_sql_query('SELECT * FROM public."FG_Cm" WHERE scenario_id = %s;', dbConnection, params=[scenario_id_from_url])
    # </editor-fold>
    # <editor-fold desc="Match ZIP Codes with Latitude and Longitude">
    nomi = pgeocode.Nominatim('US')
    df_FgCm = pd.merge(df_FgCm, df_ZipMaster, left_on='fg_warehouse', right_on='BGO Code')
    df_FgCm.rename(columns={'ZIP': 'FromZIP'}, inplace=True)
    df_FgCm.drop(['BGO Code'], axis=1, inplace=True)
    df_FgCm['FromZIP'] = df_FgCm['FromZIP'].apply(lambda x: x.zfill(5))

    df_FgCm = pd.merge(df_FgCm, df_ZipMaster, left_on='distributor', right_on='BGO Code')
    df_FgCm.rename(columns={'ZIP': 'ToZIP'}, inplace=True)
    df_FgCm['ToZIP'] = df_FgCm['ToZIP'].astype(str)
    df_FgCm.drop(['BGO Code'], axis=1, inplace=True)
    df_FgCm['ToZIP'] = df_FgCm['ToZIP'].apply(lambda x: x.zfill(5))

    df_FgCm['FromLat'] = pd.to_numeric(nomi.query_postal_code(df_FgCm['FromZIP'].tolist()).latitude)
    df_FgCm['FromLon'] = pd.to_numeric(nomi.query_postal_code(df_FgCm['FromZIP'].tolist()).longitude)
    df_FgCm['ToLat'] = pd.to_numeric(nomi.query_postal_code(df_FgCm['ToZIP'].tolist()).latitude)
    df_FgCm['ToLon'] = pd.to_numeric(nomi.query_postal_code(df_FgCm['ToZIP'].tolist()).longitude)
    # </editor-fold>
    # <editor-fold desc="Create Filters and Apply to Data">
    selCol1, selCol2 = st.columns(2)

    with selCol1:
        period = st.multiselect("Select Period: ", df_FgCm["period"].unique(), default=st.session_state.selected_period, key="selected_period")

    with selCol2:
        skuGrp = st.multiselect("Select SKU's: ", df_FgCm["sku_group"].unique(), default=st.session_state.selected_sku_group, key="selected_sku_group")

    # Apply Filters to data
    if not period and not skuGrp:
        filtered_df_FgCm = df_FgCm
    elif not skuGrp:
        filtered_df_FgCm = df_FgCm[df_FgCm["period"].isin(period)]
    elif not period:
        filtered_df_FgCm = df_FgCm[df_FgCm["sku_group"].isin(skuGrp)]
    else:
        filtered_df_FgCm = df_FgCm[df_FgCm["period"].isin(period) & df_FgCm["sku_group"].isin(skuGrp)]

    summary_df_FgCm = filtered_df_FgCm.groupby(['fg_warehouse','distributor'])['cases'].sum().reset_index()
    summary_df_FgCm = summary_df_FgCm.merge(filtered_df_FgCm[['fg_warehouse','FromLat','FromLon']].drop_duplicates(),on='fg_warehouse', how='left')
    summary_df_FgCm = summary_df_FgCm.merge(filtered_df_FgCm[['distributor','ToLat','ToLon']].drop_duplicates(),on='distributor', how='left')
    summary_df_FgCm['Color'] = 180-(80*(summary_df_FgCm['cases']/summary_df_FgCm['cases'].max()))

    qtyShipped = filtered_df_FgCm['cases'].sum()
    totalTrkLoads = filtered_df_FgCm['truck_loads'].sum()
    totalMilesTraveled = filtered_df_FgCm['route_miles'].sum()
    avgMilesPerLoad = totalMilesTraveled / totalTrkLoads
    # </editor-fold>
    # <editor-fold desc="Display Info Cards">
    col1, col2, col3, col4 = st.columns(4)
    col1.html(f'<div class="infoCard"><p class="infoCardHeading">Total Quantity Shipped</p><p class="infoCardText">{numerize.numerize(qtyShipped)}</p></div>')
    col2.html(f'<div class="infoCard"><p class="infoCardHeading">Total Truck Loads</p><p class="infoCardText">{numerize.numerize(totalTrkLoads)}</p></div>')
    col3.html(f'<div class="infoCard"><p class="infoCardHeading">Total Miles Traveled</p><p class="infoCardText">{numerize.numerize(totalMilesTraveled)}</p></div>')
    col4.html(f'<div class="infoCard"><p class="infoCardHeading">Avg. Miles per Truck Load</p><p class="infoCardText">{numerize.numerize(avgMilesPerLoad)}</p></div>')

    # </editor-fold>
    # <editor-fold desc="Draw Ray Diagram">
    st.subheader("Finished Goods (WHS) --> Distributor (Groups)")
    st.pydeck_chart(
        pdk.Deck(
            map_style=None,
            initial_view_state=pdk.ViewState(latitude=40,longitude=-117,zoom=2.4,pitch=30,),
            tooltip = {"text": "From Site: {fg_warehouse}\nDistributor: {distributor}\nCases: {cases}\nColor: {Color}"},
            layers=[
                pdk.Layer(
                    "LineLayer",
                    data=summary_df_FgCm,
                    getColor = ['Color',520,175],
                    getSourcePosition = ['FromLon','FromLat'],
                    getTargetPosition = ['ToLon', 'ToLat'],
                    getWidth = 2,
                    pickable = True,
                ),
            ],
        ),
    )

    # </editor-fold>

def comparison_page():
    scenario_list = pd.read_sql_query('SELECT id, scenario_code FROM public."Scenarios" WHERE scenario_status = 4;', dbConnection)
    scenario_list['Concatenated SC ID'] = scenario_list['id'].astype(str) + " - " + scenario_list['scenario_code'].astype(str)
    scenario_list = scenario_list.sort_values(by="id", ascending=False)

    comparisonDF = {'Headers': ['Scenario Code:',
                                'Volume',
                                'Periods',
                                'Min Batch?',
                                'Min Load?',
                                '',
                                'Req. Sales (M, Cases)',
                                'Actual Sales (M, Cases)',
                                'Total Packed (M, Cases)',
                                'Internal cs Co-Man (Pack)',
                                'Outbound Miles / Shipment',
                                '',
                                'Total COGS (M)',
                                'Total Indirect COGS (M)',
                                'Total COGS / Cs',
                                'Total Direct COGS / Cs',
                                'Total Indirect COGS / Cs',
                                '',
                                'Total Direct COGS (Var, M)',
                                'Manufacturing (Pd)',
                                'Manufacturing (Pk)',
                                'Manufacturing (rPk)',
                                'Co-Man Penalties (Pd)',
                                'Co-Man Penalties (Pk)',
                                'Co-Man Penalties (rPk)',
                                'Freight (SUBS Total)',
                                'Freight (Pk_Wip)',
                                'Freight (WIP_rPk)',
                                '',
                                'Total Direct COGS (Fxd, M)',
                                'Fixed Costs',
                                'Other Non Fixed Adj',
                                '',
                                'Total Indirect COGS (Var, M)',
                                'Handling',
                                'Storage',
                                'Intercompany Freight (Inter GFB, VP, NVP - TOTAL)',
                                'Freight (Pd_Pk)',
                                'Freight (Pk_FG)',
                                'Freight (rPk_FG)',
                                'Outbound Freight (VP, NVP - TOTAL)',
                                'Freight (FG_Cm)',
                                'Loads_SF (FG_Cm)',
                                'Dunnage',
                                'Other',
                                '',
                                'Total Indirect COGS (Fxd, M)',
                                'Fixed Storage & Handling',
                                'Other fixed',
                                'Other Non Fixed Adj',
                                '',
                                '',
                                'Pk Qty (M Cases/Yr)',
                                '',
                                'Avg. Pd Utilization (Hrs)',
                                'Avg. Pk Utilization (Hrs)',
                                'Avg. rPk Utilization (Hrs)',
                                '',
                                'Avg. Storage Utilization (WIP and/or FG, Cs)',
                                'Max Storage Utilization (WIP and/or FG, Cs)',
                                '',
                                ]}

    if "chosen_comparison_scenarios" not in st.session_state:
        st.session_state['chosen_comparison_scenarios'] = []

    # <editor-fold desc="Scenario Addition Form">
    with st.form("add_sc_form"):
        st.write("Add Scenarios to Comparison")
        chosen_scenario = st.selectbox("Choose Scenario:", options=scenario_list['Concatenated SC ID'])

        # Create Submit button and then create the logic
        submitted = st.form_submit_button("Add to Comparison")
        if submitted:
            chosen_scenario_id = int(scenario_list.loc[scenario_list['Concatenated SC ID'] == chosen_scenario, 'id'].iloc[0])
            if chosen_scenario_id not in st.session_state['chosen_comparison_scenarios']:
                chosen_scenario_summary = pd.read_sql_query('SELECT * FROM public."Summary" WHERE scenario_id = %s;', dbConnection, params=[chosen_scenario_id])
                chosen_scenario_model = pd.read_sql_query('SELECT * FROM public."Model" WHERE scenario_id = %s;', dbConnection, params=[chosen_scenario_id])
                if not chosen_scenario_summary.empty:
                    # <editor-fold desc="Get all Comparison info and add it to the Table">
                    comparisonDF[chosen_scenario_id]['Scenario Code:'] = chosen_scenario_model.loc[chosen_scenario_model['H1'] == 'Scenario Code', 'H2'].values[0]
                    comparisonDF[chosen_scenario_id]['Volume'] = chosen_scenario_model.loc[chosen_scenario_model['H1'] == 'Demand Notes', 'H2'].values[0]
                    comparisonDF[chosen_scenario_id]['Periods'] = chosen_scenario_model.loc[chosen_scenario_model['H1'] == 'Period Notes', 'H2'].values[0]
                    comparisonDF[chosen_scenario_id]['Min Batch?'] = chosen_scenario_model.loc[chosen_scenario_model['H1'] == 'Period Notes', 'H2'].values[0]
                    comparisonDF[chosen_scenario_id]['Min Load?'] = chosen_scenario_model.loc[chosen_scenario_model['H1'] == 'Period Notes', 'H2'].values[0]
                    comparisonDF[chosen_scenario_id]['Req. Sales (M, Cases)'] = chosen_scenario_summary[(chosen_scenario_summary['H1'] == "Volume") &(chosen_scenario_summary['H2'] == "Demand") &(chosen_scenario_summary['H3'] == "Required") &(chosen_scenario_summary['H4'] == "Total")]["Report Total"].sum()
                    comparisonDF[chosen_scenario_id]['Actual Sales (M, Cases)'] = chosen_scenario_summary[(chosen_scenario_summary['H1'] == "Volume") &(chosen_scenario_summary['H2'] == "Demand") &(chosen_scenario_summary['H3'] == "Delivered") &(chosen_scenario_summary['H4'] == "Total")]["Report Total"].sum()
                    comparisonDF[chosen_scenario_id]['Total Packed (M, Cases)'] = chosen_scenario_summary[(chosen_scenario_summary['H1'] == "Volume") &(chosen_scenario_summary['H2'] == "Quantity") &(chosen_scenario_summary['H3'] == "Pk")]["Report Total"].sum()
                    comparisonDF[chosen_scenario_id]['Internal cs Co-Man (Pack)'] = "x/y"
                    comparisonDF[chosen_scenario_id]['Outbound Miles / Shipment'] = ""
                    comparisonDF[chosen_scenario_id]['Total COGS (M)'] = 12222222222222222
                    comparisonDF[chosen_scenario_id]['Total Indirect COGS (M)'] = 1
                    comparisonDF[chosen_scenario_id]['Total COGS / Cs'] = 1
                    comparisonDF[chosen_scenario_id]['Total Direct COGS / Cs'] = 1
                    comparisonDF[chosen_scenario_id]['Total Indirect COGS / Cs'] = 1
                    comparisonDF[chosen_scenario_id]['Total Direct COGS (Var, M)'] = 1
                    comparisonDF[chosen_scenario_id]['Manufacturing (Pd)'] = 1
                    comparisonDF[chosen_scenario_id]['Manufacturing (Pk)'] = 1
                    comparisonDF[chosen_scenario_id]['Manufacturing (rPk)'] = 1
                    comparisonDF[chosen_scenario_id]['Co-Man Penalties (Pd)'] = 1
                    comparisonDF[chosen_scenario_id]['Co-Man Penalties (Pk)'] = 1
                    comparisonDF[chosen_scenario_id]['Co-Man Penalties (rPk)'] = 1
                    comparisonDF[chosen_scenario_id]['Freight (SUBS Total)'] = 1
                    comparisonDF[chosen_scenario_id]['Freight (Pk_Wip)'] = 1
                    comparisonDF[chosen_scenario_id]['Freight (WIP_rPk)'] = 1
                    comparisonDF[chosen_scenario_id]['Total Direct COGS (Fxd, M)'] = 1
                    comparisonDF[chosen_scenario_id]['Fixed Costs'] = 1
                    comparisonDF[chosen_scenario_id]['Other Non Fixed Adj'] = 1
                    comparisonDF[chosen_scenario_id]['Total Indirect COGS (Var, M)'] = 1
                    comparisonDF[chosen_scenario_id]['Handling'] = 1
                    comparisonDF[chosen_scenario_id]['Storage'] = 1
                    comparisonDF[chosen_scenario_id]['Intercompany Freight (Inter GFB, VP, NVP - TOTAL)'] = 1
                    comparisonDF[chosen_scenario_id]['Freight (Pd_Pk)'] = 1
                    comparisonDF[chosen_scenario_id]['Freight (Pk_FG)'] = 1
                    comparisonDF[chosen_scenario_id]['Freight (rPk_FG)'] = 1
                    comparisonDF[chosen_scenario_id]['Outbound Freight (VP, NVP - TOTAL)'] = 1
                    comparisonDF[chosen_scenario_id]['Freight (FG_Cm)'] = 1
                    comparisonDF[chosen_scenario_id]['Loads_SF (FG_Cm)'] = 1
                    comparisonDF[chosen_scenario_id]['Dunnage'] = 1
                    comparisonDF[chosen_scenario_id]['Other'] = 1
                    comparisonDF[chosen_scenario_id]['Total Indirect COGS (Fxd, M)'] = 1
                    comparisonDF[chosen_scenario_id]['Fixed Storage & Handling'] = 1
                    comparisonDF[chosen_scenario_id]['Other fixed'] = 1
                    comparisonDF[chosen_scenario_id]['Other Non Fixed Adj'] = 1
                    comparisonDF[chosen_scenario_id]['Pk Qty (M Cases/Yr)'] = 1
                    comparisonDF[chosen_scenario_id]['Avg. Pd Utilization (Hrs)'] = 1
                    comparisonDF[chosen_scenario_id]['Avg. Pk Utilization (Hrs)'] = 1
                    comparisonDF[chosen_scenario_id]['Avg. rPk Utilization (Hrs)'] = 1
                    comparisonDF[chosen_scenario_id]['Avg. Storage Utilization (WIP and/or FG, Cs)'] = 1
                    comparisonDF[chosen_scenario_id]['Max Storage Utilization (WIP and/or FG, Cs)'] = 1
                    # </editor-fold>
                    st.session_state['chosen_comparison_scenarios'].append(chosen_scenario_id)
                    st.success(f"Successfully added Scenario '{chosen_scenario}' to the Comparison!")
                else:
                    st.warning(f"Sorry, scenario '{chosen_scenario}' is old and cannot be added to the Comparison at this time")
            else:
                st.warning(f"Warning! Scenario '{chosen_scenario}' is already in the list")
    # </editor-fold>

    #chosen_comparison_scenarios_Tuple = tuple(st.session_state['chosen_comparison_scenarios'])
    #st.write(chosen_comparison_scenarios_Tuple)
    #st.write(st.session_state['chosen_comparison_scenarios'])
    st.dataframe(comparisonDF)

    #for numSc in chosen_comparison_scenarios_Tuple:



# <editor-fold desc="Page Navigation">
pages = {
"Main Page": main_page,
"Manufacturing Page": manufacturing_page,
"Distribution Page": distribution_page,
"Comparison Page": comparison_page,
}
page = st.sidebar.radio("Select a page", pages.keys())
pages[page]()
# </editor-fold>
dbConnection.close()