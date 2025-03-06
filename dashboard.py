# <editor-fold desc="Imports">
import streamlit as st
import plotly.express as px
import pandas as pd
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
from dotenv import load_dotenv
import psycopg2
# </editor-fold>

# <editor-fold desc="Setup Variables">
#filePath = r"C:\Users\AngeloGuerra\OneDrive - Rieger Industrial Consultants CC\SP_Link\RIC-530 MAB BGO Licensing\Tool Upgrades\Web Dashboards"
stylePath = r'style_v2.0.css'
ricLogoPath = "https://www.ricgroup.net/wp-content/uploads/sites/1122/2020/02/RIC_logo_800px.png"
mabLogoPath = "https://markanthony.com/wp-content/uploads/mark-anthony-group-logo.png"
# </editor-fold>

# ANGELO
# Get scenario ID from URL parameters i.e scenarioId = 1544 when URL is http://localhost:8501/?id=1544
query_params = st.query_params
scenarioId = query_params.get("id")

# <editor-fold desc="DB Connection">
# ANGELO - Ensure that you have a .env file in the root directory with 
load_dotenv()
dbConnection = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    database=os.getenv('DB_DATABASE'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)
db_cur = dbConnection.cursor()
# </editor-fold>

# <editor-fold desc="Configuration">
warnings.filterwarnings('ignore')
st.set_page_config(page_title="RIC BGO Dashboard", page_icon=":chart_with_upwards_trend:", layout="wide")
# </editor-fold>

# <editor-fold desc="Import CSS Styles and Create Page Title">
with open(stylePath) as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
st.html(f'<div class= "pageTitle"><img class="logo" align ="left" src={ricLogoPath}>BGO Scenario Dashboard<img class="logo" align="right" src={mabLogoPath}></div')
st.divider()
# </editor-fold>

# <editor-fold desc="Import the Excel File">
'''
os.chdir(filePath)
df_CmMaster = pd.read_excel("MABOutputFile_Template.xlsx", sheet_name='CmMaster')
df_ZipMaster = pd.read_excel("MABOutputFile_Template.xlsx", sheet_name='ZIPMaster')
df_ZipMaster.drop_duplicates(subset='BGO Code', keep='first', inplace=True)
df_Model = pd.read_excel("MABOutputFile_Template.xlsx", sheet_name='Model')
df_Summary = pd.read_excel("MABOutputFile_Template.xlsx", sheet_name='Summary')
'''
# </editor-fold>

# <editor-fold desc="Import from DB">
df_CmMaster = pd.read_sql_query("SELECT * FROM CmMaster WHERE scenario_id = %s", dbConnection, params=(scenarioId))
df_ZipMaster = pd.read_sql_query("SELECT * FROM ZIPMaster", dbConnection)
df_ZipMaster.drop_duplicates(subset='BGO Code', keep='first', inplace=True)
df_Model = pd.read_sql_query("SELECT * FROM Model", dbConnection)
df_Summary = pd.read_sql_query("SELECT * FROM Summary", dbConnection)
# </editor-fold>

# <editor-fold desc="Get Scenario Information">
ScCode = df_Model.loc[df_Model['H1'] == 'Scenario Code', 'H2'].values[0]
ScDescription = df_Model.loc[df_Model['H1'] == 'Scenario Description', 'H2'].values[0]
ScGrp = "Group-Level Run" if df_Model.loc[df_Model['H1'] == 'Run Model on SKU Grp Level', 'H2'].values[0] == "Yes" else "SKU Level Run"

exPd = df_Model.loc[df_Model['H1'] == 'Excluded Sites: Pd Site', 'H2'].values[0]
exPk = df_Model.loc[df_Model['H1'] == 'Excluded Sites: Pk Site', 'H2'].values[0]
exRpk = df_Model.loc[df_Model['H1'] == 'Excluded Sites: rPk Site', 'H2'].values[0]
exWip = df_Model.loc[df_Model['H1'] == 'Excluded Sites: WIP Site', 'H2'].values[0]
exFg = df_Model.loc[df_Model['H1'] == 'Excluded Sites: FG Site', 'H2'].values[0]
# </editor-fold>

def main_page():
    # <editor-fold desc="Show Scenario Info on Cards">
    colCode, colDescription, colGrp = st.columns(3)
    colCode.html(f'<div class="infoCard"><p class="infoCardHeading">Scenario Code</p><p class="infoCardText">{ScCode}</p></div>')
    colDescription.html(f'<div class="infoCard"><p class="infoCardHeading">Scenario Description</p><p class="infoCardText">{ScDescription}</p></div>')
    colGrp.html(f'<div class="infoCard"><p class="infoCardHeading">Run Type</p><p class="infoCardText">{ScGrp}</p></div>')
    # </editor-fold>
    # <editor-fold desc="Show Excl. Sites on Cards">
    colExPd, colExPk, colExRpk, colExWip, colExFg = st.columns(5)
    colExPd.html(f'<div class="infoCard2"><p class="infoCardHeading2">Excl. Pk Sites</p><p class="infoCardText2">{exPd}</p></div>')
    colExPk.html(f'<div class="infoCard2"><p class="infoCardHeading2">Excl. Pk Sites</p><p class="infoCardText2">{exPk}</p></div>')
    colExRpk.html(f'<div class="infoCard2"><p class="infoCardHeading2">Excl. rPk Sites</p><p class="infoCardText2">{exRpk}</p></div>')
    colExWip.html(f'<div class="infoCard2"><p class="infoCardHeading2">Excl. WIP Sites</p><p class="infoCardText2">{exWip}</p></div>')
    colExFg.html(f'<div class="infoCard2"><p class="infoCardHeading2">Excl. FG Sites</p><p class="infoCardText2">{exFg}</p></div>')
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
        df_manuq = pd.read_sql_query("SELECT * FROM PdStQ", dbConnection)
        unitType = "Liters"
        siteType = "Prod Site"
    elif manuTypeSelect == "Repacking":
        #df_manuq = pd.read_excel("MABOutputFile_Template.xlsx", sheet_name='rPkLnQ')
        df_manuq = pd.read_sql_query("SELECT * FROM rPkLnQ", dbConnection)
        unitType = "Cases"
        siteType = "RePack Site"
    else:
        #df_manuq = pd.read_excel("MABOutputFile_Template.xlsx", sheet_name='PkLnQ')
        df_manuq = pd.read_sql_query("SELECT * FROM PkLnQ", dbConnection)
        unitType = "Cases"
        siteType = "Pack Site"
    df_manuq['Child SKU Grp'] = df_manuq['Child SKU Grp'].replace(r'^\s+|\s+$', '', regex=True)
    # </editor-fold>
    # <editor-fold desc="Create Filters and Apply to Data">
    selCol1, selCol2, selCol3 = st.columns(3)
    with selCol1:
        period = st.multiselect("Select Period: ", df_manuq["Period"].unique())
    with selCol2:
        parentSkuGroup = st.multiselect("Select Parent SKU Group: ", df_manuq["Parent SKU Grp"].unique())
    with selCol3:
        childSkuGroup = st.multiselect("Select Child SKU Group: ", df_manuq["Child SKU Grp"].unique())

    # Apply Filters to data
    if not period and not childSkuGroup and not parentSkuGroup:
        filtered_df_manuq = df_manuq
    elif not childSkuGroup and not parentSkuGroup:
        filtered_df_manuq = df_manuq[df_manuq["Period"].isin(period)]
    elif not period and not parentSkuGroup:
        filtered_df_manuq = df_manuq[df_manuq["Child SKU Grp"].isin(childSkuGroup)]
    elif not childSkuGroup and not period:
        filtered_df_manuq = df_manuq[df_manuq["Parent SKU Grp"].isin(parentSkuGroup)]
    elif not childSkuGroup:
        filtered_df_manuq = df_manuq[df_manuq["Period"].isin(period) & df_manuq["Parent SKU Grp"].isin(parentSkuGroup)]
    elif not parentSkuGroup:
        filtered_df_manuq = df_manuq[df_manuq["Period"].isin(period) & df_manuq["Child SKU Grp"].isin(childSkuGroup)]
    elif not period:
        filtered_df_manuq = df_manuq[df_manuq["Parent SKU Grp"].isin(parentSkuGroup) & df_manuq["Child SKU Grp"].isin(childSkuGroup)]
    else:
        filtered_df_manuq = df_manuq[df_manuq["Period"].isin(period) & df_manuq["Child SKU Grp"].isin(childSkuGroup) & df_manuq["Parent SKU Grp"].isin(parentSkuGroup)]

    category_df_manuq = filtered_df_manuq.groupby(by=[siteType], as_index=False)[unitType].sum()
    category_df_manuq2 = filtered_df_manuq.groupby(by=["Period"], as_index=False)[unitType].sum()
    category_df_manuq3 = filtered_df_manuq.groupby(by=["Child SKU Grp"], as_index=False)[unitType].sum()
    category_df_manuq4 = filtered_df_manuq.pivot_table(index='SKU Group', columns=siteType, values=unitType, aggfunc='sum')
    # </editor-fold>
    # <editor-fold desc="Display Graphs">
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Quantity per Site")
        fig = px.bar(category_df_manuq, x=siteType, y=unitType, template="seaborn",height=250)
        st.plotly_chart(fig,use_container_width=True)
    with col2:
        st.subheader("Quantity per Period")
        fig = px.line(category_df_manuq2, x="Period", y=unitType, template="seaborn",height=250)
        st.plotly_chart(fig,use_container_width=True)

    col5, col6 = st.columns((2,4),gap='small')
    with col5:
        st.subheader("Quantity by Container Size")
        fig = px.line_polar(category_df_manuq3, r=unitType, theta="Child SKU Grp", line_close=True)
        st.plotly_chart(fig, use_container_width=True)
    with col6:
        st.subheader("Quantity by Site and SKU")
        st.dataframe(category_df_manuq4)
    # </editor-fold>

def distribution_page():
    # <editor-fold desc="Select Distribution Type and Import Relevant Data">
    #df_FgCm = pd.read_excel("MABOutputFile_Template.xlsx", sheet_name='FG_Cm')
    df_FgCm = pd.read_sql_query("SELECT * FROM FG_Cm", dbConnection)
    # </editor-fold>
    # <editor-fold desc="Match ZIP Codes with Latitude and Longitude">
    nomi = pgeocode.Nominatim('US')
    df_FgCm = pd.merge(df_FgCm, df_ZipMaster, left_on='FG Warehouse', right_on='BGO Code')
    df_FgCm.rename(columns={'ZIP': 'FromZIP'}, inplace=True)
    df_FgCm.drop(['BGO Code'], axis=1, inplace=True)
    df_FgCm['FromZIP'] = df_FgCm['FromZIP'].apply(lambda x: x.zfill(5))

    df_FgCm = pd.merge(df_FgCm, df_ZipMaster, left_on='Distributor', right_on='BGO Code')
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
        period = st.multiselect("Select Period: ", df_FgCm["Period"].unique())
    with selCol2:
        skuGrp = st.multiselect("Select SKU's: ", df_FgCm["SKU Group"].unique())
    # Apply Filters to data
    if not period and not skuGrp:
        filtered_df_FgCm = df_FgCm
    elif not skuGrp:
        filtered_df_FgCm = df_FgCm[df_FgCm["Period"].isin(period)]
    elif not period:
        filtered_df_FgCm = df_FgCm[df_FgCm["SKU Group"].isin(skuGrp)]
    else:
        filtered_df_FgCm = df_FgCm[df_FgCm["Period"].isin(period) & df_FgCm["SKU Group"].isin(skuGrp)]

    summary_df_FgCm = filtered_df_FgCm.groupby(['FG Warehouse','Distributor'])['Cases'].sum().reset_index()
    summary_df_FgCm = summary_df_FgCm.merge(filtered_df_FgCm[['FG Warehouse','FromLat','FromLon']].drop_duplicates(),on='FG Warehouse', how='left')
    summary_df_FgCm = summary_df_FgCm.merge(filtered_df_FgCm[['Distributor','ToLat','ToLon']].drop_duplicates(),on='Distributor', how='left')
    summary_df_FgCm['Color'] = 180-(80*(summary_df_FgCm['Cases']/summary_df_FgCm['Cases'].max()))

    qtyShipped = filtered_df_FgCm['Cases'].sum()
    totalTrkLoads = filtered_df_FgCm['Truck Loads'].sum()
    totalMilesTraveled = filtered_df_FgCm['Route Miles'].sum()
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
            tooltip = {"text": "From Site: {FG Warehouse}\nDistributor: {Distributor}\nCases: {Cases}\nColor: {Color}"},
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

# <editor-fold desc="Page Navigation">
pages = {
"Main Page": main_page,
"Manufacturing Page": manufacturing_page,
"Distribution Page": distribution_page,
}
page = st.sidebar.radio("Select a page", pages.keys())
pages[page]()
# </editor-fold>
dbConnection.close()