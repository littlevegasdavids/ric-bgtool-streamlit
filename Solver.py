'''
includes:
PdS, PkS, rPkS, Pd, Pk, rPk, FG, WIP, Cm
multi-period
Outbound site specific: Pd, Pk (WIP & FG), WIP, rPk. Site specific
'''

import math
import sys
import numpy as np
import copy
import pandas as pnd
import numbers
import warnings
from doctest import Example
from fileinput import filename
import math
from struct import pack
import sys
import copy
import numbers
import warnings
import requests
from datetime import datetime, timezone
import psycopg2
from dotenv import load_dotenv
import os
import time

#Read me:
#Tag Reno means Reno Code

#Tag Reno: Enviroment Variables
load_dotenv()
db_conn = psycopg2.connect(
    host=os.getenv('PGHOST'),
    database=os.getenv('PGDATABASE'),
    user=os.getenv('PGUSER'),
    password=os.getenv('PGPASSWORD')
)
db_cur = db_conn.cursor()

#Tag Reno: Check that scenario Id exists
scenarioId = sys.argv[1]
fileName = scenarioId + ".xlsx"
#Tag Reno: Input File Name
xlsx = pnd.ExcelFile(r"excelFiles/uploaded/" + fileName)

from pyomo import environ as pe
model = pe.ConcreteModel()


pnd.set_option("display.max_rows", 10, "display.max_columns", None)



enableScaling = 'Yes'
scalingVolume = 10000
scalingCost = 10
if enableScaling != 'Yes':
    scalingVolume = 1
    scalingCost = 1

Pk_Cm_Route = "No"
enableLoadSF_Full = "No"
enableLoadSF_MinFract = "No"
enableWIPInitialStockPenalty = "No"
enableIMM = 'Yes'


model_version = 'v9.01'

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')


cpDescription_tb = pnd.read_excel(xlsx, "Cover_Page", index_col=None, usecols="C", header=18, nrows=0)
cpDescription_data = cpDescription_tb.columns.values[0]

cpScenario_tb = pnd.read_excel(xlsx, "Cover_Page", index_col=None, usecols="F", header=21, nrows=0)
cpScenario_data = cpScenario_tb.columns.values[0]

#Tag Reno: Output file name
outputFileName = 'MABOutputFile_' + cpScenario_data + ".xlsx"

cpValSettings_tb = pnd.read_excel(xlsx, "Cover_Page", index_col=None, usecols="B,C", header=20, nrows=11).set_index('VALUE SETTINGS')
cpValSettings_data = pnd.DataFrame(cpValSettings_tb).to_dict()
MIPGap = cpValSettings_data['VALUE']['MIP Gap (%)'] / 100
cutoffTime = cpValSettings_data['VALUE']['Model Cut-Off Time (min)']
dunnage_dist = cpValSettings_data['VALUE']['Dunnage Distance (miles)']
dunnage_cost = cpValSettings_data['VALUE']['Dunnage Cost ($/load)'] / scalingCost
minLoad = cpValSettings_data['VALUE']['Min Route Size (Trucks)']

cpFnSettings_tb = pnd.read_excel(xlsx, "Cover_Page", index_col=None, usecols="E,F", header=24, nrows=7).set_index('FUNCTIONALITY SETTINGS')
cpFnSettings_data = pnd.DataFrame(cpFnSettings_tb).to_dict()

enableLoadSF_MinRnd = cpFnSettings_data['VALUE']['Enable Route Load Constraint']
enableMinBatchSize = cpFnSettings_data['VALUE']['Enable Min Batch Size']
enableFGStockCover = cpFnSettings_data['VALUE']['Enable FG Stock Cover']
autofillInitialFGStorage = cpFnSettings_data['VALUE']['Autofill Initial FG Stock']
enableWIPStockCover = cpFnSettings_data['VALUE']['Enable WIP Stock Cover']
autofillInitialWIPStorage = cpFnSettings_data['VALUE']['Autofill Initial WIP Stock']
modelGrpLevel = cpFnSettings_data['VALUE']['Run Model on SKU Grp Level']

cpSiteExcl_tb = pnd.read_excel(xlsx, "Cover_Page", index_col=None, usecols="B,C,D,E,F", header=34, nrows=7)
cpSiteExcl_data = pnd.DataFrame(cpSiteExcl_tb).to_dict('list')

cpPenaltyExcl_tb = pnd.read_excel(xlsx, "Cover_Page", index_col=None, usecols="B,C,D", header=43, nrows=7)
cpPenaltyExcl_data = pnd.DataFrame(cpPenaltyExcl_tb).to_dict('list')

cpConstraints_tb = pnd.read_excel(xlsx, "Cover_Page", index_col=None, usecols="B,C,D,E", header=53, nrows=7).set_index('Site Type').dropna()
if enableScaling == 'Yes':
    cpConstraints_tb['Value (per Period)'] = cpConstraints_tb['Value (per Period)'] / scalingVolume
if cpConstraints_tb.empty:
    cpConstraints_data = {}
else:
    cpConstraints_data = pnd.DataFrame(cpConstraints_tb).groupby('Site Type').apply(lambda x: x.set_index('Site Name').to_dict(orient='index')).to_dict()


cpScenarioNotes_tb = pnd.read_excel(xlsx, "Cover_Page", index_col=None, usecols="K,L", header=3, nrows=33)
cpScenarioNotes_tb = pnd.DataFrame(cpScenarioNotes_tb).dropna(how='all')

cpScenarioNotes_tb = cpScenarioNotes_tb.rename(columns={cpScenarioNotes_tb.columns[0]: 'Tab', cpScenarioNotes_tb.columns[1]: 'Note'})
cpScenarioNotes_data = pnd.DataFrame(cpScenarioNotes_tb).set_index(cpScenarioNotes_tb.columns[0]).to_dict()


cpIterativeMM_tb = pnd.read_excel(xlsx, "Cover_Page", index_col=None, usecols="B,C,D, E, F", header=104, nrows=3).set_index('Item')
cpIterativeMM_data = pnd.DataFrame(cpIterativeMM_tb).to_dict()

#
WarmStartPk_tb = pnd.read_excel(xlsx, "WarmStart_PkVar")
WarmStartPk_tb['SKU'] = WarmStartPk_tb['SKU'].astype(str)

WarmStartPkBin_tb = pnd.read_excel(xlsx, "WarmStart_PkBin")
WarmStartPkBin_tb['SKU'] = WarmStartPkBin_tb['SKU'].astype(str)

WarmStartDist_tb = pnd.read_excel(xlsx, "WarmStart_Dist")
WarmStartDist_tb['Distributor'] = WarmStartDist_tb['Distributor'].astype(str)

cpReporting_tb = pnd.read_excel(xlsx, "Cover_Page", index_col=None, usecols="B,C,D,E", header=68, nrows=20).set_index('Period')

cpWarmUp_tb = pnd.read_excel(xlsx, "Cover_Page", index_col=None, usecols="B,C,D", header=63, nrows=3).set_index('Copied Period Volume')

cpCoolDown_tb = pnd.read_excel(xlsx, "Cover_Page", index_col=None, usecols="B,C,D", header=90, nrows=3).set_index('Copied Period Volume')

Period_tb = pnd.read_excel(xlsx, "Period", header=0, nrows=2).set_index('Duration').drop(columns=['Limit_Type', 'Limit_Location'])

CapLimit_tb = pnd.read_excel(xlsx, "Period").set_index('Limit_Type').drop(columns=['Duration'])

Unit_tb = pnd.read_excel(xlsx, "Unit")

UnitMaster_tb = pnd.read_excel(xlsx, "Unit")

CmMaster_tb = pnd.read_excel(xlsx, "Cm_Master")

ZIPMaster_tb = pnd.read_excel(xlsx, "ZIP_Master")

CmDem_tb = pnd.read_excel(xlsx, "Demand").set_index('Distributor')
CmDem_tb['SKU_Number'] = CmDem_tb['SKU_Number'].astype(str)

FGInit_tb = pnd.read_excel(xlsx, "FG_Init").set_index('Site')

WIPInit_tb = pnd.read_excel(xlsx, "WIP_Init").set_index('Site')

PdS_tb = pnd.read_excel(xlsx, "PdS").set_index('Site')

PkS_tb = pnd.read_excel(xlsx, "PkS").set_index('Site')

rPkS_tb = pnd.read_excel(xlsx, "rPkS").set_index('Site')

PdCap_tb = pnd.read_excel(xlsx, "Pd_Cap").set_index('Site').drop(columns=['Available', 'Off_Cost', 'On_Cost', 'OB_Sites', 'Group', 'Min_Grp_Qty', 'Min_Grp_Penalty', 'OEE','Period_Availability'])

PdGrp_tb = pnd.read_excel(xlsx, "Pd_Cap", usecols=['Group', 'Min_Grp_Qty', 'Min_Grp_Penalty']).set_index('Group')

PdGrpMap_tb = pnd.read_excel(xlsx, "Pd_Cap", usecols=['Group', 'Site', 'Stream']).set_index('Group')

PdCapEff_tb = pnd.read_excel(xlsx, "Pd_Cap", usecols=['Site', 'Stream', 'OEE', 'Period_Availability']).set_index('Site')

PdOB_tb = pnd.read_excel(xlsx, "Pd_Cap", usecols=['Site', 'OB_Sites']).set_index('Site')

PdCst_tb = pnd.read_excel(xlsx, "Pd_Cost").set_index('Site')

PkCap_tb = pnd.read_excel(xlsx, "Pk_Cap").set_index('Site').drop(columns=['Site_Type', 'Available', 'Off_Cost', 'On_Cost', 'OB_Sites(FG)', 'OB_Sites(WIP)', 'Group', 'Min_Grp_Qty','TOP_Penalty', 'TIER_Penalty', 'Rebate_Min', 'Rebate', 'PenType', 'OEE', 'Period_Availability'])

PkGrp_tb = pnd.read_excel(xlsx, "Pk_Cap", usecols=['Group', 'Min_Grp_Qty', 'TOP_Penalty', 'TIER_Penalty', 'Rebate_Min', 'Rebate','PenType']).set_index('Group')

PkGrpMap_tb = pnd.read_excel(xlsx, "Pk_Cap", usecols=['Group', 'Site', 'Line']).set_index('Group')

PkCapEff_tb = pnd.read_excel(xlsx, "Pk_Cap", usecols=['Site', 'Line', 'OEE', 'Period_Availability']).set_index('Site')

PkOB_tb = pnd.read_excel(xlsx, "Pk_Cap", usecols=['Site', 'OB_Sites(FG)', 'OB_Sites(WIP)']).set_index('Site')

PkSiteType_tb = pnd.read_excel(xlsx, "Pk_Cap", usecols=['Site', 'Line', 'Site_Type']).set_index('Site')

PkCst_tb = pnd.read_excel(xlsx, "Pk_Cost").set_index('Site')

Min_Batch_tb = pnd.read_excel(xlsx, "PkMin_Batch").set_index('Pk_Site')

rPkCap_tb = pnd.read_excel(xlsx, "rPk_Cap").set_index('Site').drop(columns=['Available', 'Off_Cost', 'On_Cost', 'OB_Sites', 'Group', 'Min_Grp_Qty', 'Min_Grp_Penalty', 'OEE','Period_Availability'])

rPkGrp_tb = pnd.read_excel(xlsx, "rPk_Cap", usecols=['Group', 'Min_Grp_Qty', 'Min_Grp_Penalty']).set_index('Group')

rPkGrpMap_tb = pnd.read_excel(xlsx, "rPk_Cap", usecols=['Group', 'Site', 'Line']).set_index('Group')

rPkCapEff_tb = pnd.read_excel(xlsx, "rPk_Cap", usecols=['Site', 'Line', 'OEE', 'Period_Availability']).set_index('Site')

rPkOB_tb = pnd.read_excel(xlsx, "rPk_Cap", usecols=['Site', 'OB_Sites']).set_index('Site')

rPkCst_tb = pnd.read_excel(xlsx, "rPk_Cost").set_index('Site')

WIPCap_tb = pnd.read_excel(xlsx, "WIP_Cap").set_index('Site').drop(columns=['Site_Type', 'Shared_FG_Site', 'Available', 'Off_Cost', 'On_Cost', 'OB_Sites'])

WIPOB_tb = pnd.read_excel(xlsx, "WIP_Cap", usecols=['Site', 'OB_Sites']).set_index('Site')

WIPSiteType_tb = pnd.read_excel(xlsx, "WIP_Cap",usecols=['Site', 'Expansion', 'Site_Type', 'Shared_FG_Site']).set_index('Site')

WIPCst_tb = pnd.read_excel(xlsx, "WIP_Cost").set_index('Site')

FGCap_tb = pnd.read_excel(xlsx, "FG_Cap").set_index('Site').drop(columns=['Site_Type', 'Available', 'Off_Cost', 'On_Cost'])

FGSiteType_tb = pnd.read_excel(xlsx, "FG_Cap", usecols=['Site', 'Expansion', 'Site_Type']).set_index('Site')

FGCst_tb = pnd.read_excel(xlsx, "FG_Cost").set_index('Site')

PdSSKU_PdSKU_tb = pnd.read_excel(xlsx, "MAP_PdS.Pd").set_index('SKU_Group')
PdSSKU_PdSKU_data = pnd.DataFrame.to_dict(PdSSKU_PdSKU_tb)

PkSSKU_PkSKU_tb = pnd.read_excel(xlsx, "MAP_PkS.Pk").set_index('SKU_Group')
PkSSKU_PkSKU_data = pnd.DataFrame.to_dict(PkSSKU_PkSKU_tb)

rPkSSKU_rPkSKU_tb = pnd.read_excel(xlsx, "MAP_rPkS.rPk").set_index('SKU_Group')
rPkSSKU_rPkSKU_data = pnd.DataFrame.to_dict(rPkSSKU_rPkSKU_tb)

PdSKU_PkSKU_tb = pnd.read_excel(xlsx, "MAP_Pd.Pk").set_index('SKU_Number')
PdSKU_PkSKU_tb.index = PdSKU_PkSKU_tb.index.astype(str)

PkSKU_rPkSKU_tb = pnd.read_excel(xlsx, "MAP_Pk.rPk").set_index('SKU_Number')
PkSKU_rPkSKU_tb.index = PkSKU_rPkSKU_tb.index.astype(str)
PkSKU_rPkSKU_tb.columns = PkSKU_rPkSKU_tb.columns.astype(str)

D_Lanes_tb = pnd.read_excel(xlsx, "D_Lanes").set_index('Origin')
#
xlsx.close()

runType = 'Iterative' if enableLoadSF_MinRnd == 'Yes' and enableMinBatchSize == 'Yes' and modelGrpLevel == 'No' and enableIMM == 'Yes' else 'Standard'
itnNumber = 4 if runType == 'Iterative' else 1


for itn in range(itnNumber):


    if runType == 'Standard':
        print('Standard Model Run')
        warmStart_PkVar = 'No'
        warmStart_PkBin = 'No'
        warmStart_DistVar_FGCm = 'No'
        warmStart_DistVar_FGCmSKU = 'No'
        warmStart_DistBin = 'No'
        MIPFocus = 0




        if enableMinBatchSize == 'Yes' and modelGrpLevel == 'No':
            NoRelHeurTime = 7200
            if cutoffTime < 240:
                cutoffTime = 240
        else:
            NoRelHeurTime = 0
        Presolve = 2
        reportDistBinOnly = 'Yes'
        warmUpPkMinFactor = 0.4

        PkBatchTempRedFactor = 1
        PkCapTempRedFactor = 1



    elif itn == 0:
        print('Iterative Model Mode: Iteration ' + str(itn))
        warmStart_PkVar = 'No'
        warmStart_PkBin = 'No'
        warmStart_DistVar_FGCm = 'No'
        warmStart_DistVar_FGCmSKU = 'No'
        warmStart_DistBin = 'No'
        MIPFocus = 0

        MIPGap = cpIterativeMM_data['Iteration 0']['MIP Gap (%)'] / 100
        NoRelHeurTime = 0
        Presolve = 2
        reportDistBinOnly = 'Yes'
        warmUpPkMinFactor = 0.4

        PkBatchTempRedFactor = 0.2
        PkCapTempRedFactor = cpIterativeMM_data['Iteration 0']['Pk Capacity Margin']
        cutoffTime = cpIterativeMM_data['Iteration 0']['Model Cut-Off Time (min)']
        enableMinBatchSize = 'No'
        enableLoadSF_MinRnd = 'No'


    elif itn == 1:
        print('Iterative Model Mode: Iteration ' + str(itn))
        warmStart_PkVar = 'Yes'
        warmStart_PkBin = 'No'
        warmStart_DistVar_FGCm = 'No'
        warmStart_DistVar_FGCmSKU = 'No'
        warmStart_DistBin = 'No'
        MIPFocus = 1

        NoRelHeurTime = 2000
        Presolve = 2
        reportDistBinOnly = 'Yes'
        warmUpPkMinFactor = 0.4

        PkBatchTempRedFactor = 0.2
        PkCapTempRedFactor = cpIterativeMM_data['Iteration 1']['Pk Capacity Margin']
        MIPGap = cpIterativeMM_data['Iteration 1']['MIP Gap (%)'] / 100
        cutoffTime = cpIterativeMM_data['Iteration 1']['Model Cut-Off Time (min)']
        enableMinBatchSize = 'Yes'
        enableLoadSF_MinRnd = 'No'


    elif itn == 2:
        print('Iterative Model Mode: Iteration ' + str(itn))
        warmStart_PkVar = 'Yes'
        warmStart_PkBin = 'No'
        warmStart_DistVar_FGCm = 'No'
        warmStart_DistVar_FGCmSKU = 'No'
        warmStart_DistBin = 'No'
        MIPFocus = 0

        NoRelHeurTime = 3600
        Presolve = 2
        reportDistBinOnly = 'Yes'
        warmUpPkMinFactor = 0.4

        PkBatchTempRedFactor = 1
        PkCapTempRedFactor = cpIterativeMM_data['Iteration 2']['Pk Capacity Margin']
        MIPGap = cpIterativeMM_data['Iteration 2']['MIP Gap (%)'] / 100
        cutoffTime = cpIterativeMM_data['Iteration 2']['Model Cut-Off Time (min)']
        enableMinBatchSize = 'Yes'
        enableLoadSF_MinRnd = 'No'


    elif itn == 3:
        print('Iterative Model Mode: Iteration ' + str(itn) + ' (Final)')
        warmStart_PkVar = 'No'
        warmStart_PkBin = 'Yes'
        warmStart_DistVar_FGCm = 'No'
        warmStart_DistVar_FGCmSKU = 'No'
        warmStart_DistBin = 'Yes'
        MIPFocus = 0

        NoRelHeurTime = 0
        Presolve = 2
        reportDistBinOnly = 'Yes'
        warmUpPkMinFactor = 0.4

        PkBatchTempRedFactor = 1
        PkCapTempRedFactor = cpIterativeMM_data['Iteration 3']['Pk Capacity Margin']
        MIPGap = cpIterativeMM_data['Iteration 3']['MIP Gap (%)'] / 100
        cutoffTime = cpIterativeMM_data['Iteration 3']['Model Cut-Off Time (min)']
        enableMinBatchSize = 'Yes'
        enableLoadSF_MinRnd = 'Yes'






    if enableIMM == 'No':

        if warmStart_PkVar == 'Yes':
            WarmStartPk_tb = pnd.DataFrame(WarmStartPk_tb).values.tolist()

        if warmStart_PkBin == 'Yes':
            WarmStartPkBin_tb = pnd.DataFrame(WarmStartPkBin_tb).values.tolist()

        if warmStart_DistBin == 'Yes' or warmStart_DistVar_FGCm == 'Yes' or warmStart_DistVar_FGCmSKU:

            WarmStartDist_tb['SKU'] = WarmStartDist_tb['SKU'].astype(str)

            WarmStartDist_tbD = pnd.DataFrame(WarmStartDist_tb)
            WarmStartDist_tbD = {a: {k: f.groupby('SKU')['Period'].apply(list).to_dict() for k, f in g.groupby('Distributor')}
                                 for a, g in WarmStartDist_tbD.groupby('FG_Site')}
            WarmStartDist_tb = pnd.DataFrame(WarmStartDist_tb).values.tolist()





    cpReporting_data = pnd.DataFrame(cpReporting_tb).to_dict()

    cpWarmUp_data = pnd.DataFrame(cpWarmUp_tb).to_dict()


    '''tempDict = copy.deepcopy(cpWarmUp_data)
    for columnName in tempDict:
        for copiedPeriod in tempDict[columnName]:
            if copiedPeriod in cpReporting_data['Period Number'] and pnd.notna(copiedPeriod):
                newName = str(cpReporting_data['Period Number'][copiedPeriod]) + str(':') + str(copiedPeriod)
                cpWarmUp_data[columnName][newName] = cpWarmUp_data[columnName][copiedPeriod]
                del cpWarmUp_data[columnName][copiedPeriod]
    del tempDict'''

    cpCoolDown_data = pnd.DataFrame(cpCoolDown_tb).to_dict()

    '''tempDict = copy.deepcopy(cpCoolDown_data)
    for columnName in tempDict:
        for copiedPeriod in tempDict[columnName]:
            if copiedPeriod in cpReporting_data['Period Number'] and pnd.notna(copiedPeriod):
                newName = str(cpReporting_data['Period Number'][copiedPeriod]) + str(':') + str(copiedPeriod)
                cpCoolDown_data[columnName][newName] = cpCoolDown_data[columnName][copiedPeriod]
                del cpCoolDown_data[columnName][copiedPeriod]
    del tempDict'''





    for columnName, columnData in Period_tb.items():

        if columnName in cpReporting_data['Period Number']:
            newName = str(cpReporting_data['Period Number'][columnName]) + str(':') + str(columnName)
            Period_tb.rename(columns={columnName: newName}, inplace=True)
    Period_data = pnd.DataFrame.to_dict(Period_tb)





    for columnName, columnData in CapLimit_tb.items():

        if columnName in cpReporting_data['Period Number']:
            newName = str(cpReporting_data['Period Number'][columnName]) + str(':') + str(columnName)
            CapLimit_tb.rename(columns={columnName: newName}, inplace=True)
    CapLimit_data = pnd.DataFrame(CapLimit_tb).groupby('Limit_Type').apply(lambda x: x.set_index('Limit_Location').to_dict(orient='index')).to_dict()



    if enableScaling == 'Yes':
        Unit_tb['Qty_Per_Load'] = Unit_tb['Qty_Per_Load'] / scalingVolume
    Unit_tb['SKU_Number'] = Unit_tb['SKU_Number'].astype(str)
    if modelGrpLevel == 'Yes':
        UnitData_tb = Unit_tb.drop(columns=['SKU_Number', 'SKU_Number_Desription'])

        Unit_data = pnd.DataFrame(UnitData_tb).groupby(['SKU_Group', 'SKU_Category', 'Parent_Group', 'Child_Group'], dropna=False).mean().reset_index().set_index('SKU_Group').to_dict()

        UnitMap_tb = Unit_tb.set_index('SKU_Number').drop(columns=['SKU_Number_Desription', 'SKU_Category', 'Qty_Per_Load', 'Days_Cover(Owned)', 'Days_Cover(Outsourced)', 'Parent_Group', 'Child_Group'])
        Unit_mapGrp = pnd.DataFrame(UnitMap_tb).to_dict()
    else:
        Unit_data = pnd.DataFrame(Unit_tb).set_index('SKU_Number').to_dict()

        UnitMap_tb = Unit_tb.set_index('SKU_Group').drop(columns=['SKU_Number_Desription', 'SKU_Category', 'Qty_Per_Load', 'Days_Cover(Owned)', 'Days_Cover(Outsourced)', 'Parent_Group', 'Child_Group'])
        Unit_mapNum = pnd.DataFrame(UnitMap_tb).groupby('SKU_Group', dropna=False).agg({'SKU_Number': lambda x: x.tolist()}).to_dict()


    if enableScaling == 'Yes':
        CmDem_tb.iloc[:,1:CmDem_tb.shape[1]] = CmDem_tb.iloc[:,1:CmDem_tb.shape[1]] / scalingVolume

    CmDem_tb_temp = CmDem_tb.copy(deep=True)

    for periodName in cpReporting_data['Include in Report Period']:
        if cpReporting_data['Include in Report Period'][periodName] == 'No':
            del CmDem_tb[periodName]

    for columnName, columnData in CmDem_tb.items():

        if columnName in cpReporting_data['Period Number'] and cpReporting_data['Include in Report Period'][columnName] == 'Yes':
            newName = str(cpReporting_data['Period Number'][columnName]) + str(':') + str(columnName)
            CmDem_tb.rename(columns={columnName: newName}, inplace=True)


    reportPeriod = list(CmDem_tb)
    reportPeriod.remove('SKU_Number')



    newDemand = cpValSettings_data['VALUE']['Override Demand (M, cases)']
    if pnd.notna(newDemand):

        initialDemand = 0
        new = 0
        for period in reportPeriod:
            initialDemand += CmDem_tb[period].sum()

        for period in reportPeriod:
            if enableScaling == 'Yes':
                CmDem_tb[period] = (CmDem_tb[period] / initialDemand * newDemand * (1000000 / scalingVolume))
            else:
                CmDem_tb[period] = (CmDem_tb[period] / initialDemand * newDemand * (1000000))



    warmUp_lst = []
    coolDown_lst = []

    def addPeriodsColumnFn(lst, data):

        for copiedName in data['Period Number']:
            if pnd.notna(copiedName):
                lst.append(copiedName)

        df_columns = CmDem_tb_temp[lst]
        return df_columns


    warmUp_df = addPeriodsColumnFn(warmUp_lst, cpWarmUp_data).copy()


    position = 1
    cpWarmUpLst = []
    for columnName, columnData in warmUp_df.items():
        warmUp_df[columnName] = (1 + cpWarmUp_data['Volume Escalation'][columnName]) * warmUp_df[columnName]
        newName = cpWarmUp_data['Period Number'][columnName]
        warmUp_df.rename(columns={columnName : newName}, inplace=True)
        CmDem_tb.insert(position, column=newName, value=warmUp_df[newName])
        position += 1

        if newName not in cpWarmUpLst:
            cpWarmUpLst.append(newName)



    coolDown_df = addPeriodsColumnFn(coolDown_lst, cpCoolDown_data).copy()

    for columnName, columnData in coolDown_df.items():
        coolDown_df[columnName] = (1 + cpCoolDown_data['Volume Escalation'][columnName]) * coolDown_df[columnName]
        newName = cpCoolDown_data['Period Number'][columnName]
        coolDown_df.rename(columns={columnName : newName}, inplace=True)

        CmDem_tb[newName] = coolDown_df[newName]


    period_lst = list(CmDem_tb)
    period_lst.remove('SKU_Number')

    del CmDem_tb_temp


    CmDem_tb = pnd.DataFrame(CmDem_tb)
    if modelGrpLevel == 'Yes':
        CmDem_tb['SKU_Number'] = CmDem_tb['SKU_Number'].map(Unit_mapGrp['SKU_Group'])

        CmDem_tb = CmDem_tb.groupby(['Distributor', 'SKU_Number']).sum().reset_index().set_index('Distributor')
        CmDem_data = pnd.DataFrame(CmDem_tb).groupby('Distributor').apply(lambda x: x.set_index('SKU_Number').to_dict(orient='index')).to_dict()
    else:
        CmDem_data = pnd.DataFrame(CmDem_tb).groupby('Distributor').apply(lambda x: x.set_index('SKU_Number').to_dict(orient='index')).to_dict()



    if enableScaling == 'Yes':
        FGInit_tb.iloc[:,1:FGInit_tb.shape[1]] = FGInit_tb.iloc[:,1:FGInit_tb.shape[1]].astype(float) / scalingVolume
    FGInit_tb['SKU_Number'] = FGInit_tb['SKU_Number'].astype(str)
    FGInit_tb = pnd.DataFrame(FGInit_tb)
    if modelGrpLevel == 'Yes':
        FGInit_tb['SKU_Number'] = FGInit_tb['SKU_Number'].map(Unit_mapGrp['SKU_Group'])

        FGInit_tb = FGInit_tb.groupby(['Site', 'SKU_Number']).sum().reset_index().set_index('Site')
        FGInit_data = pnd.DataFrame(FGInit_tb).groupby('Site').apply(lambda x: x.set_index('SKU_Number').to_dict(orient='index')).to_dict()
    else:
        FGInit_data = pnd.DataFrame(FGInit_tb).groupby('Site').apply(lambda x: x.set_index('SKU_Number').to_dict(orient='index')).to_dict()



    if enableScaling == 'Yes':
        WIPInit_tb.iloc[:,1:WIPInit_tb.shape[1]] = WIPInit_tb.iloc[:,1:WIPInit_tb.shape[1]].astype(float) / scalingVolume
    WIPInit_tb['SKU_Number'] = WIPInit_tb['SKU_Number'].astype(str)
    WIPInit_tb = pnd.DataFrame(WIPInit_tb)
    if modelGrpLevel == 'Yes':
        WIPInit_tb['SKU_Number'] = WIPInit_tb['SKU_Number'].map(Unit_mapGrp['SKU_Group'])

        WIPInit_tb = WIPInit_tb.groupby(['Site', 'SKU_Number']).sum().reset_index().set_index('Site')
        WIPInit_data = pnd.DataFrame(WIPInit_tb).groupby('Site').apply(lambda x: x.set_index('SKU_Number').to_dict(orient='index')).to_dict()
    else:
        WIPInit_data = pnd.DataFrame(WIPInit_tb).groupby('Site').apply(lambda x: x.set_index('SKU_Number').to_dict(orient='index')).to_dict()




    if enableScaling == 'Yes':
        PdS_tb['Min_Unit_Qty'] = PdS_tb['Min_Unit_Qty'] / scalingVolume
        PdS_tb['Max_Unit_Qty'] = PdS_tb['Max_Unit_Qty'] / scalingVolume
        PdS_tb['Cost'] = PdS_tb['Cost'] * scalingVolume / scalingCost
    PdS_data = pnd.DataFrame(PdS_tb).groupby('Site').apply(lambda x: x.set_index('Material').to_dict(orient='index')).to_dict()



    if enableScaling == 'Yes':
        PkS_tb['Min_Unit_Qty'] = PkS_tb['Min_Unit_Qty'] / scalingVolume
        PkS_tb['Max_Unit_Qty'] = PkS_tb['Max_Unit_Qty'] / scalingVolume
        PkS_tb['Cost'] = PkS_tb['Cost'] * scalingVolume / scalingCost
    PkS_data = pnd.DataFrame(PkS_tb).groupby('Site').apply(lambda x: x.set_index('Material').to_dict(orient='index')).to_dict()



    if enableScaling == 'Yes':
        rPkS_tb['Min_Unit_Qty'] = rPkS_tb['Min_Unit_Qty'] / scalingVolume
        rPkS_tb['Max_Unit_Qty'] = rPkS_tb['Max_Unit_Qty'] / scalingVolume
        rPkS_tb['Cost'] = rPkS_tb['Cost'] * scalingVolume / scalingCost
    rPkS_data = pnd.DataFrame(rPkS_tb).groupby('Site').apply(lambda x: x.set_index('Material').to_dict(orient='index')).to_dict()



    def expandGrpSKU (dataLst):
        for site in dataLst:
            for ln in dataLst[site]:
                skuDict = {}
                for skuGrp in dataLst[site][ln]:
                    val = dataLst[site][ln][skuGrp]
                    if pnd.isna(val):
                        val = 0
                    for skuNum in Unit_mapNum['SKU_Number'][skuGrp]:
                        skuDict[skuNum] = val
                dataLst[site][ln] = skuDict


    if enableScaling == 'Yes':
        PdCap_tb.iloc[:,1:PdCap_tb.shape[1]] = PdCap_tb.iloc[:,1:PdCap_tb.shape[1]].astype(float) / scalingVolume
    PdCap_data = pnd.DataFrame(PdCap_tb).groupby('Site').apply(lambda x: x.set_index('Stream').to_dict(orient='index')).to_dict()
    if modelGrpLevel == 'No':
        expandGrpSKU(PdCap_data)



    if enableScaling == 'Yes':
        PdGrp_tb['Min_Grp_Qty'] = PdGrp_tb['Min_Grp_Qty'] / scalingVolume
        PdGrp_tb['Min_Grp_Penalty'] = PdGrp_tb['Min_Grp_Penalty'] * scalingVolume / scalingCost
    PdGrp_data = pnd.DataFrame(PdGrp_tb).to_dict()


    PdGrpMap_tb_temp = pnd.DataFrame(PdGrpMap_tb).groupby(['Group', 'Site'])['Stream'].apply(lambda x: x.tolist()).reset_index(level=1)
    PdGrpMap_data = {k : dict(g.values) for k, g in PdGrpMap_tb_temp.groupby(level=0)}


    PdCapEff_data = pnd.DataFrame(PdCapEff_tb).groupby('Site').apply(lambda x: x.set_index('Stream').to_dict(orient='index')).to_dict()

    PdOB_data = pnd.DataFrame(PdOB_tb).to_dict()
    for site in PdOB_data['OB_Sites']:
        lst = PdOB_data['OB_Sites'][site].split(',')
        lstStrip = []
        for item in lst:
            lstStrip.append(item.strip())
        PdOB_data['OB_Sites'][site] = lstStrip


    if enableScaling == 'Yes':
        PdCst_tb.iloc[:,1:PdCst_tb.shape[1]] = PdCst_tb.iloc[:,1:PdCst_tb.shape[1]] * scalingVolume / scalingCost
    PdCst_data = pnd.DataFrame(PdCst_tb).groupby('Site').apply(lambda x: x.set_index('Stream').to_dict(orient='index')).to_dict()
    if modelGrpLevel == 'No':
        expandGrpSKU(PdCst_data)




    if enableScaling == 'Yes':
        PkCap_tb.iloc[:,1:PkCap_tb.shape[1]] = PkCap_tb.iloc[:,1:PkCap_tb.shape[1]].astype(float) / scalingVolume
    PkCap_data = pnd.DataFrame(PkCap_tb).groupby('Site').apply(lambda x: x.set_index('Line').to_dict(orient='index')).to_dict()
    if modelGrpLevel == 'No':
        expandGrpSKU(PkCap_data)



    if enableScaling == 'Yes':
        PkGrp_tb['Min_Grp_Qty'] = PkGrp_tb['Min_Grp_Qty'] / scalingVolume
        PkGrp_tb['Rebate_Min'] = PkGrp_tb['Rebate_Min'] / scalingVolume
        PkGrp_tb['TOP_Penalty'] = PkGrp_tb['TOP_Penalty'] * scalingVolume / scalingCost
        PkGrp_tb['TIER_Penalty'] = PkGrp_tb['TIER_Penalty'] * scalingVolume / scalingCost
        PkGrp_tb['Rebate'] = PkGrp_tb['Rebate'] * scalingVolume / scalingCost
    PkGrp_data = pnd.DataFrame(PkGrp_tb).to_dict()


    PkGrpMap_tb_temp = pnd.DataFrame(PkGrpMap_tb).groupby(['Group', 'Site'])['Line'].apply(lambda x: x.tolist()).reset_index(level=1)
    PkGrpMap_data = {k : dict(g.values) for k, g in PkGrpMap_tb_temp.groupby(level=0)}

    PkCapEff_data = pnd.DataFrame(PkCapEff_tb).groupby('Site').apply(lambda x: x.set_index('Line').to_dict(orient='index')).to_dict()


    PkOB_data = pnd.DataFrame(PkOB_tb).to_dict()
    OBColumn = ['OB_Sites(FG)', 'OB_Sites(WIP)']
    for clm in OBColumn:
        for site in PkOB_data[clm]:
            lst = PkOB_data[clm][site].split(',')
            lstStrip = []
            for item in lst:
                lstStrip.append(item.strip())
            PkOB_data[clm][site] = lstStrip


    PkSiteType_data = pnd.DataFrame(PkSiteType_tb).groupby('Site').apply(lambda x: x.set_index('Line').to_dict(orient='index')).to_dict()


    if enableScaling == 'Yes':
        PkCst_tb.iloc[:,1:PkCst_tb.shape[1]] = PkCst_tb.iloc[:,1:PkCst_tb.shape[1]] * scalingVolume / scalingCost
    PkCst_data = pnd.DataFrame(PkCst_tb).groupby('Site').apply(lambda x: x.set_index('Line').to_dict(orient='index')).to_dict()
    if modelGrpLevel == 'No':
        expandGrpSKU(PkCst_data)


    if enableScaling == 'Yes':
        Min_Batch_tb.iloc[:,1:Min_Batch_tb.shape[1]] = Min_Batch_tb.iloc[:,1:Min_Batch_tb.shape[1]].astype(float) / scalingVolume
    Min_Batch_tb = Min_Batch_tb.fillna(0)
    Min_Batch_data = pnd.DataFrame(Min_Batch_tb).groupby('Pk_Site').apply(lambda x: x.set_index('Line').to_dict(orient='index')).to_dict()
    if modelGrpLevel == 'No':
        expandGrpSKU(Min_Batch_data)


    if enableScaling == 'Yes':
        rPkCap_tb.iloc[:,1:rPkCap_tb.shape[1]] = rPkCap_tb.iloc[:,1:rPkCap_tb.shape[1]].astype(float) / scalingVolume
    rPkCap_data = pnd.DataFrame(rPkCap_tb).groupby('Site').apply(lambda x: x.set_index('Line').to_dict(orient='index')).to_dict()

    if modelGrpLevel == 'No':
        expandGrpSKU(rPkCap_data)



    if enableScaling == 'Yes':
        rPkGrp_tb['Min_Grp_Qty'] = rPkGrp_tb['Min_Grp_Qty'].astype(float) / scalingVolume
        rPkGrp_tb['Min_Grp_Penalty'] = rPkGrp_tb['Min_Grp_Penalty'] * scalingVolume / scalingCost
    rPkGrp_data = pnd.DataFrame(rPkGrp_tb).to_dict()

    rPkGrpMap_tb_temp = pnd.DataFrame(rPkGrpMap_tb).groupby(['Group', 'Site'])['Line'].apply(lambda x: x.tolist()).reset_index(level=1)
    rPkGrpMap_data = {k : dict(g.values) for k, g in rPkGrpMap_tb_temp.groupby(level=0)}


    rPkCapEff_data = pnd.DataFrame(rPkCapEff_tb).groupby('Site').apply(lambda x: x.set_index('Line').to_dict(orient='index')).to_dict()


    rPkOB_data = pnd.DataFrame(rPkOB_tb).to_dict()
    for site in rPkOB_data['OB_Sites']:
        lst = rPkOB_data['OB_Sites'][site].split(',')
        lstStrip = []
        for item in lst:
            lstStrip.append(item.strip())
        rPkOB_data['OB_Sites'][site] = lstStrip


    if enableScaling == 'Yes':
        rPkCst_tb.iloc[:,1:rPkCst_tb.shape[1]] = rPkCst_tb.iloc[:,1:rPkCst_tb.shape[1]] * scalingVolume / scalingCost
    rPkCst_data = pnd.DataFrame(rPkCst_tb).groupby('Site').apply(lambda x: x.set_index('Line').to_dict(orient='index')).to_dict()
    if modelGrpLevel == 'No':
        expandGrpSKU(rPkCst_data)



    if enableScaling == 'Yes':
        WIPCap_tb['Total_Storage'] = WIPCap_tb['Total_Storage'].astype(float) / scalingVolume
    WIPCap_data = pnd.DataFrame(WIPCap_tb).groupby('Site').apply(lambda x: x.set_index('Expansion').to_dict(orient='index')).to_dict()


    WIPOB_data = pnd.DataFrame(WIPOB_tb).to_dict()
    for site in WIPOB_data['OB_Sites']:
        lst = WIPOB_data['OB_Sites'][site].split(',')
        lstStrip = []
        for item in lst:
            lstStrip.append(item.strip())
        WIPOB_data['OB_Sites'][site] = lstStrip

    WIPSiteType_data = pnd.DataFrame(WIPSiteType_tb).groupby('Site').apply(lambda x: x.set_index('Expansion').to_dict(orient='index')).to_dict()


    if enableScaling == 'Yes':
        WIPCst_tb['Handling_Cost'] = WIPCst_tb['Handling_Cost'] * scalingVolume / scalingCost
        WIPCst_tb['Storage_Cost'] = WIPCst_tb['Storage_Cost'] * scalingVolume / scalingCost
    WIPCst_data = pnd.DataFrame(WIPCst_tb).to_dict()



    if enableScaling == 'Yes':
        FGCap_tb['Total_Storage'] = FGCap_tb['Total_Storage'].astype(float) / scalingVolume
    FGCap_data = pnd.DataFrame(FGCap_tb).groupby('Site').apply(lambda x: x.set_index('Expansion').to_dict(orient='index')).to_dict()

    FGSiteType_data = pnd.DataFrame(FGSiteType_tb).groupby('Site').apply(lambda x: x.set_index('Expansion').to_dict(orient='index')).to_dict()


    if enableScaling == 'Yes':
        FGCst_tb['Handling_Cost'] = FGCst_tb['Handling_Cost'] * scalingVolume / scalingCost
        FGCst_tb['Storage_Cost'] = FGCst_tb['Storage_Cost'] * scalingVolume / scalingCost
    FGCst_data = pnd.DataFrame(FGCst_tb).to_dict()

    PdSKU_PkSKU_tb = pnd.DataFrame(PdSKU_PkSKU_tb)
    if modelGrpLevel == 'Yes':
        PdSKU_PkSKU_tb.index = PdSKU_PkSKU_tb.index.map(Unit_mapGrp['SKU_Group'])

        PdSKU_PkSKU_data = PdSKU_PkSKU_tb.groupby(['SKU_Number']).mean().to_dict()
    else:
        PdSKU_PkSKU_data = pnd.DataFrame(PdSKU_PkSKU_tb).to_dict()



    def expandMapGrpSKU (dataLst):
        skuDict = {}
        for skuNum in dataLst:
            skuGrp = Unit_mapGrp['SKU_Group'][skuNum]
            if skuGrp not in skuDict:
                skuDict[skuGrp] = dataLst[skuNum]
            else:
                for skuVPGrp in dataLst[skuNum]:
                    skuDict[skuGrp][skuVPGrp] += dataLst[skuNum][skuVPGrp]
        dataLst.clear()
        dataLst.update(skuDict)
        del skuDict


    PkSKU_rPkSKU_tb = pnd.DataFrame(PkSKU_rPkSKU_tb)
    if modelGrpLevel == 'Yes':

        PkSKU_rPkSKU_tb.index = PkSKU_rPkSKU_tb.index.map(Unit_mapGrp['SKU_Group'])

        PkSKU_rPkSKU_data = PkSKU_rPkSKU_tb.groupby(['SKU_Number']).mean().to_dict()
        expandMapGrpSKU(PkSKU_rPkSKU_data)
    else:
        PkSKU_rPkSKU_data = pnd.DataFrame(PkSKU_rPkSKU_tb).to_dict()



    D_Lanes_tb['Cost'] = D_Lanes_tb['Cost'] / scalingCost
    D_Lanes_data = pnd.DataFrame(D_Lanes_tb).groupby('Origin').apply(lambda x: x.set_index('Destination').to_dict(orient='index')).to_dict()




    def addExclSites(mapDict, siteType, groupType):
        tempMapCopy = copy.deepcopy(mapDict)
        for grp in mapDict:
            for grpSite in mapDict[grp].keys():
                if grpSite in cpSiteExcl_data[siteType]:
                    del tempMapCopy[grp][grpSite]

                if len(tempMapCopy[grp]) == 0 and grpSite in cpSiteExcl_data[siteType] and grp not in cpPenaltyExcl_data[groupType]:
                    cpPenaltyExcl_data[groupType].append(grp)
        del tempMapCopy
    addExclSites(mapDict=PdGrpMap_data, siteType='Pd Site', groupType='Pd Group')
    addExclSites(mapDict=PkGrpMap_data, siteType='Pk Site', groupType='Pk Group')
    addExclSites(mapDict=rPkGrpMap_data, siteType='rPk Site', groupType='rPk Group')



    def mnfCostUpdate(dict, type):
        for s1 in dict:
            for s2 in dict[s1]:
                for s3 in dict[s1][s2]:
                    val = dict[s1][s2][s3]
                    dict[s1][s2][s3] = val * (1 + cpValSettings_data['VALUE'][type])
    mnfCostUpdate(PdCst_data, 'Production Cost Escalation')
    mnfCostUpdate(PkCst_data, 'Packaging Cost Escalation')
    mnfCostUpdate(rPkCst_data, 'RePacking Cost Escalation')


    def distCostUpdate(dict):
        for s1 in dict:
            for s2 in dict[s1]:
                val = dict[s1][s2]['Cost']
                dict[s1][s2]['Cost'] = val * (1 + cpValSettings_data['VALUE']['Distribution Cost Escalation'])
    distCostUpdate(D_Lanes_data)



    for site in cpPenaltyExcl_data['Pd Group']:
        if pnd.notna(site):
            PdGrp_data['Min_Grp_Penalty'][site] = 0
    for site in cpPenaltyExcl_data['Pk Group']:
        if pnd.notna(site):
            PkGrp_data['TOP_Penalty'][site] = 0
            PkGrp_data['TIER_Penalty'][site] = 0
            PkGrp_data['Rebate'][site] = 0
    for site in cpPenaltyExcl_data['rPk Group']:
        if pnd.notna(site):
            rPkGrp_data['Min_Grp_Penalty'][site] = 0


    for site in cpSiteExcl_data['Pd Site']:
        if pnd.notna(site):
            del PdCap_data[site]

            count = 0
            for grp in PdGrpMap_data:
                if site in PdGrpMap_data[grp]:
                    del PdGrpMap_data[grp][site]
                    removedGroup = grp
                    for nestSite in PdGrpMap_data[grp]:
                        count += 1
            if count == 0:
                del PdGrpMap_data[removedGroup]
    for site in cpSiteExcl_data['Pk Site']:
        if pnd.notna(site):
            del PkCap_data[site]

            count = 0
            for grp in PkGrpMap_data:
                if site in PkGrpMap_data[grp]:
                    del PkGrpMap_data[grp][site]
                    removedGroup = grp
                    for nestSite in PkGrpMap_data[grp]:
                        count += 1
            if count == 0:
                del PkGrpMap_data[removedGroup]
    for site in cpSiteExcl_data['rPk Site']:
        if pnd.notna(site):
            del rPkCap_data[site]

            count = 0
            for grp in rPkGrpMap_data:
                if site in rPkGrpMap_data[grp]:
                    del rPkGrpMap_data[grp][site]
                    removedGroup = grp
                    for nestSite in rPkGrpMap_data[grp]:
                        count += 1
            if count == 0:
                del rPkGrpMap_data[removedGroup]
    for site in cpSiteExcl_data['WIP Site']:
        if pnd.notna(site):
            del WIPCap_data[site]
    for site in cpSiteExcl_data['FG Site']:
        if pnd.notna(site):
            del FGCap_data[site]



    PdSSKU_lst = []
    PkSSKU_lst = []
    rPkSSKU_lst = []
    PdSKU_lst = []
    PkSKU_lst = []
    subSKU_lst = []
    nvpSKU_lst = []
    vpSKU_lst = []



    def skuLstPopFn(sku, skuLst):
        if sku not in skuLst:
            skuLst.append(sku)


    for sku in Unit_data['SKU_Category']:
        cat = Unit_data['SKU_Category'][sku]
        if cat == 'Pd RM':
            skuLstPopFn(sku, PdSSKU_lst)
        if cat == 'Pk RM':
            skuLstPopFn(sku, PkSSKU_lst)
        if cat == 'rPk RM':
            skuLstPopFn(sku, rPkSSKU_lst)
        if cat == 'Pd':
            skuLstPopFn(sku, PdSKU_lst)
        if cat == 'SUB' or cat == 'NVP':
            skuLstPopFn(sku, PkSKU_lst)
        if cat == 'SUB':
            skuLstPopFn(sku, subSKU_lst)
        if cat == 'NVP':
            skuLstPopFn(sku, nvpSKU_lst)
        if cat == 'VP':
            skuLstPopFn(sku, vpSKU_lst)


    PkTier_lst = [] #here AG - Tier
    for grp in PkGrp_data['PenType']:
        if grp not in cpPenaltyExcl_data['Pk Group']:
            if PkGrp_data['PenType'][grp] == 'Tier' or PkGrp_data['PenType'][grp] == 'Both':
                PkTier_lst.append(grp)

    PkRb_lst = [] #here AG - Rebate
    for grp in PkGrp_data['PenType']:
        if PkGrp_data['Rebate'][grp] > 0:
            PkRb_lst.append(grp)

    PkMinBatch_lst = []
    for pk in PkCap_data:

        if pk not in cpSiteExcl_data['Pk Site']:
            for ln in PkCap_data[pk]:
                for sku in PkCap_data[pk][ln]:

                    if sku in PkSKU_lst:


                        if PkCap_data[pk][ln][sku] > 0 and Min_Batch_data[pk][ln][sku] > 0 and PkSiteType_data[pk][ln]['Site_Type'] == 'Owned':
                            PkMinBatch_lst.append((pk, ln, sku))
                        if PkCap_data[pk][ln][sku] > 0 and Min_Batch_data[pk][ln][sku] > 0 and PkSiteType_data[pk][ln]['Site_Type'] == 'Outsourced':
                            PkMinBatch_lst.append((pk, ln, sku))


    PkMinBatchP_lst = []
    if warmStart_PkVar == 'Yes' and enableMinBatchSize == 'Yes':
        for [site, line, sku, period] in WarmStartPk_tb:
            if (site, line, sku, period) not in PkMinBatchP_lst:
                PkMinBatchP_lst.append((site, line, sku, period))


    DistVar_FGCmP_lst = []
    if enableLoadSF_MinRnd == 'Yes' and warmStart_DistVar_FGCm == 'Yes' or warmStart_DistVar_FGCmSKU == 'Yes':
        for FG in WarmStartDist_tbD:
            for Cm in WarmStartDist_tbD[FG]:
                if reportDistBinOnly == 'Yes':
                    for period in reportPeriod:
                        if (FG, Cm, period) not in DistVar_FGCmP_lst:
                            DistVar_FGCmP_lst.append((FG, Cm, period))
                else:
                    for period in period_lst:
                        if (FG, Cm, period) not in DistVar_FGCmP_lst:
                            DistVar_FGCmP_lst.append((FG, Cm, period))


    CmSKU_Pk_dict = copy.deepcopy(CmDem_data)
    CmSKU_rPk_dict = copy.deepcopy(CmDem_data)
    SKUCm_dict = {}
    Tot_CmDemand = 0

    maxCmPkFGQty = 0
    maxCmrPkFGQty = 0
    maxCmFGQty = 0
    maxCmLdDict = {}
    activeSKU_lst = []

    for cm in CmDem_data.keys():

        for pd in period_lst:
            maxCmLdDict[cm, pd] = 0
        for sku in CmDem_data[cm]:

            if sku not in nvpSKU_lst:
                del CmSKU_Pk_dict[cm][sku]
                FGtype = 'rPk'
            else:
                FGtype = 'Pk'

            if sku not in vpSKU_lst:
                del CmSKU_rPk_dict[cm][sku]

            if sku not in SKUCm_dict:
                SKUCm_dict[sku] = [cm]
            else:
                SKUCm_dict[sku].append(cm)


            for p in CmDem_data[cm][sku]:
                vol = CmDem_data[cm][sku][p]
                if vol > 0 and sku not in activeSKU_lst:
                    activeSKU_lst.append(sku)
                Tot_CmDemand += vol

                maxCmLdDict[cm, p] += vol / Unit_data['Qty_Per_Load'][sku]

                if vol > maxCmFGQty:
                    maxCmFGQty = vol
                if FGtype == 'Pk' and vol > maxCmPkFGQty:
                    maxCmPkFGQty = vol
                if FGtype == 'rPk' and vol > maxCmrPkFGQty:
                    maxCmrPkFGQty = vol


        if (len(CmSKU_Pk_dict[cm])) == 0:
            del CmSKU_Pk_dict[cm]
        if (len(CmSKU_rPk_dict[cm])) == 0:
            del CmSKU_rPk_dict[cm]



    nvpSKU_templst = copy.deepcopy(nvpSKU_lst)
    vpSKU_templst = copy.deepcopy(vpSKU_lst)

    for sku in nvpSKU_templst:
        if sku not in activeSKU_lst:
            nvpSKU_lst.remove(sku)
    for sku in vpSKU_templst:
        if sku not in activeSKU_lst:
            vpSKU_lst.remove(sku)

    del nvpSKU_templst
    del vpSKU_templst


    maxCmLd = 0
    for cm, p in maxCmLdDict:
        if maxCmLdDict[cm, p] > maxCmLd:
            maxCmLd = maxCmLdDict[cm, p]


    model.PdS = pe.Set(initialize=PdS_data.keys())

    PdSSKU_lst = []
    for pds in PdS_data:
        for mat in PdS_data[pds]:
            if mat not in PdSSKU_lst:
                PdSSKU_lst.append(mat)
    model.PdSSKU = pe.Set(initialize=PdSSKU_lst)


    model.PkS = pe.Set(initialize=PkS_data.keys())
    PkSSKU_lst = []
    for pks in PkS_data:
        for mat in PkS_data[pks]:
            if mat not in PkSSKU_lst:
                PkSSKU_lst.append(mat)
    model.PkSSKU = pe.Set(initialize=PkSSKU_lst)


    model.rPkS = pe.Set(initialize=rPkS_data.keys())
    rPkSSKU_lst = []
    for rpks in rPkS_data:
        for mat in rPkS_data[rpks]:
            if mat not in rPkSSKU_lst:
                rPkSSKU_lst.append(mat)
    model.rPkSSKU = pe.Set(initialize=rPkSSKU_lst)




    model.Pd = pe.Set(initialize=PdCap_data.keys(), ordered=True)
    model.PdSt_arr = pe.Set(model.Pd, initialize=PdCap_data, ordered=True)
    def PdSt_init(m):
        return ((site, line) for site in PdCap_data for line in PdCap_data[site])
    model.PdSt = pe.Set(dimen=2, initialize=PdSt_init, ordered=True)
    model.PdGrp = pe.Set(initialize=PdGrpMap_data.keys(), ordered=True)
    model.PdSKU = pe.Set(initialize=PdSKU_lst, ordered=True)



    model.Pk = pe.Set(initialize=PkCap_data.keys(), ordered=True)
    model.PkLn_arr = pe.Set(model.Pk, initialize=PkCap_data, ordered=True)
    def PkLn_init(m):
        return ((site, line) for site in PkCap_data for line in PkCap_data[site])
    model.PkLn = pe.Set(dimen=2, initialize=PkLn_init, ordered=True)

    model.PkGrp = pe.Set(initialize=PkGrpMap_data.keys(), ordered=True)
    model.PkSKU = pe.Set(initialize=PkSKU_lst, ordered=True)
    model.PkSKU_FG = pe.Set(initialize=nvpSKU_lst, ordered=True)
    model.PkSKU_SUB = pe.Set(initialize=subSKU_lst, ordered=True)

    def PkTierLst_init(m): #here AG - Tier
        return ((group) for group in PkTier_lst)
    model.PkTierLst = pe.Set(dimen=1, initialize=PkTierLst_init, ordered=True)

    def PkRbLst_init(m): #here AG - Rebate
        return ((group) for group in PkRb_lst)
    model.PkRbLst = pe.Set(dimen=1, initialize=PkRbLst_init, ordered=True)

    def PkMinBatchLst_init(m):
        return ((site, line, sku) for site, line, sku in PkMinBatch_lst)
    model.PkMinBatchLst = pe.Set(dimen=3, initialize=PkMinBatchLst_init, ordered=True)


    def PkMinBatchPLst_init(m):
        return ((site, line, sku, period) for site, line, sku, period in PkMinBatchP_lst)
    model.PkMinBatchPLst = pe.Set(dimen=4, initialize=PkMinBatchPLst_init, ordered=True)



    model.rPk = pe.Set(initialize=rPkCap_data.keys(), ordered=True)
    model.rPkLn_arr = pe.Set(model.rPk, initialize=rPkCap_data, ordered=True)
    def rPkLn_init(m):
        return ((site, line) for site in rPkCap_data for line in rPkCap_data[site])
    model.rPkLn = pe.Set(dimen=2, initialize=rPkLn_init, ordered=True)
    model.rPkGrp = pe.Set(initialize=rPkGrpMap_data.keys(), ordered=True)
    model.rPkSKU_FG = pe.Set(initialize=vpSKU_lst, ordered=True)



    model.FG = pe.Set(initialize=FGCap_data.keys())
    def FGPh_init(m):
        return ((site, phase) for site in FGCap_data for phase in FGCap_data[site])
    model.FGPh = pe.Set(dimen=2, initialize=FGPh_init, ordered=True)


    model.WIP = pe.Set(initialize=WIPCap_data.keys())
    def WIPPh_init(m):
        return ((site, phase) for site in WIPCap_data for phase in WIPCap_data[site])
    model.WIPPh = pe.Set(dimen=2, initialize=WIPPh_init, ordered=True)



    model.Cm = pe.Set(initialize=CmDem_data.keys(), ordered=True)

    def CmSKUnest_init(model, Cm):
        return CmDem_data[Cm]
    model.CmSKUnest = pe.Set(model.Cm, initialize=CmSKUnest_init, ordered=True)

    def CmSKU_init(m):
        return ((customer, sku) for customer in CmDem_data for sku in CmDem_data[customer])
    model.CmSKU = pe.Set(dimen=2, initialize=CmSKU_init, ordered=True)

    model.Cm_FG = pe.Set(initialize=CmSKU_Pk_dict.keys(), ordered=True)
    def CmSKU_Pk_init(m):
        return ((customer, sku) for customer in CmSKU_Pk_dict for sku in CmSKU_Pk_dict[customer])
    model.CmSKU_Pk = pe.Set(dimen=2, initialize=CmSKU_Pk_init, ordered=True)

    model.Cm_rFG = pe.Set(initialize=CmSKU_rPk_dict.keys(), ordered=True)
    def CmSKU_rPk_init(m):
        return ((customer, sku) for customer in CmSKU_rPk_dict for sku in CmSKU_rPk_dict[customer])
    model.CmSKU_rPk = pe.Set(dimen=2, initialize=CmSKU_rPk_init, ordered=True)


    def FGCmPLst_init(m):
        return ((FG, Cm, period) for FG, Cm, period in DistVar_FGCmP_lst)
    model.DistVar_FGCmP = pe.Set(dimen=3, initialize=FGCmPLst_init, ordered=True)

    model.SKU = pe.Set(initialize=SKUCm_dict.keys(), ordered=True)
    model.SKUCm_arr = pe.Set(model.SKU, initialize=SKUCm_dict, ordered=True)

    model.SC_SKU = pe.Set(initialize=Unit_data['Qty_Per_Load'].keys(), ordered=True)


    model.period = pe.Set(initialize=period_lst, ordered=True)
    model.reportingPeriod = pe.Set(initialize=reportPeriod, ordered=True)


    def LdQty_fn(model, SKU):
        return Unit_data['Qty_Per_Load'][SKU]
    model.LdQty = pe.Param(model.SC_SKU, initialize=LdQty_fn, domain=pe.NonNegativeReals)

    def hoursCoverFG_fn(model, FG, Ph, SKU):

        if FGSiteType_data[FG][Ph]['Site_Type'] == 'Owned':
            val = Unit_data['Days_Cover(Owned)'][SKU]
        if FGSiteType_data[FG][Ph]['Site_Type'] == 'Outsourced':
            val = Unit_data['Days_Cover(Outsourced)'][SKU]
        if pnd.isna(val):
            val = 0
        return val * 24
    model.hoursCoverFG = pe.Param(model.FGPh, model.SC_SKU, initialize=hoursCoverFG_fn, domain=pe.NonNegativeReals)

    def hoursCoverWIP_fn(model, WIP, Ph, SKU):

        if WIPSiteType_data[WIP][Ph]['Site_Type'] == 'Owned':
            val = Unit_data['Days_Cover(Owned)'][SKU]
        if WIPSiteType_data[WIP][Ph]['Site_Type'] == 'Outsourced':
            val = Unit_data['Days_Cover(Outsourced)'][SKU]
        if pnd.isna(val):
            val = 0
        return val * 24
    model.hoursCoverWIP = pe.Param(model.WIPPh, model.SC_SKU, initialize=hoursCoverWIP_fn, domain=pe.NonNegativeReals)

    def PdSCap_fn(model, PdS, PdSSKU):

        if PdS_data.get(PdS, 0).get(PdSSKU, 0) == 0:
            return 0
        return PdS_data[PdS][PdSSKU]['Max_Unit_Qty']
    model.PdSCap = pe.Param(model.PdS, model.PdSSKU, initialize=PdSCap_fn, domain=pe.NonNegativeReals)

    def PdSCapMin_fn(model, PdS, PdSSKU):

        if PdS_data.get(PdS, 0).get(PdSSKU, 0) == 0:
            return 0
        return PdS_data[PdS][PdSSKU]['Min_Unit_Qty']
    model.PdSCapMin = pe.Param(model.PdS, model.PdSSKU, initialize=PdSCapMin_fn, domain=pe.NonNegativeReals)

    def PkSCap_fn(model, PkS, PkSSKU):

        if PkS_data.get(PkS, 0).get(PkSSKU, 0) == 0:
            return 0
        return PkS_data[PkS][PkSSKU]['Max_Unit_Qty']
    model.PkSCap = pe.Param(model.PkS, model.PkSSKU, initialize=PkSCap_fn, domain=pe.NonNegativeReals)

    def PkSCapMin_fn(model, PkS, PkSSKU):

        if PkS_data.get(PkS, 0).get(PkSSKU, 0) == 0:
            return 0
        return PkS_data[PkS][PkSSKU]['Min_Unit_Qty']
    model.PkSCapMin = pe.Param(model.PkS, model.PkSSKU, initialize=PkSCapMin_fn, domain=pe.NonNegativeReals)

    def rPkSCap_fn(model, rPkS, rPkSSKU):

        if rPkS_data.get(rPkS, 0).get(rPkSSKU, 0) == 0:
            return 0
        return rPkS_data[rPkS][rPkSSKU]['Max_Unit_Qty']
    model.rPkSCap = pe.Param(model.rPkS, model.rPkSSKU, initialize=rPkSCap_fn, domain=pe.NonNegativeReals)

    def rPkSCapMin_fn(model, rPkS, rPkSSKU):

        if rPkS_data.get(rPkS, 0).get(rPkSSKU, 0) == 0:
            return 0
        return rPkS_data[rPkS][rPkSSKU]['Min_Unit_Qty']
    model.rPkSCapMin = pe.Param(model.rPkS, model.rPkSSKU, initialize=rPkSCapMin_fn, domain=pe.NonNegativeReals)

    def PdCap_fn(model, Pd, St, PdSKU):
        val = PdCap_data[Pd][St][PdSKU]
        if pnd.isna(val):
            val = 0
        return val * PdCapEff_data[Pd][St]['OEE'] * PdCapEff_data[Pd][St]['Period_Availability']
    model.PdCap = pe.Param(model.PdSt, model.PdSKU, initialize=PdCap_fn, domain=pe.NonNegativeReals)

    def PdGrpMin_fn(model, Grp):
        val = PdGrp_data['Min_Grp_Qty'][Grp]
        if pnd.isna(val):
            val = 0
        return val
    model.PdGrpMin = pe.Param(model.PdGrp, initialize=PdGrpMin_fn, domain=pe.NonNegativeReals)

    def PkCap_fn(model, Pk, Ln, PkSKU):
        val = PkCap_data[Pk][Ln][PkSKU]
        if pnd.isna(val):
            val = 0
        return val * PkCapEff_data[Pk][Ln]['OEE'] * PkCapEff_data[Pk][Ln]['Period_Availability'] * PkCapTempRedFactor
    model.PkCap = pe.Param(model.PkLn, model.PkSKU, initialize=PkCap_fn, domain=pe.NonNegativeReals)


    def PkGrpMin_fn(model, Grp):
        val = PkGrp_data['Min_Grp_Qty'][Grp]
        if pnd.isna(val):
            val = 0
        return val
    model.PkGrpMin = pe.Param(model.PkGrp, initialize=PkGrpMin_fn, domain=pe.NonNegativeReals)

    def PkGrpRebateMin_fn(model, Grp): #here AG - Rebate
        val = PkGrp_data['Rebate_Min'][Grp]
        if pnd.isna(val):
            val = 0
        return val
    model.PkGrpRebateMin = pe.Param(model.PkGrp, initialize=PkGrpRebateMin_fn, domain=pe.NonNegativeReals)

    def rPkCap_fn(model, rPk, Ln, SKU):
        val = rPkCap_data[rPk][Ln][SKU]
        if pnd.isna(val):
            val = 0
        return val * rPkCapEff_data[rPk][Ln]['OEE'] * rPkCapEff_data[rPk][Ln]['Period_Availability']
    model.rPkCap = pe.Param(model.rPkLn, model.rPkSKU_FG, initialize=rPkCap_fn, domain=pe.NonNegativeReals)

    def rPkGrpMin_fn(model, Grp):
        val = rPkGrp_data['Min_Grp_Qty'][Grp]
        if pnd.isna(val):
            val = 0
        return val
    model.rPkGrpMin = pe.Param(model.rPkGrp, initialize=rPkGrpMin_fn, domain=pe.NonNegativeReals)



    def WIPCap_fn(model, WIP):
        if WIPCap_data.get(WIP, 0).get("Ph1", 0) == 0:
            return 0
        return WIPCap_data[WIP]["Ph1"]["Total_Storage"]
    model.WIPCap = pe.Param(model.WIP, initialize=WIPCap_fn, domain=pe.NonNegativeReals)

    def initialWIPQty_fn(model, WIP, SKU):
        val = 0
        if WIP in WIPInit_data:
            if SKU in WIPInit_data[WIP]:
                val = WIPInit_data[WIP][SKU]['Initial_Qty']
                if pnd.isna(val):
                    val = 0
        return val
    model.initialWIPQty = pe.Param(model.WIP, model.PkSKU_SUB, initialize=initialWIPQty_fn, domain=pe.NonNegativeReals)



    def FGCap_fn(model, FG):
        if FGCap_data.get(FG, 0).get("Ph1", 0) == 0:
            return 0
        return FGCap_data[FG]["Ph1"]["Total_Storage"]
    model.FGCap = pe.Param(model.FG, initialize=FGCap_fn, domain=pe.NonNegativeReals)

    def initialFGQty_fn(model, FG, SKU):
        val = 0
        if FG in FGInit_data:
            if SKU in FGInit_data[FG]:
                val = FGInit_data[FG][SKU]['Initial_Qty']
                if pnd.isna(val):
                    val = 0
        return val
    model.initialFGQty = pe.Param(model.FG, model.SKU, initialize=initialFGQty_fn, domain=pe.NonNegativeReals)



    def CmDem_fn(model, Cm, SKU, period):
        return CmDem_data[Cm][SKU][period]
    model.CmDem = pe.Param(model.CmSKU, model.period, initialize=CmDem_fn, domain=pe.NonNegativeReals)





    def D_CostExclDunnage_fn(model, origin, destination):
        return D_Lanes_data[origin][destination]['Cost']
    def D_CostTankering_fn(model, origin, destination):
        return D_Lanes_data[origin][destination]['Cost'] * (1 + cpValSettings_data['VALUE']['Tankering Modification'])
    def D_CostInclDunnage_fn(model, origin, destination):
        routeDist = D_Lanes_data[origin][destination]['Mileage']
        if routeDist > dunnage_dist:
            return D_Lanes_data[origin][destination]['Cost'] + dunnage_cost
        else:
            return D_Lanes_data[origin][destination]['Cost']

    model.PdS_Pd = pe.Param(model.PdS, model.Pd, initialize=D_CostExclDunnage_fn, domain=pe.NonNegativeReals)
    model.PkS_Pk = pe.Param(model.PkS, model.Pk, initialize=D_CostExclDunnage_fn, domain=pe.NonNegativeReals)
    model.rPkS_rPk = pe.Param(model.rPkS, model.rPk, initialize=D_CostExclDunnage_fn, domain=pe.NonNegativeReals)
    model.Pd_Pk = pe.Param(model.Pd, model.Pk, initialize=D_CostTankering_fn, domain=pe.NonNegativeReals)
    if Pk_Cm_Route == "Yes":
        model.Pk_Cm = pe.Param(model.Pk, model.Cm, initialize=D_CostInclDunnage_fn, domain=pe.NonNegativeReals)
    model.Pk_FG = pe.Param(model.Pk, model.FG, initialize=D_CostInclDunnage_fn, domain=pe.NonNegativeReals)
    model.Pk_WIP = pe.Param(model.Pk, model.WIP, initialize=D_CostInclDunnage_fn, domain=pe.NonNegativeReals)
    model.WIP_rPk = pe.Param(model.WIP, model.rPk, initialize=D_CostInclDunnage_fn, domain=pe.NonNegativeReals)
    model.rPk_FG = pe.Param(model.rPk, model.FG, initialize=D_CostInclDunnage_fn, domain=pe.NonNegativeReals)
    model.FG_Cm = pe.Param(model.FG, model.Cm, initialize=D_CostInclDunnage_fn, domain=pe.NonNegativeReals)



    def PdSCst_fn(model, PdS, PdSSKU):
        if PdS_data.get(PdS, 0).get(PdSSKU, 0) == 0:
            return 0
        return PdS_data[PdS][PdSSKU]['Cost']
    model.PdSCst = pe.Param(model.PdS, model.PdSSKU, initialize=PdSCst_fn, domain=pe.NonNegativeReals)

    def PkSCst_fn(model, PkS, PkSSKU):
        if PkS_data.get(PkS, 0).get(PkSSKU, 0) == 0:
            return 0
        return PkS_data[PkS][PkSSKU]['Cost']
    model.PkSCst = pe.Param(model.PkS, model.PkSSKU, initialize=PkSCst_fn, domain=pe.NonNegativeReals)

    def rPkSCst_fn(model, rPkS, rPkSSKU):
        if rPkS_data.get(rPkS, 0).get(rPkSSKU, 0) == 0:
            return 0
        return rPkS_data[rPkS][rPkSSKU]['Cost']
    model.rPkSCst = pe.Param(model.rPkS, model.rPkSSKU, initialize=rPkSCst_fn, domain=pe.NonNegativeReals)

    def PdCst_fn(model, Pd, St, PdSKU):
        val = PdCst_data[Pd][St][PdSKU]
        if pnd.isna(val):
            return 0
        return val
    model.PdCst = pe.Param(model.PdSt, model.PdSKU, initialize=PdCst_fn, domain=pe.NonNegativeReals)

    def PdSFCst_fn(model, Grp):
        val = PdGrp_data['Min_Grp_Penalty'][Grp]
        if pnd.isna(val):
            val = 0
        return val
    model.PdSFCst = pe.Param(model.PdGrp, initialize=PdSFCst_fn, domain=pe.NonNegativeReals)

    def PkCst_fn(model, Pk, Ln, PkSKU):
        val = PkCst_data[Pk][Ln][PkSKU]
        if pnd.isna(val):
            return 0
        return val
    model.PkCst = pe.Param(model.PkLn, model.PkSKU, initialize=PkCst_fn, domain=pe.NonNegativeReals)

    def PkSFCst_fn(model, Grp): #here AG - TOP
        val = PkGrp_data['TOP_Penalty'][Grp]
        penType = PkGrp_data['PenType'][Grp]
        if pnd.isna(val):
            val = 0
        if penType != 'Both' and penType != 'TakeOrPay':
            val = 0
        return val
    model.PkSFCst = pe.Param(model.PkGrp, initialize=PkSFCst_fn, domain=pe.NonNegativeReals)

    def PkRCst_fn(model, Grp): #here AG - Rebate
        val = PkGrp_data['Rebate'][Grp]
        if pnd.isna(val):
            val = 0
        return val
    model.PkRCst = pe.Param(model.PkGrp, initialize=PkRCst_fn, domain=pe.NonNegativeReals)

    def PkSFTCst_fn(model, Grp): #here AG - Tier
        val = PkGrp_data['TIER_Penalty'][Grp]
        penType = PkGrp_data['PenType'][Grp]
        if pnd.isna(val):
            val = 0
        if penType != 'Both' and penType != 'Tier':
            val = 0
        return val
    model.PkSFTCst = pe.Param(model.PkGrp, initialize=PkSFTCst_fn, domain=pe.NonNegativeReals)

    def rPkCst_fn(model, rPk, Ln, SKU):
        val = rPkCst_data[rPk][Ln][SKU]
        if pnd.isna(val):
            return 0
        return val
    model.rPkCst = pe.Param(model.rPkLn, model.rPkSKU_FG, initialize=rPkCst_fn, domain=pe.NonNegativeReals)

    def rPkSFCst_fn(model, Grp):
        val = rPkGrp_data['Min_Grp_Penalty'][Grp]
        if pnd.isna(val):
            val = 0
        return val
    model.rPkSFCst = pe.Param(model.rPkGrp, initialize=rPkSFCst_fn, domain=pe.NonNegativeReals)

    def FGCstH_fn(model, FG):
        return FGCst_data["Handling_Cost"][FG]
    model.FGCstH = pe.Param(model.FG, initialize=FGCstH_fn, domain=pe.NonNegativeReals)

    def FGCstS_fn(model, FG):
        return FGCst_data["Storage_Cost"][FG]
    model.FGCstS = pe.Param(model.FG, initialize=FGCstS_fn, domain=pe.NonNegativeReals)

    def WIPCstH_fn(model, WIP):
        return WIPCst_data["Handling_Cost"][WIP]
    model.WIPCstH = pe.Param(model.WIP, initialize=WIPCstH_fn, domain=pe.NonNegativeReals)

    def WIPCstS_fn(model, WIP):
        return WIPCst_data["Storage_Cost"][WIP]
    model.WIPCstS = pe.Param(model.WIP, initialize=WIPCstS_fn, domain=pe.NonNegativeReals)





    def PdSSKU_PdSKU_fn(model, PdSSKU, PdSKU):
        return PdSSKU_PdSKU_data[PdSSKU][PdSKU] if modelGrpLevel == 'Yes' else PdSSKU_PdSKU_data[PdSSKU][Unit_data['SKU_Group'][PdSKU]]
    model.PdSSKU_PdSKU = pe.Param(model.PdSSKU, model.PdSKU, initialize = PdSSKU_PdSKU_fn, domain = pe.NonNegativeReals)


    def PkSSKU_PkSKU_fn(model, PkSSKU, PkSKU):
        return PkSSKU_PkSKU_data[PkSSKU][PkSKU] if modelGrpLevel == 'Yes' else PkSSKU_PkSKU_data[PkSSKU][Unit_data['SKU_Group'][PkSKU]]
    model.PkSSKU_PkSKU = pe.Param(model.PkSSKU, model.PkSKU, initialize = PkSSKU_PkSKU_fn, domain = pe.NonNegativeReals)


    def rPkSSKU_rPkSKU_fn(model, rPkSSKU, rPkSKU):
        return rPkSSKU_rPkSKU_data[rPkSSKU][rPkSKU] if modelGrpLevel == 'Yes' else rPkSSKU_rPkSKU_data[rPkSSKU][Unit_data['SKU_Group'][rPkSKU]]
    model.rPkSSKU_rPkSKU = pe.Param(model.rPkSSKU, model.rPkSKU_FG, initialize = rPkSSKU_rPkSKU_fn, domain = pe.NonNegativeReals)

    def PdSKU_PkSKU_fn(model, PdSKU, PkSKU):
        return PdSKU_PkSKU_data[PdSKU][PkSKU]
    model.PdSKU_PkSKU = pe.Param(model.PdSKU, model.PkSKU, initialize = PdSKU_PkSKU_fn, domain = pe.NonNegativeReals)

    def PkSKU_rPkSKU_fn(model, PkSKU_SUB, SKU):
        return PkSKU_rPkSKU_data[PkSKU_SUB][SKU]
    model.PkSKU_rPkSKU = pe.Param(model.PkSKU_SUB, model.rPkSKU_FG, initialize = PkSKU_rPkSKU_fn, domain = pe.NonNegativeReals)


    def periodLen_fn(model, period):
        return Period_data[period]["Period_Length"]
    model.periodLen = pe.Param(model.period, initialize = periodLen_fn, domain = pe.NonNegativeReals)

    def batchSizeMult_fn(model, period):
        return Period_data[period]["Batch_Size_Multiplier"]
    model.batchSizeMult = pe.Param(model.period, initialize = batchSizeMult_fn, domain = pe.NonNegativeReals)





    model.x_PdS_Pd = pe.Var(model.PdS, model.Pd, model.PdSSKU, model.period, domain=pe.NonNegativeReals, bounds=(0, None))
    model.x_PkS_Pk = pe.Var(model.PkS, model.Pk, model.PkSSKU, model.period, domain=pe.NonNegativeReals, bounds=(0, None))
    model.x_rPkS_rPk = pe.Var(model.rPkS, model.rPk, model.rPkSSKU, model.period, domain=pe.NonNegativeReals, bounds=(0, None))
    model.x_Pd_Pk = pe.Var(model.Pd, model.Pk, model.PdSKU, model.period, domain=pe.NonNegativeReals, bounds=(0, None))
    if Pk_Cm_Route == "Yes":
        model.x_Pk_Cm = pe.Var(model.Pk, model.CmSKU_Pk, model.period, domain=pe.NonNegativeReals, bounds=(0, None))
    model.x_Pk_FG = pe.Var(model.Pk, model.FG, model.PkSKU_FG, model.period, domain=pe.NonNegativeReals, bounds=(0, None))
    model.x_Pk_WIP = pe.Var(model.Pk, model.WIP, model.PkSKU_SUB, model.period, domain=pe.NonNegativeReals, bounds=(0, None))
    model.x_WIP_rPk = pe.Var(model.WIP, model.rPk, model.PkSKU_SUB, model.period, domain=pe.NonNegativeReals, bounds=(0, None))
    model.x_rPk_FG = pe.Var(model.rPk, model.FG, model.rPkSKU_FG, model.period, domain=pe.NonNegativeReals, bounds=(0, None))
    model.x_FG_Cm = pe.Var(model.FG, model.CmSKU, model.period, domain=pe.NonNegativeReals, bounds=(0, None))



    if enableLoadSF_Full == 'Yes':

        if Pk_Cm_Route == "Yes":
            model.x_Pk_Cm_Lds = pe.Var(model.Pk, model.Cm, model.period, domain=pe.NonNegativeIntegers, bounds=(0, None))
        model.x_FG_Cm_Lds = pe.Var(model.FG, model.Cm, model.period, domain=pe.NonNegativeIntegers, bounds=(0, None))

        if Pk_Cm_Route == "Yes":
            model.x_Pk_Cm_LSF = pe.Var(model.Pk, model.Cm, model.period, domain=pe.NonNegativeReals, bounds=(0, 1))
        model.x_FG_Cm_LSF = pe.Var(model.FG, model.Cm, model.period, domain=pe.NonNegativeReals, bounds=(0, 1))


    if enableLoadSF_MinFract == 'Yes' or enableLoadSF_MinRnd == 'Yes':

        if Pk_Cm_Route == "Yes":
            model.x_Pk_Cm_Lds = pe.Var(model.Pk, model.Cm, model.period, domain=pe.NonNegativeReals, bounds=(0, None))


        if Pk_Cm_Route == "Yes":
            if reportDistBinOnly == 'Yes':
                model.x_Pk_Cm_LdBinary = pe.Var(model.Pk, model.Cm, model.reportingPeriod, domain=pe.Binary, initialize=0)
            else:
                model.x_Pk_Cm_LdBinary = pe.Var(model.Pk, model.Cm, model.period, domain=pe.Binary, initialize=0)


        if reportDistBinOnly == 'Yes' and warmStart_DistVar_FGCm == 'No' and warmStart_DistVar_FGCmSKU == 'No':
            model.x_FG_Cm_LdBinary = pe.Var(model.FG, model.Cm, model.reportingPeriod, domain=pe.Binary, initialize=0)
            model.x_FG_Cm_Lds = pe.Var(model.FG, model.Cm, model.reportingPeriod, domain=pe.NonNegativeReals, bounds=(0, None))
        elif reportDistBinOnly == 'No' and warmStart_DistVar_FGCm == 'No' and warmStart_DistVar_FGCmSKU == 'No':
            model.x_FG_Cm_LdBinary = pe.Var(model.FG, model.Cm, model.period, domain=pe.Binary, initialize=0)
            model.x_FG_Cm_Lds = pe.Var(model.FG, model.Cm, model.period, domain=pe.NonNegativeReals, bounds=(0, None))
        else:

            model.x_FG_Cm_LdBinary = pe.Var(model.DistVar_FGCmP, domain=pe.Binary, initialize=0)
            model.x_FG_Cm_Lds = pe.Var(model.DistVar_FGCmP, domain=pe.NonNegativeReals, bounds=(0, None))



        if enableLoadSF_MinFract == 'Yes':

            if Pk_Cm_Route == "Yes":
                model.x_Pk_Cm_LSF = pe.Var(model.Pk, model.Cm, model.period, domain=pe.NonNegativeReals, bounds=(0, 1))
            model.x_FG_Cm_LSF = pe.Var(model.FG, model.Cm, model.period, domain=pe.NonNegativeReals, bounds=(0, 1))



    model.x_PdStQ = pe.Var(model.PdSt, model.PdSKU, model.period, domain=pe.NonNegativeReals, bounds=(0, None))
    model.x_PkLnQ = pe.Var(model.PkLn, model.PkSKU, model.period, domain=pe.NonNegativeReals, bounds=(0, None))
    model.x_rPkLnQ = pe.Var(model.rPkLn, model.rPkSKU_FG, model.period, domain=pe.NonNegativeReals, bounds=(0, None))


    if enableMinBatchSize == "Yes":

        if warmStart_PkVar == 'Yes':
            model.x_PkBatchBinary = pe.Var(model.PkMinBatchPLst, domain=pe.Binary, initialize=0)
        else:
            model.x_PkBatchBinary = pe.Var(model.PkMinBatchLst, model.period, domain=pe.Binary, initialize=0)



    model.x_PdGrpSFQ = pe.Var(model.PdGrp, model.period, domain=pe.NonNegativeReals, bounds=(0, None))

    model.x_PkGrpSFQ = pe.Var(model.PkGrp, model.period, domain=pe.NonNegativeReals, bounds=(0, None)) #here AG - TOP
    model.x_rPkGrpSFQ = pe.Var(model.rPkGrp, model.period, domain=pe.NonNegativeReals, bounds=(0, None))
    model.x_PkGrpTSFQ_Bin = pe.Var(model.PkTierLst, model.period, domain=pe.Binary, initialize=0) #here AG - Tier
    model.x_PkGrpRQ = pe.Var(model.PkGrp, model.period, domain=pe.NonNegativeReals, bounds=(0, None)) #here AG - Rebate
    model.x_PkGrpRQ_Bin = pe.Var(model.PkRbLst, model.period, domain=pe.Binary, initialize=0)  # here AG - Rebate

    model.x_FGSQ = pe.Var(model.FG, model.SKU, model.period, domain=pe.NonNegativeReals, bounds=(0, None))
    model.x_WIPSQ = pe.Var(model.WIP, model.PkSKU_SUB, model.period, domain=pe.NonNegativeReals, bounds=(0, None))
    model.x_FGSQ_init = pe.Var(model.FG, model.SKU, domain=pe.NonNegativeReals, bounds=(0, None))
    model.x_WIPSQ_init = pe.Var(model.WIP, model.PkSKU_SUB, domain=pe.NonNegativeReals, bounds=(0, None))


    if enableWIPInitialStockPenalty == 'Yes':
        model.x_WIPSQ_initPenalty = pe.Var(model.WIP, model.PkSKU_SUB, domain=pe.NonNegativeReals, bounds=(0, None))




    def PdS_Capacity_Rule(model, PdS, PdSSKU, period):
      return sum(model.x_PdS_Pd[PdS, Pd, PdSSKU, period] for Pd in model.Pd) <= \
             model.PdSCap[PdS, PdSSKU] * model.periodLen[period]
    model.PdS_Capacity = pe.Constraint(model.PdS, model.PdSSKU, model.period, rule=PdS_Capacity_Rule)


    def PdS_CapacityMin_Rule(model, PdS, PdSSKU, period):
      return sum(model.x_PdS_Pd[PdS, Pd, PdSSKU, period] for Pd in model.Pd) >= \
             model.PdSCapMin[PdS, PdSSKU] * model.periodLen[period]
    model.PdS_CapacityMin = pe.Constraint(model.PdS, model.PdSSKU, model.period, rule=PdS_CapacityMin_Rule)


    def PkS_Capacity_Rule(model, PkS, PkSSKU, period):
      return sum(model.x_PkS_Pk[PkS, Pk, PkSSKU, period] for Pk in model.Pk) <= \
             model.PkSCap[PkS, PkSSKU] * model.periodLen[period]
    model.PkS_Capacity = pe.Constraint(model.PkS, model.PkSSKU, model.period, rule=PkS_Capacity_Rule)


    def PkS_CapacityMin_Rule(model, PkS, PkSSKU, period):
      return sum(model.x_PkS_Pk[PkS, Pk, PkSSKU, period] for Pk in model.Pk) >= \
             model.PkSCapMin[PkS, PkSSKU] * model.periodLen[period]
    model.PkS_CapacityMin = pe.Constraint(model.PkS, model.PkSSKU, model.period, rule=PkS_CapacityMin_Rule)


    def rPkS_Capacity_Rule(model, rPkS, rPkSSKU, period):
      return sum(model.x_rPkS_rPk[rPkS, rPk, rPkSSKU, period] for rPk in model.rPk) <= \
             model.rPkSCap[rPkS, rPkSSKU] * model.periodLen[period]
    model.rPkS_Capacity = pe.Constraint(model.rPkS, model.rPkSSKU, model.period, rule=rPkS_Capacity_Rule)


    def rPkS_CapacityMin_Rule(model, rPkS, rPkSSKU, period):
      return sum(model.x_rPkS_rPk[rPkS, rPk, rPkSSKU, period] for rPk in model.rPk) >= \
             model.rPkSCapMin[rPkS, rPkSSKU] * model.periodLen[period]
    model.rPkS_CapacityMin = pe.Constraint(model.rPkS, model.rPkSSKU, model.period, rule=rPkS_CapacityMin_Rule)




    def Pd_RMFlow_Rule(model, Pd, PdSSKU, period):
        return sum(model.PdSSKU_PdSKU[PdSSKU, SKU] * model.x_Pd_Pk[Pd, Pk, SKU, period] for Pk in model.Pk for SKU in model.PdSKU) <= \
               sum(model.x_PdS_Pd[PdS, Pd, PdSSKU, period] for PdS in model.PdS)
    model.Pd_RMFlow = pe.Constraint(model.Pd, model.PdSSKU, model.period, rule=Pd_RMFlow_Rule)


    def Pd_StCapability_Rule(model, Pd, St, SKU, period):
        if model.PdCap[Pd, St, SKU] == 0:
                return model.x_PdStQ[Pd, St, SKU, period] == 0
        else:
            return model.x_PdStQ[Pd, St, SKU, period] >= 0
    model.Pd_StCapability = pe.Constraint(model.PdSt, model.PdSKU, model.period, rule=Pd_StCapability_Rule)


    def Pd_StCapacity_Rule(model, Pd, St, period):
        hrs = 0
        for SKU in model.PdSKU:
            if model.PdCap[Pd, St, SKU] == 0:
                hrs += 0
            else:
                hrs += (model.x_PdStQ[Pd, St, SKU, period] / model.PdCap[Pd, St, SKU])

        if 'Pd' in CapLimit_data and (Pd + '>' + St) in CapLimit_data['Pd']:
            limit = CapLimit_data['Pd'][Pd + '>' + St][period]
        else:
            limit = 1
        return hrs <= model.periodLen[period] * limit
    model.Pd_StCapacity = pe.Constraint(model.PdSt, model.period, rule=Pd_StCapacity_Rule)


    def Pd_Capacity_Rule(model, Pd, PdSKU, period):
      return sum(model.x_Pd_Pk[Pd, Pk, PdSKU, period] for Pk in model.Pk) <= \
             sum(model.x_PdStQ[Pd, St, PdSKU, period] for St in model.PdSt_arr[Pd])
    model.Pd_Capacity = pe.Constraint(model.Pd, model.PdSKU, model.period, rule=Pd_Capacity_Rule)


    def Pd_InputConstraint_Rule(model, Pd, period):
        if 'Pd' in cpConstraints_data and Pd in cpConstraints_data['Pd']:
            val = cpConstraints_data['Pd'][Pd]['Value (per Period)']
            if cpConstraints_data['Pd'][Pd]['Operator'] == '<':
                return sum(model.x_PdStQ[Pd, St, SKU, period] for St in model.PdSt_arr[Pd] for SKU in model.PdSKU) < val
            if cpConstraints_data['Pd'][Pd]['Operator'] == '<=':
                return sum(model.x_PdStQ[Pd, St, SKU, period] for St in model.PdSt_arr[Pd] for SKU in model.PdSKU) <= val
            if cpConstraints_data['Pd'][Pd]['Operator'] == '>':
                return sum(model.x_PdStQ[Pd, St, SKU, period] for St in model.PdSt_arr[Pd] for SKU in model.PdSKU) > val
            if cpConstraints_data['Pd'][Pd]['Operator'] == '>=':
                return sum(model.x_PdStQ[Pd, St, SKU, period] for St in model.PdSt_arr[Pd] for SKU in model.PdSKU) >= val
            if cpConstraints_data['Pd'][Pd]['Operator'] == '==':
                return sum(model.x_PdStQ[Pd, St, SKU, period] for St in model.PdSt_arr[Pd] for SKU in model.PdSKU) == val
        else:
            return sum(model.x_PdStQ[Pd, St, SKU, period] for St in model.PdSt_arr[Pd] for SKU in model.PdSKU) >= 0
    model.Pd_InputConstraint = pe.Constraint(model.Pd, model.period, rule=Pd_InputConstraint_Rule)


    def Pd_Shortfall_Rule(model, Grp, period):
        return sum(model.x_PdStQ[Pd, St, PdSKU, period] for Pd in PdGrpMap_data[Grp] for St in PdGrpMap_data[Grp][Pd] for PdSKU in model.PdSKU) >= \
                (model.PdGrpMin[Grp] * model.periodLen[period]) - model.x_PdGrpSFQ[Grp, period]
    model.Pd_Shortfall = pe.Constraint(model.PdGrp, model.period, rule=Pd_Shortfall_Rule)


    def Pd_Outbound_Rule(model, Pd, Pk, PdSKU, period):
        if 'All' in PdOB_data['OB_Sites'][Pd]:
            return model.x_Pd_Pk[Pd, Pk, PdSKU, period] >= 0
        elif Pk in PdOB_data['OB_Sites'][Pd]:
            return model.x_Pd_Pk[Pd, Pk, PdSKU, period] >= 0
        else:
            return model.x_Pd_Pk[Pd, Pk, PdSKU, period] == 0
    model.Pd_Outbound = pe.Constraint(model.Pd, model.Pk, model.PdSKU, model.period, rule=Pd_Outbound_Rule)













    def Pk_RMFlow_Rule(model, Pk, PkSSKU, period):
        i = 0
        if Pk_Cm_Route == "Yes":
            i = sum(model.PkSSKU_PkSKU[PkSSKU, SKU] * model.x_Pk_Cm[Pk, Cm, SKU, period] for Cm, SKU in model.CmSKU_Pk)
        return i + \
            sum(model.PkSSKU_PkSKU[PkSSKU, SKU] * model.x_Pk_FG[Pk, FG, SKU, period] for FG in model.FG for SKU in model.PkSKU_FG) + \
            sum(model.PkSSKU_PkSKU[PkSSKU, SKU] * model.x_Pk_WIP[Pk, WIP, SKU, period] for WIP in model.WIP for SKU in model.PkSKU_SUB) <= \
            sum(model.x_PkS_Pk[PkS, Pk, PkSSKU, period] for PkS in model.PkS)
    model.Pk_RMFlow = pe.Constraint(model.Pk, model.PkSSKU, model.period, rule=Pk_RMFlow_Rule)


    def Pk_Flow_Rule(model, Pk, PdSKU, period):
        i = 0
        if Pk_Cm_Route == "Yes":
            i = sum(model.PdSKU_PkSKU[PdSKU, SKU] * model.x_Pk_Cm[Pk, Cm, SKU, period] for Cm, SKU in model.CmSKU_Pk)
        return i + \
             sum(model.PdSKU_PkSKU[PdSKU, SKU] * model.x_Pk_FG[Pk, FG, SKU, period] for FG in model.FG for SKU in model.PkSKU_FG) + \
             sum(model.PdSKU_PkSKU[PdSKU, SKU] * model.x_Pk_WIP[Pk, WIP, SKU, period] for WIP in model.WIP for SKU in model.PkSKU_SUB) <= \
             sum(model.x_Pd_Pk[Pd, Pk, PdSKU, period] for Pd in model.Pd)
    model.Pk_Flow = pe.Constraint(model.Pk, model.PdSKU, model.period, rule=Pk_Flow_Rule)


    def Pk_LnCapability_Rule(model, Pk, Ln, SKU, period):
        if model.PkCap[Pk, Ln, SKU] == 0:
                return model.x_PkLnQ[Pk, Ln, SKU, period] == 0
        else:
            return model.x_PkLnQ[Pk, Ln, SKU, period] >= 0
    model.Pk_LnCapability = pe.Constraint(model.PkLn, model.PkSKU, model.period, rule=Pk_LnCapability_Rule)


    def Pk_LnCapacity_Rule(model, Pk, Ln, period):
        hrs = 0
        for SKU in model.PkSKU:
            if model.PkCap[Pk, Ln, SKU] == 0:
                hrs += 0
            else:
                hrs += (model.x_PkLnQ[Pk, Ln, SKU, period] / model.PkCap[Pk, Ln, SKU])

        if 'Pk' in CapLimit_data and (Pk + '>' + Ln) in CapLimit_data['Pk']:
            limit = CapLimit_data['Pk'][Pk + '>' + Ln][period]
        else:
            limit = 1
        return hrs <= model.periodLen[period] * limit
    model.Pk_LnCapacity = pe.Constraint(model.PkLn, model.period, rule=Pk_LnCapacity_Rule)


    def Pk_Capacity_Rule1(model, Pk, SKU, period):
        i = 0
        j = 0
        if Pk_Cm_Route == "Yes":
            i = sum(model.x_Pk_Cm[Pk, Cm, SKU, period] for Cm in model.SKUCm_arr[SKU])
        if SKU in model.PkSKU_SUB:
            j = sum(model.x_Pk_WIP[Pk, WIP, SKU, period] for WIP in model.WIP)
        return i + j + \
             sum(model.x_Pk_FG[Pk, FG, SKU, period] for FG in model.FG) <= \
             sum(model.x_PkLnQ[Pk, Ln, SKU, period] for Ln in model.PkLn_arr[Pk])
    model.Pk_Capacity1 = pe.Constraint(model.Pk, model.PkSKU_FG, model.period, rule=Pk_Capacity_Rule1)


    def Pk_Capacity_Rule2(model, Pk, SKU, period):
        return sum(model.x_Pk_WIP[Pk, WIP, SKU, period] for WIP in model.WIP) <= \
             sum(model.x_PkLnQ[Pk, Ln, SKU, period] for Ln in model.PkLn_arr[Pk])
    model.Pk_Capacity2 = pe.Constraint(model.Pk, model.PkSKU_SUB, model.period, rule=Pk_Capacity_Rule2)


    def Pk_InputConstraint_Rule(model, Pk, period):
        if 'Pk' in cpConstraints_data and Pk in cpConstraints_data['Pk']:
            val = cpConstraints_data['Pk'][Pk]['Value (per Period)']
            if cpConstraints_data['Pk'][Pk]['Operator'] == '<':
                return sum(model.x_PkLnQ[Pk, Ln, SKU, period] for Ln in model.PkLn_arr[Pk] for SKU in model.PkSKU) < val
            if cpConstraints_data['Pk'][Pk]['Operator'] == '<=':
                return sum(model.x_PkLnQ[Pk, Ln, SKU, period] for Ln in model.PkLn_arr[Pk] for SKU in model.PkSKU) <= val
            if cpConstraints_data['Pk'][Pk]['Operator'] == '>':
                return sum(model.x_PkLnQ[Pk, Ln, SKU, period] for Ln in model.PkLn_arr[Pk] for SKU in model.PkSKU) > val
            if cpConstraints_data['Pk'][Pk]['Operator'] == '>=':
                return sum(model.x_PkLnQ[Pk, Ln, SKU, period] for Ln in model.PkLn_arr[Pk] for SKU in model.PkSKU) >= val
            if cpConstraints_data['Pk'][Pk]['Operator'] == '==':
                return sum(model.x_PkLnQ[Pk, Ln, SKU, period] for Ln in model.PkLn_arr[Pk] for SKU in model.PkSKU) == val
        else:
            return sum(model.x_PkLnQ[Pk, Ln, SKU, period] for Ln in model.PkLn_arr[Pk] for SKU in model.PkSKU) >= 0
    model.Pk_InputConstraint = pe.Constraint(model.Pk, model.period, rule=Pk_InputConstraint_Rule)


    def Pk_Shortfall_Rule(model, Grp, period): #here AG - TOP
        return sum(model.x_PkLnQ[Pk, Ln, SKU, period] for Pk in PkGrpMap_data[Grp] for Ln in PkGrpMap_data[Grp][Pk] for SKU in model.PkSKU) >= \
               (model.PkGrpMin[Grp] * model.periodLen[period]) - model.x_PkGrpSFQ[Grp, period]
    model.Pk_Shortfall = pe.Constraint(model.PkGrp, model.period, rule=Pk_Shortfall_Rule)

    def Pk_TierShortfall_RuleA(model, Grp, period): #here AG - Tier
        #bigM = 10000000
        #'''
        bigM = 0
        for Pk in PkGrpMap_data[Grp]:
            for Ln in PkGrpMap_data[Grp][Pk]:
                maxDF = {k: v for k, v in PkCap_data[Pk][Ln].items() if pnd.Series(v).notna().all()}
                if 'Pk' in CapLimit_data and (Pk + '>' + Ln) in CapLimit_data['Pk']:
                    limit = CapLimit_data['Pk'][Pk + '>' + Ln][period]
                else:
                    limit = 1
                bigM += max(maxDF.values()) * Period_data[period]["Period_Length"] * PkCapEff_data[Pk][Ln]['OEE'] * PkCapEff_data[Pk][Ln]['Period_Availability'] * limit
        #'''
        return sum(model.x_PkLnQ[Pk, Ln, SKU, period] for Pk in PkGrpMap_data[Grp] for Ln in PkGrpMap_data[Grp][Pk] for SKU in model.PkSKU) >= \
               (model.PkGrpMin[Grp] * model.periodLen[period]) - (model.x_PkGrpTSFQ_Bin[Grp, period] * bigM)
    model.Pk_TierShortfallA = pe.Constraint(model.PkTierLst, model.period, rule=Pk_TierShortfall_RuleA)


    def Pk_TierShortfall_RuleB(model, Grp, period): #here AG - Tier
        #bigM = 10000000
        #'''
        bigM = 0
        for Pk in PkGrpMap_data[Grp]:
            for Ln in PkGrpMap_data[Grp][Pk]:
                maxDF = {k: v for k, v in PkCap_data[Pk][Ln].items() if pnd.Series(v).notna().all()}
                if 'Pk' in CapLimit_data and (Pk + '>' + Ln) in CapLimit_data['Pk']:
                    limit = CapLimit_data['Pk'][Pk + '>' + Ln][period]
                else:
                    limit = 1
                bigM += max(maxDF.values()) * Period_data[period]["Period_Length"] * PkCapEff_data[Pk][Ln]['OEE'] * PkCapEff_data[Pk][Ln]['Period_Availability'] * limit
        #'''
        return sum(model.x_PkLnQ[Pk, Ln, SKU, period] for Pk in PkGrpMap_data[Grp] for Ln in PkGrpMap_data[Grp][Pk] for SKU in model.PkSKU) <= \
               (model.PkGrpMin[Grp] * model.periodLen[period]) + ((1-model.x_PkGrpTSFQ_Bin[Grp, period]) * bigM)
    model.Pk_TierShortfallB = pe.Constraint(model.PkTierLst, model.period, rule=Pk_TierShortfall_RuleB)

    def Pk_Rebate_RuleA(model, Grp, period): #here AG - Rebate
        return sum(model.x_PkLnQ[Pk, Ln, SKU, period] for Pk in PkGrpMap_data[Grp] for Ln in PkGrpMap_data[Grp][Pk] for SKU in model.PkSKU) >= \
               (model.PkGrpRebateMin[Grp] * model.periodLen[period] * model.x_PkGrpRQ_Bin[Grp, period]) + model.x_PkGrpRQ[Grp, period]
    model.Pk_RebateA = pe.Constraint(model.PkRbLst, model.period, rule=Pk_Rebate_RuleA)

    def Pk_Rebate_RuleB(model, Grp, period): #here AG - Rebate
        #bigM = 10000000
        #'''
        bigM = 0
        for Pk in PkGrpMap_data[Grp]:
            for Ln in PkGrpMap_data[Grp][Pk]:
                maxDF = {k: v for k, v in PkCap_data[Pk][Ln].items() if pnd.Series(v).notna().all()}
                if 'Pk' in CapLimit_data and (Pk + '>' + Ln) in CapLimit_data['Pk']:
                    limit = CapLimit_data['Pk'][Pk + '>' + Ln][period]
                else:
                    limit = 1
                bigM += max(maxDF.values()) * Period_data[period]["Period_Length"] * PkCapEff_data[Pk][Ln]['OEE'] * PkCapEff_data[Pk][Ln]['Period_Availability'] * limit
        #'''
        return model.x_PkGrpRQ[Grp, period] <= (bigM * model.x_PkGrpRQ_Bin[Grp, period])
    model.Pk_RebateB = pe.Constraint(model.PkRbLst, model.period, rule=Pk_Rebate_RuleB)

    def Pk_Outbound_FG_Rule(model, Pk, FG, SKU, period):
        if 'All' in PkOB_data['OB_Sites(FG)'][Pk]:
            return model.x_Pk_FG[Pk, FG, SKU, period] >= 0
        elif FG in PkOB_data['OB_Sites(FG)'][Pk]:
            return model.x_Pk_FG[Pk, FG, SKU, period] >= 0
        else:
            return model.x_Pk_FG[Pk, FG, SKU, period] == 0
    model.Pk_Outbound_FG = pe.Constraint(model.Pk, model.FG, model.PkSKU_FG, model.period, rule=Pk_Outbound_FG_Rule)


    def Pk_Outbound_WIP_Rule(model, Pk, WIP, SKU, period):
        if 'All' in PkOB_data['OB_Sites(WIP)'][Pk]:
            return model.x_Pk_WIP[Pk, WIP, SKU, period] >= 0
        elif WIP in PkOB_data['OB_Sites(WIP)'][Pk]:
            return model.x_Pk_WIP[Pk, WIP, SKU, period] >= 0
        else:
            return model.x_Pk_WIP[Pk, WIP, SKU, period] == 0
    model.Pk_Outbound_WIP = pe.Constraint(model.Pk, model.WIP, model.PkSKU_SUB, model.period, rule=Pk_Outbound_WIP_Rule)


    def Pk_MinBatch_RuleA(model, Pk, Ln, SKU, period):

        if period in cpWarmUpLst:
            minBatchSize = Min_Batch_data[Pk][Ln][SKU] * warmUpPkMinFactor * PkBatchTempRedFactor
        else:
            minBatchSize = Min_Batch_data[Pk][Ln][SKU] * PkBatchTempRedFactor
        batchSize = minBatchSize * model.batchSizeMult[period]
        return model.x_PkLnQ[Pk, Ln, SKU, period] >= batchSize * model.x_PkBatchBinary[Pk, Ln, SKU, period]
    if enableMinBatchSize == "Yes":
        if warmStart_PkVar == 'Yes':
            model.Pk_MinBatchA = pe.Constraint(model.PkMinBatchPLst, rule=Pk_MinBatch_RuleA)
        else:
            model.Pk_MinBatchA = pe.Constraint(model.PkMinBatchLst, model.period, rule=Pk_MinBatch_RuleA)

    def Pk_MinBatch_RuleB(model, Pk, Ln, SKU, period):
        bigM = model.PkCap[Pk, Ln, SKU] * model.periodLen[period] + 1
        return model.x_PkLnQ[Pk, Ln, SKU, period] <= bigM * model.x_PkBatchBinary[Pk, Ln, SKU, period]
    if enableMinBatchSize == "Yes":
        if warmStart_PkVar == 'Yes':
            model.Pk_MinBatchB = pe.Constraint(model.PkMinBatchPLst, rule=Pk_MinBatch_RuleB)
        else:
            model.Pk_MinBatchB = pe.Constraint(model.PkMinBatchLst, model.period, rule=Pk_MinBatch_RuleB)


    def removePkLnVar_Rule(model, Pk, Ln, SKU, period):
        if [Pk, Ln, SKU, period] in WarmStartPk_tb:
            return model.x_PkLnQ[Pk, Ln, SKU, period] >= 0
        else:
            return model.x_PkLnQ[Pk, Ln, SKU, period] == 0
    if warmStart_PkVar == 'Yes':
        model.removePkLnVar = pe.Constraint(model.PkLn, model.PkSKU, model.period, rule=removePkLnVar_Rule)




    def rPk_RMFlow_Rule(model, rPk, rPkSSKU, period):
        return sum(model.rPkSSKU_rPkSKU[rPkSSKU, SKU] * model.x_rPk_FG[rPk, FG, SKU, period] for FG in model.FG for SKU in model.rPkSKU_FG) <= \
            sum(model.x_rPkS_rPk[rPkS, rPk, rPkSSKU, period] for rPkS in model.rPkS)
    model.rPk_RMFlow = pe.Constraint(model.rPk, model.rPkSSKU, model.period, rule=rPk_RMFlow_Rule)


    def rPk_Flow_Rule(model, rPk, SKU, period):
      return sum(model.PkSKU_rPkSKU[SKU, rPkSKU] * model.x_rPk_FG[rPk, FG, rPkSKU, period] for FG in model.FG for rPkSKU in model.rPkSKU_FG) <= \
             sum(model.x_WIP_rPk[WIP, rPk, SKU, period] for WIP in model.WIP)
    model.rPk_Flow = pe.Constraint(model.rPk, model.PkSKU_SUB, model.period, rule=rPk_Flow_Rule)


    def rPk_LnCapability_Rule(model, rPk, Ln, SKU, period):
        if model.rPkCap[rPk, Ln, SKU] == 0:
            return model.x_rPkLnQ[rPk, Ln, SKU, period] == 0
        else:
            return model.x_rPkLnQ[rPk, Ln, SKU, period] >= 0
    model.rPk_LnCapability = pe.Constraint(model.rPkLn, model.rPkSKU_FG, model.period, rule=rPk_LnCapability_Rule)


    def rPk_LnCapacity_Rule(model, rPk, Ln, period):
        hrs = 0
        for SKU in model.rPkSKU_FG:
            if model.rPkCap[rPk, Ln, SKU] == 0:
                hrs += 0
            else:
                hrs += (model.x_rPkLnQ[rPk, Ln, SKU, period] / model.rPkCap[rPk, Ln, SKU])

        if 'rPk' in CapLimit_data and (rPk + '>' + Ln) in CapLimit_data['rPk']:
            limit = CapLimit_data['rPk'][rPk + '>' + Ln][period]
        else:
            limit = 1
        return hrs <= model.periodLen[period] * limit
    model.rPk_LnCapacity = pe.Constraint(model.rPkLn, model.period, rule=rPk_LnCapacity_Rule)


    def rPk_Capacity_Rule(model, rPk, SKU, period):
      return sum(model.x_rPk_FG[rPk, FG, SKU, period] for FG in model.FG) <= \
             sum(model.x_rPkLnQ[rPk, Ln, SKU, period] for Ln in model.rPkLn_arr[rPk])
    model.rPk_Capacity = pe.Constraint(model.rPk, model.rPkSKU_FG, model.period, rule=rPk_Capacity_Rule)


    def rPk_InputConstraint_Rule(model, rPk, period):
        if 'rPk' in cpConstraints_data and rPk in cpConstraints_data['rPk']:
            val = cpConstraints_data['rPk'][rPk]['Value (per Period)']
            if cpConstraints_data['rPk'][rPk]['Operator'] == '<':
                return sum(model.x_rPkLnQ[rPk, Ln, SKU, period] for Ln in model.rPkLn_arr[rPk] for SKU in model.rPkSKU_FG) < val
            if cpConstraints_data['rPk'][rPk]['Operator'] == '<=':
                return sum(model.x_rPkLnQ[rPk, Ln, SKU, period] for Ln in model.rPkLn_arr[rPk] for SKU in model.rPkSKU_FG) <= val
            if cpConstraints_data['rPk'][rPk]['Operator'] == '>':
                return sum(model.x_rPkLnQ[rPk, Ln, SKU, period] for Ln in model.rPkLn_arr[rPk] for SKU in model.rPkSKU_FG) > val
            if cpConstraints_data['rPk'][rPk]['Operator'] == '>=':
                return sum(model.x_rPkLnQ[rPk, Ln, SKU, period] for Ln in model.rPkLn_arr[rPk] for SKU in model.rPkSKU_FG) >= val
            if cpConstraints_data['rPk'][rPk]['Operator'] == '==':
                return sum(model.x_rPkLnQ[rPk, Ln, SKU, period] for Ln in model.rPkLn_arr[rPk] for SKU in model.rPkSKU_FG) == val
        else:
            return sum(model.x_rPkLnQ[rPk, Ln, SKU, period] for Ln in model.rPkLn_arr[rPk] for SKU in model.rPkSKU_FG) >= 0
    model.rPk_InputConstraint = pe.Constraint(model.rPk, model.period, rule=rPk_InputConstraint_Rule)


    def rPk_Shortfall_Rule(model, Grp, period):
        return sum(model.x_rPkLnQ[rPk, Ln, SKU, period] for rPk in rPkGrpMap_data[Grp] for Ln in rPkGrpMap_data[Grp][rPk] for SKU in model.rPkSKU_FG) >= \
                (model.rPkGrpMin[Grp] * model.periodLen[period]) - model.x_rPkGrpSFQ[Grp, period]
    model.rPk_Shortfall = pe.Constraint(model.rPkGrp, model.period, rule=rPk_Shortfall_Rule)


    def rPk_Outbound_Rule(model, rPk, FG, SKU, period):
        if 'All' in rPkOB_data['OB_Sites'][rPk]:
            return model.x_rPk_FG[rPk, FG, SKU, period] >= 0
        elif FG in rPkOB_data['OB_Sites'][rPk]:
            return model.x_rPk_FG[rPk, FG, SKU, period] >= 0
        else:
            return model.x_rPk_FG[rPk, FG, SKU, period] == 0
    model.rPk_Outbound = pe.Constraint(model.rPk, model.FG, model.rPkSKU_FG, model.period, rule=rPk_Outbound_Rule)




    def FG_InventorySKU_Rule1(model, FG, SKU, period):
        periodCount = -1
        for item in period_lst:
            periodCount += 1
            if item == period:
                break

        if periodCount == 0:
            return sum(model.x_Pk_FG[Pk, FG, SKU, period] for Pk in model.Pk) - \
                   sum(model.x_FG_Cm[FG, Cm, SKU, period] for Cm in model.SKUCm_arr[SKU]) \
                   + model.x_FGSQ_init[FG, SKU] == \
                   model.x_FGSQ[FG, SKU, period]

        else:
            return sum(model.x_Pk_FG[Pk, FG, SKU, period] for Pk in model.Pk) - \
                   sum(model.x_FG_Cm[FG, Cm, SKU, period] for Cm in model.SKUCm_arr[SKU]) + \
                   model.x_FGSQ[FG, SKU, period_lst[periodCount - 1]] == \
                   model.x_FGSQ[FG, SKU, period]
    model.FG_InventorySKU1 = pe.Constraint(model.FG, model.PkSKU_FG, model.period, rule=FG_InventorySKU_Rule1)

    def FG_InventorySKU_Rule2(model, FG, SKU, period):
        periodCount = -1
        for item in period_lst:
            periodCount += 1
            if item == period:
                break

        if periodCount == 0:
            return sum(model.x_rPk_FG[rPk, FG, SKU, period] for rPk in model.rPk) - \
                   sum(model.x_FG_Cm[FG, Cm, SKU, period] for Cm in model.SKUCm_arr[SKU]) + \
                   model.x_FGSQ_init[FG, SKU] == \
                   model.x_FGSQ[FG, SKU, period]

        else:
            return sum(model.x_rPk_FG[rPk, FG, SKU, period] for rPk in model.rPk) - \
                   sum(model.x_FG_Cm[FG, Cm, SKU, period] for Cm in model.SKUCm_arr[SKU]) + \
                   model.x_FGSQ[FG, SKU, period_lst[periodCount - 1]] == \
                   model.x_FGSQ[FG, SKU, period]
    model.FG_InventorySKU2 = pe.Constraint(model.FG, model.rPkSKU_FG, model.period, rule=FG_InventorySKU_Rule2)


    def FG_InitCapacity_rule(model, FG):
      return sum(model.x_FGSQ_init[FG, SKU] for SKU in model.SKU) <= model.FGCap[FG]
    model.FG_InitCapacity = pe.Constraint(model.FG, rule=FG_InitCapacity_rule)


    def FG_Capacity_rule(model, FG, period):
      return sum(model.x_FGSQ[FG, SKU, period] for SKU in model.SKU) <= model.FGCap[FG]
    model.FG_Capacity = pe.Constraint(model.FG, model.period, rule=FG_Capacity_rule)


    def FG_InitStockCover_rule(model, FG, Ph, SKU):
        period = period_lst[0]
        if enableFGStockCover == 'Yes':
            if autofillInitialFGStorage == 'Yes':
                return model.x_FGSQ_init[FG, SKU] == \
                       sum(model.x_FG_Cm[FG, Cm, SKU, period] for Cm in model.SKUCm_arr[SKU]) / model.periodLen[period] * model.hoursCoverFG[FG, Ph, SKU]
            else:
                return model.x_FGSQ_init[FG, SKU] == model.initialFGQty[FG, SKU]
        else:
            if autofillInitialFGStorage == 'Yes':
                return model.x_FGSQ_init[FG, SKU] == 0
            else:
                return model.x_FGSQ_init[FG, SKU] == model.initialFGQty[FG, SKU]
    model.FG_InitStockCover = pe.Constraint(model.FGPh, model.SKU, rule=FG_InitStockCover_rule)



    def FG_StockCover_rule(model, FG, Ph, SKU, period):
        periodCount = -1
        for item in period_lst:
            periodCount += 1
            if item == period:
                break

        if periodCount == len(period_lst) - 1:
            return model.x_FGSQ[FG, SKU, period] >= model.x_FGSQ_init[FG, SKU]

        else:
            return model.x_FGSQ[FG, SKU, period] >= \
                   sum(model.x_FG_Cm[FG, Cm, SKU, period_lst[periodCount + 1]] for Cm in model.SKUCm_arr[SKU]) / model.periodLen[period_lst[periodCount + 1]] * model.hoursCoverFG[FG, Ph, SKU]
    if enableFGStockCover == 'Yes':
        model.FG_StockCover = pe.Constraint(model.FGPh, model.SKU, model.period, rule=FG_StockCover_rule)



    def FG_Flow_Rule1(model, FG, SKU, period):
        periodCount = -1
        for item in period_lst:
            periodCount += 1
            if item == period:
                break

        if periodCount == 0:
            return sum(model.x_FG_Cm[FG, Cm, SKU, period] for Cm in model.SKUCm_arr[SKU]) <= \
                   sum(model.x_Pk_FG[Pk, FG, SKU, period] for Pk in model.Pk) + \
                   model.x_FGSQ_init[FG, SKU]

        else:
            return sum(model.x_FG_Cm[FG, Cm, SKU, period] for Cm in model.SKUCm_arr[SKU]) <= \
                   sum(model.x_Pk_FG[Pk, FG, SKU, period] for Pk in model.Pk) + \
                   model.x_FGSQ[FG, SKU, period_lst[periodCount - 1]]
    model.FG_Flow1 = pe.Constraint(model.FG, model.PkSKU_FG, model.period, rule=FG_Flow_Rule1)



    def FG_Flow_Rule2(model, FG, SKU, period):
        periodCount = -1
        for item in period_lst:
            periodCount += 1
            if item == period:
                break

        if periodCount == 0:
            return sum(model.x_FG_Cm[FG, Cm, SKU, period] for Cm in model.SKUCm_arr[SKU]) <= \
                   sum(model.x_rPk_FG[rPk, FG, SKU, period] for rPk in model.rPk) + \
                   model.x_FGSQ_init[FG, SKU]

        else:
            return sum(model.x_FG_Cm[FG, Cm, SKU, period] for Cm in model.SKUCm_arr[SKU]) <= \
                   sum(model.x_rPk_FG[rPk, FG, SKU, period] for rPk in model.rPk) + \
                   model.x_FGSQ[FG, SKU, period_lst[periodCount - 1]]
    model.FG_Flow2 = pe.Constraint(model.FG, model.rPkSKU_FG, model.period, rule=FG_Flow_Rule2)



    def FG_Cm_Loads_Rule(model, FG, Cm, period):
        return sum(model.x_FG_Cm[FG, Cm, SKU, period] / model.LdQty[SKU] for SKU in model.CmSKUnest[Cm]) <= \
                       model.x_FG_Cm_Lds[FG, Cm, period]
    if enableLoadSF_Full == "Yes":
        model.FG_Cm_Loads = pe.Constraint(model.FG, model.Cm, model.period, rule=FG_Cm_Loads_Rule)


    def FG_Cm_LoadSF_Rule(model, FG, Cm, period):
        return model.x_FG_Cm_Lds[FG, Cm, period] - \
               sum(model.x_FG_Cm[FG, Cm, SKU, period] / model.LdQty[SKU] for SKU in model.CmSKUnest[Cm]) <= \
               model.x_FG_Cm_LSF[FG, Cm, period]
    if enableLoadSF_Full == "Yes":
        model.FG_Cm_LoadSF = pe.Constraint(model.FG, model.Cm, model.period, rule=FG_Cm_LoadSF_Rule)





    def FG_Cm_LoadMin_RuleA(model, FG, Cm, period):
        if enableLoadSF_MinFract == "Yes":
            return sum(model.x_FG_Cm[FG, Cm, SKU, period] / model.LdQty[SKU] for SKU in model.CmSKUnest[Cm]) <= \
                           model.x_FG_Cm_Lds[FG, Cm, period]
        if enableLoadSF_MinRnd == "Yes":
            return sum(model.x_FG_Cm[FG, Cm, SKU, period] / model.LdQty[SKU] for SKU in model.CmSKUnest[Cm]) == \
                           model.x_FG_Cm_Lds[FG, Cm, period]
    if enableLoadSF_MinFract == "Yes" or enableLoadSF_MinRnd == "Yes":
        if reportDistBinOnly == 'Yes' and warmStart_DistVar_FGCm == 'No' and warmStart_DistVar_FGCmSKU == 'No':
            model.FG_Cm_LoadMinA = pe.Constraint(model.FG, model.Cm, model.reportingPeriod, rule=FG_Cm_LoadMin_RuleA)
        elif reportDistBinOnly == 'No' and warmStart_DistVar_FGCm == 'No' and warmStart_DistVar_FGCmSKU == 'No':
            model.FG_Cm_LoadMinA = pe.Constraint(model.FG, model.Cm, model.period, rule=FG_Cm_LoadMin_RuleA)
        else:
            model.FG_Cm_LoadMinA = pe.Constraint(model.DistVar_FGCmP, rule=FG_Cm_LoadMin_RuleA)


    def FG_Cm_LoadMin_RuleB(model, FG, Cm, period):
        return model.x_FG_Cm_Lds[FG, Cm, period] >= minLoad * model.x_FG_Cm_LdBinary[FG, Cm, period]
    if enableLoadSF_MinFract == "Yes" or enableLoadSF_MinRnd == "Yes":
        if reportDistBinOnly == 'Yes' and warmStart_DistVar_FGCm == 'No' and warmStart_DistVar_FGCmSKU == 'No':
            model.FG_Cm_LoadMinB = pe.Constraint(model.FG, model.Cm, model.reportingPeriod, rule=FG_Cm_LoadMin_RuleB)
        elif reportDistBinOnly == 'No' and warmStart_DistVar_FGCm == 'No' and warmStart_DistVar_FGCmSKU == 'No':
            model.FG_Cm_LoadMinB = pe.Constraint(model.FG, model.Cm, model.period, rule=FG_Cm_LoadMin_RuleB)
        else:
            model.FG_Cm_LoadMinB = pe.Constraint(model.DistVar_FGCmP, rule=FG_Cm_LoadMin_RuleB)


    def FG_Cm_LoadMin_RuleC(model, FG, Cm, period):
        bigM = maxCmLd + 10
        return model.x_FG_Cm_Lds[FG, Cm, period] <= bigM * model.x_FG_Cm_LdBinary[FG, Cm, period]
    if enableLoadSF_MinFract == "Yes" or enableLoadSF_MinRnd == "Yes":
        if reportDistBinOnly == 'Yes' and warmStart_DistVar_FGCm == 'No' and warmStart_DistVar_FGCmSKU == 'No':
            model.FG_Cm_LoadMinC = pe.Constraint(model.FG, model.Cm, model.reportingPeriod, rule=FG_Cm_LoadMin_RuleC)
        elif reportDistBinOnly == 'No' and warmStart_DistVar_FGCm == 'No' and warmStart_DistVar_FGCmSKU == 'No':
            model.FG_Cm_LoadMinC = pe.Constraint(model.FG, model.Cm, model.period, rule=FG_Cm_LoadMin_RuleC)
        else:
            model.FG_Cm_LoadMinC = pe.Constraint(model.DistVar_FGCmP, rule=FG_Cm_LoadMin_RuleC)



    def FG_Cm_LoadSF_Rule(model, FG, Cm, period):
        return model.x_FG_Cm_Lds[FG, Cm, period] - \
               sum(model.x_FG_Cm[FG, Cm, SKU, period] / model.LdQty[SKU] for SKU in model.CmSKUnest[Cm]) <= \
               model.x_FG_Cm_LSF[FG, Cm, period]
    if enableLoadSF_MinFract == "Yes":
        if reportDistBinOnly == 'Yes' and warmStart_DistVar_FGCm == 'No' and warmStart_DistVar_FGCmSKU == 'No':
            model.FG_Cm_LoadSF = pe.Constraint(model.FG, model.Cm, model.reportingPeriod, rule=FG_Cm_LoadSF_Rule)
        elif reportDistBinOnly == 'No' and warmStart_DistVar_FGCm == 'No' and warmStart_DistVar_FGCmSKU == 'No':
            model.FG_Cm_LoadSF = pe.Constraint(model.FG, model.Cm, model.period, rule=FG_Cm_LoadSF_Rule)
        else:
            model.FG_Cm_LoadSF = pe.Constraint(model.DistVar_FGCmP, rule=FG_Cm_LoadSF_Rule)





    def WIP_InventorySKU_Rule(model, WIP, SKU, period):
        periodCount = -1
        for item in period_lst:
            periodCount += 1
            if item == period:
                break

        if periodCount == 0:
            return sum(model.x_Pk_WIP[Pk, WIP, SKU, period] for Pk in model.Pk) - \
                   sum(model.x_WIP_rPk[WIP, rPk, SKU, period] for rPk in model.rPk) + \
                   model.x_WIPSQ_init[WIP, SKU] == \
                   model.x_WIPSQ[WIP, SKU, period]

        else:
            return sum(model.x_Pk_WIP[Pk, WIP, SKU, period] for Pk in model.Pk) - \
                   sum(model.x_WIP_rPk[WIP, rPk, SKU, period] for rPk in model.rPk) + \
                   model.x_WIPSQ[WIP, SKU, period_lst[periodCount - 1]] == \
                   model.x_WIPSQ[WIP, SKU, period]
    model.WIP_InventorySKU = pe.Constraint(model.WIP, model.PkSKU_SUB, model.period, rule=WIP_InventorySKU_Rule)



    def WIP_InitCapacity_rule(model, WIP, Ph):
        sharedSite = WIPSiteType_data[WIP][Ph]['Shared_FG_Site']
        if pnd.isna(sharedSite) or sharedSite == 'None':
            return sum(model.x_WIPSQ_init[WIP, SKU] for SKU in model.PkSKU_SUB) <= model.WIPCap[WIP]
        else:
            return sum(model.x_WIPSQ_init[WIP, SKU] for SKU in model.PkSKU_SUB) + \
                   sum(model.x_FGSQ_init[sharedSite, SKU] for SKU in model.SKU) <= \
                       model.WIPCap[WIP]
    model.WIP_InitCapacity = pe.Constraint(model.WIPPh, rule=WIP_InitCapacity_rule)


    def WIP_Capacity_rule(model, WIP, Ph, period):
        sharedSite = WIPSiteType_data[WIP][Ph]['Shared_FG_Site']
        if pnd.isna(sharedSite) or sharedSite == 'None':
            return sum(model.x_WIPSQ[WIP, SKU, period] for SKU in model.PkSKU_SUB) <= model.WIPCap[WIP]
        else:
            return sum(model.x_WIPSQ[WIP, SKU, period] for SKU in model.PkSKU_SUB) + \
                   sum(model.x_FGSQ[sharedSite, SKU, period] for SKU in model.SKU) <= \
                   model.WIPCap[WIP]
    model.WIP_Capacity = pe.Constraint(model.WIPPh, model.period, rule=WIP_Capacity_rule)









    def WIP_InitStockCover_rule(model, WIP, Ph, SKU):
        period = period_lst[0]
        if enableWIPStockCover == 'Yes':
            if autofillInitialWIPStorage == 'Yes':
                return model.x_WIPSQ_init[WIP, SKU] == \
                       sum(model.x_WIP_rPk[WIP, rPk, SKU, period] for rPk in model.rPk) / model.periodLen[period] * model.hoursCoverWIP[WIP, Ph, SKU]
            else:
                return model.x_WIPSQ_init[WIP, SKU] == model.initialWIPQty[WIP, SKU]
        else:
            if autofillInitialWIPStorage == 'Yes':
                return model.x_WIPSQ_init[WIP, SKU] == 0
            else:
                return model.x_WIPSQ_init[WIP, SKU] == model.initialWIPQty[WIP, SKU]
    model.WIP_InitStockCover = pe.Constraint(model.WIPPh, model.PkSKU_SUB, rule=WIP_InitStockCover_rule)



    def WIP_StockCover_rule(model, WIP, Ph, SKU, period):
        periodCount = -1
        for item in period_lst:
            periodCount += 1
            if item == period:
                break

        if periodCount == len(period_lst) - 1:
            return model.x_WIPSQ[WIP, SKU, period] >= model.x_WIPSQ_init[WIP, SKU]

        else:
            return model.x_WIPSQ[WIP, SKU, period] >= \
                   sum(model.x_WIP_rPk[WIP, rPk, SKU, period_lst[periodCount + 1]] for rPk in model.rPk) / model.periodLen[period_lst[periodCount + 1]] * model.hoursCoverWIP[WIP, Ph, SKU]
    if enableWIPStockCover == 'Yes':
        model.WIP_StockCover = pe.Constraint(model.WIPPh, model.PkSKU_SUB, model.period, rule=WIP_StockCover_rule)



    def WIP_InitStockPenalty_rule(model, WIP, Ph, SKU):
        return model.x_WIPSQ_initPenalty[WIP, SKU] == model.x_WIPSQ_init[WIP, SKU]
    if enableWIPInitialStockPenalty == 'Yes':
        model.WIP_InitStockPenalty = pe.Constraint(model.WIPPh, model.PkSKU_SUB, rule=WIP_InitStockPenalty_rule)


    def WIP_ExtBalance_Rule(model, WIP, SKU, period):
        periodCount = -1
        for item in period_lst:
            periodCount += 1
            if item == period:
                break

        if periodCount == 0:
            return sum(model.x_WIP_rPk[WIP, rPk, SKU, period] for rPk in model.rPk) <= \
                   sum(model.x_Pk_WIP[Pk, WIP, SKU, period] for Pk in model.Pk) + \
                   model.x_WIPSQ_init[WIP, SKU]

        else:
            return sum(model.x_WIP_rPk[WIP, rPk, SKU, period] for rPk in model.rPk) <= \
                   sum(model.x_Pk_WIP[Pk, WIP, SKU, period] for Pk in model.Pk) + \
                   model.x_WIPSQ[WIP, SKU, period_lst[periodCount - 1]]
    model.WIP_ExtBalance = pe.Constraint(model.WIP, model.PkSKU_SUB, model.period, rule=WIP_ExtBalance_Rule)


    def WIP_Outbound_Rule(model, WIP, rPk, SKU, period):
        if 'All' in WIPOB_data['OB_Sites'][WIP]:
            return model.x_WIP_rPk[WIP, rPk, SKU, period] >= 0
        elif rPk in WIPOB_data['OB_Sites'][WIP]:
            return model.x_WIP_rPk[WIP, rPk, SKU, period] >= 0
        else:
            return model.x_WIP_rPk[WIP, rPk, SKU, period] == 0
    model.WIP_Outbound = pe.Constraint(model.WIP, model.rPk, model.PkSKU_SUB, model.period, rule=WIP_Outbound_Rule)





    def Cm_Demand_rule1(model, Cm, SKU, period):
        i = 0
        if Pk_Cm_Route == "Yes":
            i = sum(model.x_Pk_Cm[Pk, Cm, SKU, period] for Pk in model.Pk)
        return i + \
               sum(model.x_FG_Cm[FG, Cm, SKU, period] for FG in model.FG) >= \
               model.CmDem[Cm, SKU, period]
    model.Cm_Demand1 = pe.Constraint(model.CmSKU_Pk, model.period, rule=Cm_Demand_rule1)


    def Cm_Demand_rule2(model, Cm, SKU, period):
      return sum(model.x_FG_Cm[FG, Cm, SKU, period] for FG in model.FG) >= \
             model.CmDem[Cm, SKU, period]
    model.Cm_Demand2 = pe.Constraint(model.CmSKU_rPk, model.period, rule=Cm_Demand_rule2)



    def removeCmRoutes_rule(model, FG, Cm, SKU, period):


        if warmStart_DistVar_FGCm == 'Yes':
            if FG in WarmStartDist_tbD and Cm in WarmStartDist_tbD[FG]:
                return model.x_FG_Cm[FG, Cm, SKU, period] >= 0
            else:
                return model.x_FG_Cm[FG, Cm, SKU, period] == 0

        else:
            if FG in WarmStartDist_tbD and Cm in WarmStartDist_tbD[FG] and SKU in WarmStartDist_tbD[FG][Cm]:
                return model.x_FG_Cm[FG, Cm, SKU, period] >= 0
            else:
                return model.x_FG_Cm[FG, Cm, SKU, period] == 0
    if warmStart_DistVar_FGCm == 'Yes' or warmStart_DistVar_FGCmSKU == 'Yes':
        model.removeCmRoutes = pe.Constraint(model.FG, model.CmSKU, model.period, rule=removeCmRoutes_rule)




    if warmStart_PkBin == 'Yes':
        for site, line, sku, period in WarmStartPkBin_tb:
            model.x_PkBatchBinary[site, line, sku, period] = 1
    if warmStart_DistBin == 'Yes':
        for FG, Cm, sku, period in WarmStartDist_tb:
            if reportDistBinOnly == 'Yes':
                if period in reportPeriod:
                    model.x_FG_Cm_LdBinary[FG, Cm, period] = 1
            else:
                model.x_FG_Cm_LdBinary[FG, Cm, period] = 1





    def objective_rule(model):

        x1 = 0
        if Pk_Cm_Route == "Yes":
            x1 = sum((model.Pk_Cm[Pk, Cm] / model.LdQty[SKU]) * model.x_Pk_Cm[Pk, Cm, SKU, period]
                      for Pk in model.Pk for Cm,SKU in model.CmSKU_Pk for period in model.period)

        x2 = 0
        if enableLoadSF_Full == "Yes" or enableLoadSF_MinFract == 'Yes':
            x2 = sum(model.FG_Cm[FG, Cm] * model.x_FG_Cm_LSF[FG, Cm, period]
                    for FG in model.FG for Cm in model.Cm for period in model.period)
        x4 = 0
        if enableWIPInitialStockPenalty == 'Yes':
            x4 = sum((model.WIPCstS[WIP] * model.periodLen[period_lst[0]] + 20) * model.x_WIPSQ_initPenalty[WIP, SKU]
                     for WIP in model.WIP for SKU in model.PkSKU_SUB)

        return sum((model.Pd_Pk[Pd, Pk] / model.LdQty[SKU]) * model.x_Pd_Pk[Pd, Pk, SKU, period]
                   for Pd in model.Pd for Pk in model.Pk for SKU in model.PdSKU for period in model.period) + \
            x1 + \
            sum(((model.Pk_FG[Pk, FG] / model.LdQty[SKU]) + model.FGCstH[FG]) * model.x_Pk_FG[Pk, FG, SKU, period]
                for Pk in model.Pk for FG in model.FG for SKU in model.PkSKU_FG for period in model.period) + \
            sum(((model.Pk_WIP[Pk, WIP] / model.LdQty[SKU]) + model.WIPCstH[WIP]) * model.x_Pk_WIP[Pk, WIP, SKU, period]
                for Pk in model.Pk for WIP in model.WIP for SKU in model.PkSKU_SUB for period in model.period) + \
            sum((model.WIP_rPk[WIP, rPk] / model.LdQty[SKU]) * model.x_WIP_rPk[WIP, rPk, SKU, period]
                for WIP in model.WIP for rPk in model.rPk for SKU in model.PkSKU_SUB for period in model.period) + \
            sum(((model.rPk_FG[rPk, FG] / model.LdQty[SKU]) + model.FGCstH[FG]) * model.x_rPk_FG[rPk, FG, SKU, period]
                for rPk in model.rPk for FG in model.FG for SKU in model.rPkSKU_FG for period in model.period) + \
            sum((model.FG_Cm[FG, Cm] / model.LdQty[SKU]) * model.x_FG_Cm[FG, Cm, SKU, period]
                for FG in model.FG for Cm, SKU in model.CmSKU for period in model.period) + \
            sum(model.PdCst[Pd, St, SKU] * model.x_PdStQ[Pd, St, SKU, period]
                for Pd, St in model.PdSt for SKU in model.PdSKU for period in model.period) + \
            sum(model.PdSFCst[Grp] * model.x_PdGrpSFQ[Grp, period]
                for Grp in model.PdGrp for period in model.period) + \
            sum(model.PkCst[Pk, Ln, SKU] * model.x_PkLnQ[Pk, Ln, SKU, period]
                for Pk, Ln in model.PkLn for SKU in model.PkSKU for period in model.period) + \
            sum(model.PkSFCst[Grp] * model.x_PkGrpSFQ[Grp, period] #here AG - TOP
                for Grp in model.PkGrp for period in model.period) + \
            sum(model.PkSFTCst[Grp] * model.x_PkLnQ[Pk, Ln, SKU, period] * model.x_PkGrpTSFQ_Bin[Grp, period] #here AG - Tier
                for Grp in model.PkTierLst for period in model.period for Pk, Ln in model.PkLn for SKU in model.PkSKU) - \
            sum(model.PkRCst[Grp] * model.x_PkGrpRQ[Grp, period] #here AG - Rebate
                for Grp in model.PkRbLst for period in model.period) + \
            sum(model.rPkCst[rPk, Ln, SKU] * model.x_rPkLnQ[rPk, Ln, SKU, period]
                for rPk, Ln in model.rPkLn for SKU in model.rPkSKU_FG for period in model.period) + \
            sum(model.rPkSFCst[Grp] * model.x_rPkGrpSFQ[Grp, period]
                for Grp in model.rPkGrp for period in model.period) + \
            sum(model.FGCstS[FG] * model.periodLen[period] * model.x_FGSQ[FG, SKU, period]
                for FG in model.FG for SKU in model.SKU for period in model.period) + \
            sum(model.WIPCstS[WIP] * model.periodLen[period] * model.x_WIPSQ[WIP, SKU, period]
                for WIP in model.WIP for SKU in model.PkSKU_SUB for period in model.period) + \
            sum(model.PdSCst[PdS, PdSSKU] * model.x_PdS_Pd[PdS, Pd, PdSSKU, period]
                for PdS in model.PdS for Pd in model.Pd for PdSSKU in model.PdSSKU for period in model.period) + \
            sum(model.PkSCst[PkS, PkSSKU] * model.x_PkS_Pk[PkS, Pk, PkSSKU, period]
                for PkS in model.PkS for Pk in model.Pk for PkSSKU in model.PkSSKU for period in model.period) + \
            sum(model.rPkSCst[rPkS, rPkSSKU] * model.x_rPkS_rPk[rPkS, rPk, rPkSSKU, period]
                for rPkS in model.rPkS for rPk in model.rPk for rPkSSKU in model.rPkSSKU for period in model.period) + \
            x2 + \
            x4



    model.objective = pe.Objective(rule=objective_rule, sense=pe.minimize)




    def pyomo_postprocess(options=None, instance=None, results=None):
        model.x_PdS_Pd.display()
        model.x_PkS_Pk.display()
        model.x_rPkS_rPk.display()
        model.x_Pd_Pk.display()
        if Pk_Cm_Route == "Yes":
            model.x_Pk_Cm.display()
        model.x_Pk_FG.display()
        model.x_Pk_WIP.display()
        model.x_WIP_rPk.display()
        model.x_rPk_FG.display()
        model.x_FG_Cm.display()

        model.x_PdStQ.display()
        model.x_PkLnQ.display()
        model.x_rPkLnQ.display()

        model.x_FGSQ.display()
        model.x_WIPSQ.display()


    #Tag Reno: Main Code
    if __name__ == '__main__':

        from pyomo.opt import SolverFactory, SolverStatus, TerminationCondition
        import pyomo.environ
        #opt = SolverFactory('ipopt') #opt = SolverFactory('cbc')
        #opt.options['max_iter'] = 250
        #'''
        opt = SolverFactory("gurobi", solver_io="python") #here - online Run
        opt.options['TimeLimit'] = (cutoffTime * 60)
        opt.options['MIPFocus'] = MIPFocus
        opt.options['MIPGap'] = MIPGap
        opt.options['NoRelHeurTime'] = NoRelHeurTime
        opt.options['Presolve'] = Presolve #'''

        # Tag Reno - Leave print below
        print('*** Setup complete. Beginning RIC BGO Solver ***')
        # Tag Reno - Changing scenario status
        db_cur.execute('UPDATE public."Scenarios" SET scenario_status = %s WHERE id = %s', (2, scenarioId))
        db_conn.commit()

        if warmStart_PkBin == 'Yes' or warmStart_DistBin == 'Yes':
            results = opt.solve(model, tee=True, keepfiles=True, warmstart=True)
        else:
            results = opt.solve(model, tee=True, keepfiles=True)



        if (results.solver.status == SolverStatus.ok) and (results.solver.termination_condition == TerminationCondition.optimal):
            print("Solution is feasible")
            print("Objective function value = ", model.objective())
        elif results.solver.termination_condition == TerminationCondition.infeasible:
            print("*** Failed to find solution - infeasible. ***")
            # Tag Reno - If solution is not feasible then the script must exit
            db_cur.execute('UPDATE public."Scenarios" SET scenario_status = %s, error_message = %s WHERE id = %s', (5, "Solution infeasible", scenarioId))
            db_conn.commit()
            time.sleep(10)
            exit()
        else:

            print(str(results.solver))


        results.write()




        print("\nDisplaying Solution\n" + '-' * 60)






        if runType == 'Iterative':
            if itn <= 1:
                WarmStartPk_tb = []
                for S1, Ln, sku, period in model.x_PkLnQ:
                    val = model.x_PkLnQ[S1, Ln, sku, period].value * scalingVolume
                    if val > 0:
                        WarmStartPk_tb.append([S1, Ln, sku, period])


            if itn == 2:
                WarmStartPkBin_tb = []
                for S1, Ln, sku, period in model.x_PkLnQ:
                    val = model.x_PkLnQ[S1, Ln, sku, period].value * scalingVolume
                    if val > 0:
                        WarmStartPkBin_tb.append([S1, Ln, sku, period])
                WarmStartDist_tb = []
                for S1, Cm, sku, period in model.x_FG_Cm:
                    val = model.x_FG_Cm[S1, Cm, sku, period].value * scalingVolume
                    if val > 0:
                        WarmStartDist_tb.append([S1, Cm, sku, period])

            if itn != 3:
                model.clear()
                print('Model Reset')



if __name__ == '__main__':


    PdS_Pd_soln = pnd.DataFrame(model.x_PdS_Pd.get_values().items()).set_index(0)
    PdS_Pd_soln.index = pnd.MultiIndex.from_tuples(PdS_Pd_soln.index, names=['Prod Supplier', 'Prod Site', 'Raw Material', 'Period'])
    PdS_Pd_soln.rename(columns={PdS_Pd_soln.columns[0]: "Quantity"}, inplace=True)
    if enableScaling == 'Yes':
        PdS_Pd_soln['Quantity'] = PdS_Pd_soln['Quantity'] * scalingVolume

    PkS_Pk_soln = pnd.DataFrame(model.x_PkS_Pk.get_values().items()).set_index(0)
    PkS_Pk_soln.index = pnd.MultiIndex.from_tuples(PkS_Pk_soln.index, names=['Pack Supplier', 'Pack Site', 'Raw Material', 'Period'])
    PkS_Pk_soln.rename(columns={PkS_Pk_soln.columns[0]: "Quantity"}, inplace=True)
    if enableScaling == 'Yes':
        PkS_Pk_soln['Quantity'] = PkS_Pk_soln['Quantity'] * scalingVolume

    rPkS_rPk_soln = pnd.DataFrame(model.x_rPkS_rPk.get_values().items()).set_index(0)
    rPkS_rPk_soln.index = pnd.MultiIndex.from_tuples(rPkS_rPk_soln.index, names=['RePack Supplier', 'RePack Site', 'Raw Material', 'Period'])
    rPkS_rPk_soln.rename(columns={rPkS_rPk_soln.columns[0]: "Quantity"}, inplace=True)
    if enableScaling == 'Yes':
        rPkS_rPk_soln['Quantity'] = rPkS_rPk_soln['Quantity'] * scalingVolume


    dictPd = {}
    dictPdSt = {}
    dictPdParent = {}
    dictParentPdSt = {}
    dictPk = {}
    dictPkLn = {}
    dictPkParent = {}
    dictParentPkLn = {}
    dictrPk = {}
    dictrPkLn = {}
    dictrPkParent = {}
    dictParentrPkLn = {}
    dictPdGrp = {}
    dictPkGrp = {}
    dictrPkGrp = {}
    dictWIP = {}
    dictFG = {}
    dictPd_Pk = {}
    dictPk_Cm = {}
    dictPk_FG = {}
    dictPk_WIP = {}
    dictWIP_rPk = {}
    dictrPk_FG = {}
    dictFG_Cm = {}
    dictVolDelivered = {}
    dictVolRequired = {}
    dictKPILoads = {}
    dictLoadsSF = {}



    period_lstExt = copy.deepcopy(period_lst)
    period_lstExt.insert(0, 'Report Total')
    period_lstExt.insert(0, 'Total')

    for period in period_lstExt:

        for s1 in PdCap_data:
            dictPd['Pd', s1, 'Total', period] = 0
            lstParent = []
            for st in PdCap_data[s1]:
                dictPdSt['Pd', s1, st, period] = 0

                for sku in PdCap_data[s1][st]:
                    if PdCap_data[s1][st][sku] > 0:
                        parentGrp = Unit_data['Parent_Group'][sku]
                        if pnd.notna(parentGrp):
                            if parentGrp not in lstParent:
                                lstParent.append(parentGrp)

                            if (parentGrp, s1) not in dictParentPdSt:
                                dictParentPdSt[(parentGrp, s1)] = [st]
                            elif st not in dictParentPdSt[(parentGrp, s1)]:
                                dictParentPdSt[(parentGrp, s1)].append(st)

            for parent in lstParent:
                dictPdParent['Pd', s1, parent, period] = 0

        for s1 in PkCap_data:
            dictPk['Pk', s1, 'Total', period] = 0
            lstParent = []
            for ln in PkCap_data[s1]:
                dictPkLn['Pk', s1, ln, period] = 0

                for sku in PkCap_data[s1][ln]:
                    if PkCap_data[s1][ln][sku] > 0:
                        parentGrp = Unit_data['Parent_Group'][sku]
                        if pnd.notna(parentGrp):
                            if parentGrp not in lstParent:
                                lstParent.append(parentGrp)

                            if (parentGrp, s1) not in dictParentPkLn:
                                dictParentPkLn[(parentGrp, s1)] = [ln]
                            elif ln not in dictParentPkLn[(parentGrp, s1)]:
                                dictParentPkLn[(parentGrp, s1)].append(ln)

            for parent in lstParent:
                dictPkParent['Pk', s1, parent, period] = 0

        for s1 in rPkCap_data:
            dictrPk['rPk', s1, 'Total', period] = 0
            lstParent = []
            for ln in rPkCap_data[s1]:
                dictrPkLn['rPk', s1, ln, period] = 0

                for sku in rPkCap_data[s1][ln]:
                    if rPkCap_data[s1][ln][sku] > 0:
                        parentGrp = Unit_data['Parent_Group'][sku]
                        if pnd.notna(parentGrp):
                            if parentGrp not in lstParent:
                                lstParent.append(parentGrp)

                            if (parentGrp, s1) not in dictParentrPkLn:
                                dictParentrPkLn[(parentGrp, s1)] = [ln]
                            elif ln not in dictParentrPkLn[(parentGrp, s1)]:
                                dictParentrPkLn[(parentGrp, s1)].append(ln)

            for parent in lstParent:
                dictrPkParent['rPk', s1, parent, period] = 0

        for g1 in PdGrpMap_data:
            dictPdGrp['Pd', g1, 'Total', period] = 0
        for g1 in PkGrpMap_data:
            dictPkGrp['Pk', g1, 'Total', period] = 0
        for g1 in rPkGrpMap_data:
            dictrPkGrp['rPk', g1, 'Total', period] = 0
        for s1 in WIPCap_data:
            dictWIP['WIP', s1, 'Total', period] = 0
        for s1 in FGCap_data:
            dictFG['FG', s1, 'Total', period] = 0

        for s1 in PdCap_data:
            for s2 in PkCap_data:
                dictPd_Pk['Pd_Pk', s1, s2, period] = 0
        for s1 in PkCap_data:
            for s2 in CmDem_data:
                dictPk_Cm['Pk_Cm', s1, s2, period] = 0
        for s1 in PkCap_data:
            for s2 in FGCap_data:
                dictPk_FG['Pk_FG', s1, s2, period] = 0
        for s1 in PkCap_data:
            for s2 in WIPCap_data:
                dictPk_WIP['Pk_WIP', s1, s2, period] = 0
        for s1 in WIPCap_data:
            for s2 in rPkCap_data:
                dictWIP_rPk['WIP_rPk', s1, s2, period] = 0
        for s1 in rPkCap_data:
            for s2 in FGCap_data:
                dictrPk_FG['rPk_FG', s1, s2, period] = 0
        for s1 in FGCap_data:
            for s2 in CmDem_data:
                dictFG_Cm['FG_Cm', s1, s2, period] = 0


        dictVolDelivered['Delivered', 'Total', 'Total', period] = 0



        qty = 0
        if period != 'Total' and period != 'Report Total':
            for cm in CmDem_data:
                for sku in CmDem_data[cm]:
                    qty += CmDem_data[cm][sku][period]
        dictVolRequired['Required', 'Total', 'Total', period] = qty * scalingVolume


        dictKPILoads['FG_Cm', 'Total Miles', period] = 0
        dictKPILoads['FG_Cm', 'Total Loads', period] = 0





    dictCstMan = {}
    dictCstMan.update(dictPdSt)
    dictCstMan.update(dictPkLn)
    dictCstMan.update(dictrPkLn)

    dictCstFreight = {}
    dictCstFreight.update(dictPd_Pk)
    dictCstFreight.update(dictPk_Cm)
    dictCstFreight.update(dictPk_FG)
    dictCstFreight.update(dictPk_WIP)
    dictCstFreight.update(dictWIP_rPk)
    dictCstFreight.update(dictrPk_FG)
    dictCstFreight.update(dictFG_Cm)

    dictCstDunnage = copy.deepcopy(dictCstFreight)

    dictCstPenalty = {} #here AG - Tier
    dictCstPenalty.update(dictPdGrp)
    dictCstPenalty.update(dictPkGrp)
    dictCstPenalty.update(dictrPkGrp)
    dictTierCstPenalty = {}  #here AG - Tier
    dictTierCstPenalty.update(dictPkGrp)
    dictRBCstPenalty = {}  #here AG - Rebate
    dictRBCstPenalty.update(dictPkGrp)

    dictCstHandling = {}
    dictCstHandling.update(dictWIP)
    dictCstHandling.update(dictFG)

    dictCstStorage = copy.deepcopy(dictCstHandling)
    dictCstStorageInit = {}
    dictCstStorageInit.update(dictWIP)

    dictCstFreightSF = {}
    dictCstFreightSF.update(dictFG_Cm)

    dictVolQty = copy.deepcopy(dictCstMan)
    dictVolQty.update(dictWIP)
    dictVolQty.update(dictFG)
    dictVolSF = copy.deepcopy(dictCstPenalty) #here AG - TOP
    dictVolTier = copy.deepcopy(dictTierCstPenalty)  #here AG - Tier
    dictVolRB = copy.deepcopy(dictRBCstPenalty)  #here AG - Rebate
    dictHrsUsage = copy.deepcopy(dictCstMan)
    dictHrsAvail = copy.deepcopy(dictCstMan)

    dictHrsUtil = {}
    dictHrsUtil.update(dictPd)
    dictHrsUtil.update(dictPk)
    dictHrsUtil.update(dictrPk)
    dictVolUtil = {}
    dictVolUtil.update(dictWIP)
    dictVolUtil.update(dictFG)

    dictHrsUsageParent = {}
    dictHrsUsageParent.update(dictPdParent)
    dictHrsUsageParent.update(dictPkParent)
    dictHrsUsageParent.update(dictrPkParent)

    dictHrsAvailParent = copy.deepcopy(dictHrsUsageParent)
    dictHrsUtilParent = copy.deepcopy(dictHrsUsageParent)

    dictLoadsQty = copy.deepcopy(dictCstFreight)
    dictLoadsSF = copy.deepcopy(dictCstFreight)

    sc = cpScenario_data



    def distnOutputDF(x_S1_S2, handlingCost, route, storeType, dunnage, outputDict, handlingDict):
        for s1, s2, sku, period in x_S1_S2:
            val = x_S1_S2[s1, s2, sku, period].value * scalingVolume

            if val > 0:
                if route == 'Pd_Pk':
                    cst = D_Lanes_data[s1][s2]['Cost'] * scalingCost * (1 + cpValSettings_data['VALUE']['Tankering Modification']) / (Unit_data['Qty_Per_Load'][sku] * scalingVolume)
                else:
                    cst = D_Lanes_data[s1][s2]['Cost'] * scalingCost / (Unit_data['Qty_Per_Load'][sku] * scalingVolume)

                mi = D_Lanes_data[s1][s2]['Mileage']
                lds = val / (Unit_data['Qty_Per_Load'][sku] * scalingVolume)
                dictCstFreight[route, s1, s2, period] += cst * val
                dictLoadsQty[route, s1, s2, period] += lds

                if handlingCost == 'Y':
                    cst2 = handlingDict["Handling_Cost"][s2] / scalingVolume * scalingCost
                    dictCstHandling[storeType, s2, 'Total', period] += cst2 * val
                if dunnage == 'Y' and mi > dunnage_dist:
                    dictCstDunnage[route, s1, s2, period] += dunnage_cost * scalingCost * val / (Unit_data['Qty_Per_Load'][sku] * scalingVolume)
                if route == 'FG_Cm':
                    dictVolDelivered['Delivered', 'Total', 'Total', period] += val
                skuGrp = sku if modelGrpLevel == 'Yes' else Unit_data['SKU_Group'][sku]
                parent = Unit_data['Parent_Group'][sku]
                child = Unit_data['Child_Group'][sku]
                if handlingCost == 'Y':
                    outputDict[sc, s1, s2, sku, period] = (val, cst, cst2, mi, lds, skuGrp, parent, child)
                else:
                    outputDict[sc, s1, s2, sku, period] = (val, cst, mi, lds, skuGrp, parent, child)



    Pd_Pk_dict = {}

    distnOutputDF(model.x_Pd_Pk, handlingCost='N', route='Pd_Pk', storeType='none', dunnage='N', outputDict=Pd_Pk_dict, handlingDict='none')
    Pd_Pk_soln = pnd.DataFrame(Pd_Pk_dict.items()).set_index(0)
    Pd_Pk_soln.index = pnd.MultiIndex.from_tuples(Pd_Pk_soln.index, names=['Code', 'Prod Site', 'Pack Site', 'SKU', 'Period'])
    Pd_Pk_soln[['Liters', 'Dist. Cost ($/Cs)', 'Route Miles', 'Truck Loads', 'SKU Group', 'Parent SKU Grp', 'Child SKU Grp']] = Pd_Pk_soln[1].tolist()
    del Pd_Pk_soln[1]


    if Pk_Cm_Route == "Yes":
        Pk_Cm_dict = {}

        distnOutputDF(model.x_Pk_Cm, handlingCost='N', route='Pk_Cm', storeType='none', dunnage='Y', outputDict=Pk_Cm_dict, handlingDict='none')
        Pk_Cm_soln = pnd.DataFrame(Pk_Cm_dict.items()).set_index(0)
        Pk_Cm_soln.index = pnd.MultiIndex.from_tuples(Pk_Cm_soln.index, names=['Code', 'Pack Site', 'Distributor', 'SKU', 'Period'])
        Pk_Cm_soln[['Cases', 'Dist. Cost ($/Cs)', 'Route Miles', 'Truck Loads', 'SKU Group', 'Parent SKU Grp', 'Child SKU Grp']] = Pk_Cm_soln[1].tolist()
        del Pk_Cm_soln[1]


    Pk_FG_dict = {}

    distnOutputDF(model.x_Pk_FG, handlingCost='Y', route='Pk_FG', storeType='FG', dunnage='Y', outputDict=Pk_FG_dict, handlingDict=FGCst_data)
    Pk_FG_soln = pnd.DataFrame(Pk_FG_dict.items()).set_index(0)
    Pk_FG_soln.index = pnd.MultiIndex.from_tuples(Pk_FG_soln.index, names=['Code', 'Pack Site', 'FG Warehouse', 'SKU', 'Period'])
    Pk_FG_soln[['Cases', 'Dist. Cost ($/Cs)', 'WH Handling ($/Cs)', 'Route Miles', 'Truck Loads', 'SKU Group', 'Parent SKU Grp', 'Child SKU Grp']] = Pk_FG_soln[1].tolist()
    del Pk_FG_soln[1]

    Pk_WIP_dict = {}

    distnOutputDF(model.x_Pk_WIP, handlingCost='Y', route='Pk_WIP', storeType='WIP', dunnage='Y', outputDict=Pk_WIP_dict, handlingDict=WIPCst_data)
    Pk_WIP_soln = pnd.DataFrame(Pk_WIP_dict.items()).set_index(0)
    Pk_WIP_soln.index = pnd.MultiIndex.from_tuples(Pk_WIP_soln.index, names=['Code', 'Pack Site', 'WIP Warehouse', 'SKU', 'Period'])
    Pk_WIP_soln[['Cases', 'Dist. Cost ($/Cs)', 'WH Handling ($/Cs)', 'Route Miles', 'Truck Loads', 'SKU Group', 'Parent SKU Grp', 'Child SKU Grp']] = Pk_WIP_soln[1].tolist()
    del Pk_WIP_soln[1]

    WIP_rPk_dict = {}

    distnOutputDF(model.x_WIP_rPk, handlingCost='N', route='WIP_rPk', storeType='none', dunnage='Y', outputDict=WIP_rPk_dict, handlingDict='none')
    WIP_rPk_soln = pnd.DataFrame(WIP_rPk_dict.items()).set_index(0)
    WIP_rPk_soln.index = pnd.MultiIndex.from_tuples(WIP_rPk_soln.index, names=['Code', 'WIP Warehouse', 'RePack Site', 'SKU', 'Period'])
    WIP_rPk_soln[['Cases', 'Dist. Cost ($/Cs)', 'Route Miles', 'Truck Loads', 'SKU Group', 'Parent SKU Grp', 'Child SKU Grp']] = WIP_rPk_soln[1].tolist()
    del WIP_rPk_soln[1]

    rPk_FG_dict = {}

    distnOutputDF(model.x_rPk_FG, handlingCost='Y', route='rPk_FG', storeType='FG', dunnage='Y', outputDict=rPk_FG_dict, handlingDict=FGCst_data)
    rPk_FG_soln = pnd.DataFrame(rPk_FG_dict.items()).set_index(0)
    rPk_FG_soln.index = pnd.MultiIndex.from_tuples(rPk_FG_soln.index, names=['Code', 'RePack Site', 'FG Warehouse', 'SKU', 'Period'])
    rPk_FG_soln[['Cases', 'Dist. Cost ($/Cs)', 'WH Handling ($/Cs)', 'Route Miles', 'Truck Loads', 'SKU Group', 'Parent SKU Grp', 'Child SKU Grp']] = rPk_FG_soln[1].tolist()
    del rPk_FG_soln[1]

    FG_Cm_dict = {}

    distnOutputDF(model.x_FG_Cm, handlingCost='N', route='FG_Cm', storeType='none', dunnage='Y', outputDict=FG_Cm_dict, handlingDict='none')
    FG_Cm_soln = pnd.DataFrame(FG_Cm_dict.items()).set_index(0)
    FG_Cm_soln.index = pnd.MultiIndex.from_tuples(FG_Cm_soln.index, names=['Code', 'FG Warehouse', 'Distributor', 'SKU', 'Period'])
    FG_Cm_soln[['Cases', 'Dist. Cost ($/Cs)', 'Route Miles', 'Truck Loads', 'SKU Group', 'Parent SKU Grp', 'Child SKU Grp']] = FG_Cm_soln[1].tolist()
    del FG_Cm_soln[1]






    def manOutputDF(x_S1LnQ, costDict, siteType, capFn, effDict, mapDict, outputDict):
        for S1, Ln, sku, period in x_S1LnQ:
            val = x_S1LnQ[S1, Ln, sku, period].value * scalingVolume

            if val > 0:
                cst = costDict[S1][Ln][sku] / scalingVolume * scalingCost

                dictCstMan[siteType, S1, Ln, period] += cst * val
                dictVolQty[siteType, S1, Ln, period] += val

                cap = capFn('none', S1, Ln, sku) * scalingVolume / effDict[S1][Ln]['Period_Availability']
                try:
                    hrs = val / cap
                except ZeroDivisionError:
                    hrs = 0

                dictHrsUsage[siteType, S1, Ln, period] += hrs

                siteGrp = 'none'
                for gp in mapDict:
                    for s1 in mapDict[gp]:
                        for ln in mapDict[gp][s1]:
                            if s1 == S1 and ln == Ln:
                                siteGrp = gp
                skuGrp = sku if modelGrpLevel == 'Yes' else Unit_data['SKU_Group'][sku]
                parent = Unit_data['Parent_Group'][sku]
                child = Unit_data['Child_Group'][sku]
                outputDict[sc, S1, Ln, sku, period] = [val, cst, cap, hrs, siteGrp, skuGrp, parent, child]


    Pd_dict = {}

    manOutputDF(model.x_PdStQ, costDict=PdCst_data, siteType='Pd', capFn=PdCap_fn, effDict=PdCapEff_data, mapDict=PdGrpMap_data, outputDict=Pd_dict)
    PdStQ_soln = pnd.DataFrame(Pd_dict.items()).set_index(0)
    PdStQ_soln.index = pnd.MultiIndex.from_tuples(PdStQ_soln.index, names=['Code', 'Prod Site', 'Stream', 'SKU', 'Period'])
    PdStQ_soln[['Liters', 'Var Cost ($/Lt)', 'Eff. Capacity (Lt/Hrs)', 'Rqd. Stream Hrs', 'Site Group', 'SKU Group', 'Parent SKU Grp', 'Child SKU Grp']] = PdStQ_soln[1].tolist()
    del PdStQ_soln[1]






    Pk_dict = {}

    manOutputDF(model.x_PkLnQ, costDict=PkCst_data, siteType='Pk', capFn=PkCap_fn, effDict=PkCapEff_data, mapDict=PkGrpMap_data, outputDict=Pk_dict)
    PkLnQ_soln = pnd.DataFrame(Pk_dict.items()).set_index(0)
    PkLnQ_soln.index = pnd.MultiIndex.from_tuples(PkLnQ_soln.index, names=['Code', 'Pack Site', 'Line', 'SKU', 'Period'])
    PkLnQ_soln[['Cases', 'Var Cost ($/Cs)', 'Eff. Capacity (Cs/Hrs)', 'Rqd. Line Hours', 'Site Group', 'SKU Group', 'Parent SKU Grp', 'Child SKU Grp']] = PkLnQ_soln[1].tolist()
    del PkLnQ_soln[1]

    rPk_dict = {}

    manOutputDF(model.x_rPkLnQ, costDict=rPkCst_data, siteType='rPk', capFn=rPkCap_fn, effDict=rPkCapEff_data, mapDict=rPkGrpMap_data, outputDict=rPk_dict)
    rPkLnQ_soln = pnd.DataFrame(rPk_dict.items()).set_index(0)
    rPkLnQ_soln.index = pnd.MultiIndex.from_tuples(rPkLnQ_soln.index, names=['Code', 'RePack Site', 'Line', 'SKU', 'Period'])
    rPkLnQ_soln[['Cases', 'Var Cost ($/Cs)', 'Eff. Capacity (Cs/Hrs)', 'Rqd. Line Hours', 'Site Group', 'SKU Group', 'Parent SKU Grp', 'Child SKU Grp']] = rPkLnQ_soln[1].tolist()
    del rPkLnQ_soln[1]

    FG_dict = {}
    for FG, sku in model.x_FGSQ_init:
        val = model.x_FGSQ_init[FG, sku].value * scalingVolume

        if val > 0:
            hrCst = FGCst_data["Storage_Cost"][FG] / scalingVolume * scalingCost
            cst = 0
            skuGrp = sku if modelGrpLevel == 'Yes' else Unit_data['SKU_Group'][sku]
            parent = Unit_data['Parent_Group'][sku]
            child = Unit_data['Child_Group'][sku]
            FG_dict[sc, FG, sku, 'P0(Start-Up)'] = [val, hrCst, cst, skuGrp, parent, child]
    for FG, sku, period in model.x_FGSQ:
        val = model.x_FGSQ[FG, sku, period].value * scalingVolume

        if val > 0:
            dictVolQty['FG', FG, 'Total', period] += val
            hrCst = FGCst_data["Storage_Cost"][FG] / scalingVolume * scalingCost
            cst = hrCst * Period_data[period]["Period_Length"]
            dictCstStorage['FG', FG, 'Total', period] += val * cst
            skuGrp = sku if modelGrpLevel == 'Yes' else Unit_data['SKU_Group'][sku]
            parent = Unit_data['Parent_Group'][sku]
            child = Unit_data['Child_Group'][sku]
            FG_dict[sc, FG, sku, period] = [val, hrCst, cst, skuGrp, parent, child]
    FGSQ_soln = pnd.DataFrame(FG_dict.items()).set_index(0)
    FGSQ_soln.index = pnd.MultiIndex.from_tuples(FGSQ_soln.index, names=['Code', 'FG Warehouse', 'SKU', 'Period'])
    FGSQ_soln[['Cases', 'Var Cost ($/Cs/Hr)', 'Var Period Cost($/Cs)', 'SKU Group', 'Parent SKU Grp', 'Child SKU Grp']] = FGSQ_soln[1].tolist()
    del FGSQ_soln[1]




    WIP_dict = {}
    if enableWIPInitialStockPenalty == 'Yes':
        for WIP, sku in model.x_WIPSQ_initPenalty:
            val = model.x_WIPSQ_initPenalty[WIP, sku].value * scalingVolume

            if val > 0:
                hrCst = WIPCst_data["Storage_Cost"][WIP] / scalingVolume * scalingCost
                cst = hrCst * Period_data[period_lst[0]]["Period_Length"]
                dictCstStorageInit['WIP', WIP, 'Total', period_lst[0]] += val * (cst + 20)
                skuGrp = sku if modelGrpLevel == 'Yes' else Unit_data['SKU_Group'][sku]
                parent = Unit_data['Parent_Group'][sku]
                child = Unit_data['Child_Group'][sku]
                WIP_dict[sc, WIP, sku, 'P0(Start-Up Shortfall)'] = [val, hrCst, cst, skuGrp, parent, child]
    for WIP, sku in model.x_WIPSQ_init:
        val = model.x_WIPSQ_init[WIP, sku].value * scalingVolume

        if val > 0:
            hrCst = WIPCst_data["Storage_Cost"][WIP] / scalingVolume * scalingCost
            cst = 0
            skuGrp = sku if modelGrpLevel == 'Yes' else Unit_data['SKU_Group'][sku]
            parent = Unit_data['Parent_Group'][sku]
            child = Unit_data['Child_Group'][sku]
            WIP_dict[sc, WIP, sku, 'P0(Start-Up)'] = [val, hrCst, cst, skuGrp, parent, child]
    for WIP, sku, period in model.x_WIPSQ:
        val = model.x_WIPSQ[WIP, sku, period].value * scalingVolume

        if val > 0:
            dictVolQty['WIP', WIP, 'Total', period] += val
            hrCst = WIPCst_data["Storage_Cost"][WIP] / scalingVolume * scalingCost
            cst = hrCst * Period_data[period]["Period_Length"]
            dictCstStorage['WIP', WIP, 'Total', period] += val * cst
            skuGrp = sku if modelGrpLevel == 'Yes' else Unit_data['SKU_Group'][sku]
            parent = Unit_data['Parent_Group'][sku]
            child = Unit_data['Child_Group'][sku]
            WIP_dict[sc, WIP, sku, period] = [val, hrCst, cst, skuGrp, parent, child]
    WIPSQ_soln = pnd.DataFrame(WIP_dict.items()).set_index(0)
    WIPSQ_soln.index = pnd.MultiIndex.from_tuples(WIPSQ_soln.index, names=['Code', 'WIP Warehouse', 'SKU', 'Period'])
    WIPSQ_soln[['Cases', 'Var Cost ($/Cs/Hr)', 'Var Period Cost($/Cs)', 'SKU Group', 'Parent SKU Grp', 'Child SKU Grp']] = WIPSQ_soln[1].tolist()
    del WIPSQ_soln[1]


    if enableLoadSF_Full == "Yes" or enableLoadSF_MinFract == 'Yes':
        for s1, s2, period in model.x_FG_Cm_LSF:
            val = model.x_FG_Cm_LSF[s1, s2, period].value
            dictLoadsSF['FG_Cm', s1, s2, period] = val

            dictCstFreightSF['FG_Cm', s1, s2, period] = val * D_Lanes_data[s1][s2]['Cost'] * scalingCost


    dictHrsUtilMP = {}
    for Pd in PdCap_data:
        dictHrsUtilMP['Pd', Pd, 'Avail', 'Total'] = 0
        dictHrsUtilMP['Pd', Pd, 'Usage', 'Total'] = 0
        dictHrsUtilMP['Pd', Pd, 'Avail', 'Report Total'] = 0
        dictHrsUtilMP['Pd', Pd, 'Usage', 'Report Total'] = 0
    for Pk in PkCap_data:
        dictHrsUtilMP['Pk', Pk, 'Avail', 'Total'] = 0
        dictHrsUtilMP['Pk', Pk, 'Usage', 'Total'] = 0
        dictHrsUtilMP['Pk', Pk, 'Avail', 'Report Total'] = 0
        dictHrsUtilMP['Pk', Pk, 'Usage', 'Report Total'] = 0
    for rPk in rPkCap_data:
        dictHrsUtilMP['rPk', rPk, 'Avail', 'Total'] = 0
        dictHrsUtilMP['rPk', rPk, 'Usage', 'Total'] = 0
        dictHrsUtilMP['rPk', rPk, 'Avail', 'Report Total'] = 0
        dictHrsUtilMP['rPk', rPk, 'Usage', 'Report Total'] = 0
    dictVolUtilMP = {}
    for WIP in WIPCap_data:
        dictVolUtilMP['WIP', WIP, 'Avail', 'Total'] = 0
        dictVolUtilMP['WIP', WIP, 'Usage', 'Total'] = 0
        dictVolUtilMP['WIP', WIP, 'Avail', 'Report Total'] = 0
        dictVolUtilMP['WIP', WIP, 'Usage', 'Report Total'] = 0
    for FG in FGCap_data:
        dictVolUtilMP['FG', FG, 'Avail', 'Total'] = 0
        dictVolUtilMP['FG', FG, 'Usage', 'Total'] = 0
        dictVolUtilMP['FG', FG, 'Avail', 'Report Total'] = 0
        dictVolUtilMP['FG', FG, 'Usage', 'Report Total'] = 0

    for period in period_lst:

        for Pd in PdCap_data:
            totAvail = 0
            totUsage = 0
            for St in PdCap_data[Pd]:

                capLimit = CapLimit_data.get('Pd', {}).get(Pd + '>' + St, {}).get(period, 1)
                hrs = Period_data[period]['Period_Length'] * PdCapEff_data[Pd][St]['Period_Availability'] * capLimit
                dictHrsAvail['Pd', Pd, St, period] = hrs
                totAvail += hrs
                totUsage += dictHrsUsage['Pd', Pd, St, period]

                for parentGrp, site in dictParentPdSt:
                    if site == Pd and St in dictParentPdSt[parentGrp, site]:
                        dictHrsUsageParent['Pd', Pd, parentGrp, period] += dictHrsUsage['Pd', Pd, St, period]
                        dictHrsAvailParent['Pd', Pd, parentGrp, period] += hrs
            dictHrsUtil['Pd', Pd, 'Total', period] = totUsage / totAvail * 100

            dictHrsUtilMP['Pd', Pd, 'Avail', 'Total'] += totAvail
            dictHrsUtilMP['Pd', Pd, 'Usage', 'Total'] += totUsage
            if period in reportPeriod:
                dictHrsUtilMP['Pd', Pd, 'Avail', 'Report Total'] += totAvail
                dictHrsUtilMP['Pd', Pd, 'Usage', 'Report Total'] += totUsage
            for parentGrp, site in dictParentPdSt:
                if site == Pd:
                    dictHrsUtilParent['Pd', Pd, parentGrp, period] = dictHrsUsageParent['Pd', Pd, parentGrp, period] / dictHrsAvailParent['Pd', Pd, parentGrp, period] * 100
        for Pk in PkCap_data:
            totAvail = 0
            totUsage = 0
            for Ln in PkCap_data[Pk]:

                capLimit = CapLimit_data.get('Pk', {}).get(Pk + '>' + Ln, {}).get(period, 1)
                hrs = Period_data[period]['Period_Length'] * PkCapEff_data[Pk][Ln]['Period_Availability'] * capLimit
                dictHrsAvail['Pk', Pk, Ln, period] = hrs
                totAvail += hrs
                totUsage += dictHrsUsage['Pk', Pk, Ln, period]

                for parentGrp, site in dictParentPkLn:
                    if site == Pk and Ln in dictParentPkLn[parentGrp, site]:
                        dictHrsUsageParent['Pk', Pk, parentGrp, period] += dictHrsUsage['Pk', Pk, Ln, period]
                        dictHrsAvailParent['Pk', Pk, parentGrp, period] += hrs
            dictHrsUtil['Pk', Pk, 'Total', period] = totUsage / totAvail * 100

            dictHrsUtilMP['Pk', Pk, 'Avail', 'Total'] += totAvail
            dictHrsUtilMP['Pk', Pk, 'Usage', 'Total'] += totUsage
            if period in reportPeriod:
                dictHrsUtilMP['Pk', Pk, 'Avail', 'Report Total'] += totAvail
                dictHrsUtilMP['Pk', Pk, 'Usage', 'Report Total'] += totUsage
            for parentGrp, site in dictParentPkLn:
                if site == Pk:
                    dictHrsUtilParent['Pk', Pk, parentGrp, period] = dictHrsUsageParent['Pk', Pk, parentGrp, period] / dictHrsAvailParent['Pk', Pk, parentGrp, period] * 100
        for rPk in rPkCap_data:
            totAvail = 0
            totUsage = 0
            for Ln in rPkCap_data[rPk]:

                capLimit = CapLimit_data.get('rPk', {}).get(rPk + '>' + Ln, {}).get(period, 1)
                hrs = Period_data[period]['Period_Length'] * rPkCapEff_data[rPk][Ln]['Period_Availability'] * capLimit
                dictHrsAvail['rPk', rPk, Ln, period] = hrs
                totAvail += hrs
                totUsage += dictHrsUsage['rPk', rPk, Ln, period]

                for parentGrp, site in dictParentrPkLn:
                    if site == rPk and Ln in dictParentrPkLn[parentGrp, site]:
                        dictHrsUsageParent['rPk', rPk, parentGrp, period] += dictHrsUsage['rPk', rPk, Ln, period]
                        dictHrsAvailParent['rPk', rPk, parentGrp, period] += hrs
            dictHrsUtil['rPk', rPk, 'Total', period] = totUsage / totAvail * 100

            dictHrsUtilMP['rPk', rPk, 'Avail', 'Total'] += totAvail
            dictHrsUtilMP['rPk', rPk, 'Usage', 'Total'] += totUsage
            if period in reportPeriod:
                dictHrsUtilMP['rPk', rPk, 'Avail', 'Report Total'] += totAvail
                dictHrsUtilMP['rPk', rPk, 'Usage', 'Report Total'] += totUsage
            for parentGrp, site in dictParentrPkLn:
                if site == rPk:
                    dictHrsUtilParent['rPk', rPk, parentGrp, period] = dictHrsUsageParent['rPk', rPk, parentGrp, period] / dictHrsAvailParent['rPk', rPk, parentGrp, period] * 100

        for FG in FGCap_data:
            totAvail = FGCap_data[FG]["Ph1"]["Total_Storage"] * scalingVolume
            totUsage = dictVolQty['FG', FG, 'Total', period]
            dictVolUtil['FG', FG, 'Total', period] = totUsage / totAvail * 100

            dictVolUtilMP['FG', FG, 'Avail', 'Total'] += totAvail
            dictVolUtilMP['FG', FG, 'Usage', 'Total'] += totUsage
            if period in reportPeriod:
                dictVolUtilMP['FG', FG, 'Avail', 'Report Total'] += totAvail
                dictVolUtilMP['FG', FG, 'Usage', 'Report Total'] += totUsage
        for WIP in WIPCap_data:
            totAvail = WIPCap_data[WIP]["Ph1"]["Total_Storage"] * scalingVolume
            totUsage = dictVolQty['WIP', WIP, 'Total', period]
            dictVolUtil['WIP', WIP, 'Total', period] = totUsage / totAvail * 100

            dictVolUtilMP['WIP', WIP, 'Avail', 'Total'] += totAvail
            dictVolUtilMP['WIP', WIP, 'Usage', 'Total'] += totUsage
            if period in reportPeriod:
                dictVolUtilMP['WIP', WIP, 'Avail', 'Report Total'] += totAvail
                dictVolUtilMP['WIP', WIP, 'Usage', 'Report Total'] += totUsage
            for Ph in WIPSiteType_data[WIP]:
                sharedSite = WIPSiteType_data[WIP][Ph]['Shared_FG_Site']
                if pnd.notna(sharedSite) and sharedSite != 'None':
                    totCombiUsage = (dictVolQty['WIP', WIP, 'Total', period] + dictVolQty['FG', sharedSite, 'Total', period])
                    dictVolUtil['WIP/FG', WIP+'/'+sharedSite, 'Total', period] = totCombiUsage / totAvail * 100


                    if ('WIP/FG', WIP+'/'+sharedSite, 'Avail', 'Total') not in dictVolUtilMP:
                        dictVolUtilMP['WIP/FG', WIP+'/'+sharedSite, 'Avail', 'Total'] = 0
                        dictVolUtilMP['WIP/FG', WIP+'/'+sharedSite, 'Usage', 'Total'] = 0
                        dictVolUtilMP['WIP/FG', WIP+'/'+sharedSite, 'Avail', 'Report Total'] = 0
                        dictVolUtilMP['WIP/FG', WIP+'/'+sharedSite, 'Usage', 'Report Total'] = 0

                        dictVolUtil['WIP/FG', WIP+'/'+sharedSite, 'Total', 'Total'] = 0
                        dictVolUtil['WIP/FG', WIP + '/' + sharedSite, 'Total', 'Report Total'] = 0
                    dictVolUtilMP['WIP/FG', WIP+'/'+sharedSite, 'Avail', 'Total'] += totAvail
                    dictVolUtilMP['WIP/FG', WIP+'/'+sharedSite, 'Usage', 'Total'] += totCombiUsage
                    if period in reportPeriod:
                        dictVolUtilMP['WIP/FG', WIP+'/'+sharedSite, 'Avail', 'Report Total'] += totAvail
                        dictVolUtilMP['WIP/FG', WIP+'/'+sharedSite, 'Usage', 'Report Total'] += totCombiUsage


        for grp in PdGrpMap_data:
            val = model.x_PdGrpSFQ[grp, period].value * scalingVolume
            dictVolSF['Pd', grp, 'Total', period] += val
            cst = PdGrp_data['Min_Grp_Penalty'][grp] / scalingVolume * scalingCost
            if val > 0 and pnd.notna(cst):
                dictCstPenalty['Pd', grp, 'Total', period] += val * cst
        for grp in PkGrpMap_data: #here AG - TOP
            val = model.x_PkGrpSFQ[grp, period].value * scalingVolume
            dictVolSF['Pk', grp, 'Total', period] += val
            cst = model.PkSFCst[grp] / scalingVolume * scalingCost
            if val > 0 and pnd.notna(cst):
                dictCstPenalty['Pk', grp, 'Total', period] += val * cst
        for grp in model.PkTierLst: #here AG - Tier
            val = 0
            for Pk, Ln in model.PkLn:
                for SKU in model.PkSKU:
                    val += model.x_PkLnQ[Pk, Ln, SKU, period].value * model.x_PkGrpTSFQ_Bin[grp, period].value * scalingVolume

            dictVolTier['Pk', grp, 'Total', period] += val
            cst = model.PkSFTCst[grp] / scalingVolume * scalingCost
            if val > 0 and pnd.notna(cst):
                dictTierCstPenalty['Pk', grp, 'Total', period] += val * cst
        for grp in model.PkRbLst:  #here AG - Rebate
            val = model.x_PkGrpRQ[grp, period].value * scalingVolume
            dictVolRB['Pk', grp, 'Total', period] += val
            cst = PkGrp_data['Rebate'][grp] / scalingVolume * scalingCost
            if val > 0 and pnd.notna(cst):
                dictRBCstPenalty['Pk', grp, 'Total', period] += val * cst
        for grp in rPkGrpMap_data:
            val = model.x_rPkGrpSFQ[grp, period].value * scalingVolume
            dictVolSF['rPk', grp, 'Total', period] += val
            cst = rPkGrp_data['Min_Grp_Penalty'][grp] / scalingVolume * scalingCost
            if val > 0 and pnd.notna(cst):
                dictCstPenalty['rPk', grp, 'Total', period] += val * cst


    mapSite = {'Pd': 'Production', 'Pk': 'Packaging', 'rPk': 'RePacking', 'WIP': 'WIP Storage', 'FG': 'FG Storage'}
    lstRoute = ['Prod to Pack', 'Pack to Distr', 'Pack to FG', 'Pack to WIP', 'WIP to RePack', 'RePack to FG', 'FG to Distr']

    summaryDict = {}

    for dim, s1, s2, period in dictCstMan:
        val = dictCstMan[dim, s1, s2, period]
        if period != 'Total' and period != 'Report Total':
            summaryDict[sc, 'Cost', 'Manufacturing', dim, s1, s2, '$', 'Total'] += val
        if period in reportPeriod:
            summaryDict[sc, 'Cost', 'Manufacturing', dim, s1, s2, '$', 'Report Total'] += val
        summaryDict[sc, 'Cost', 'Manufacturing', dim, s1, s2, '$', period] = val
    for dim, s1, s2, period in dictCstFreight:
        val = dictCstFreight[dim, s1, s2, period]
        if period != 'Total' and period != 'Report Total':
            summaryDict[sc, 'Cost', 'Freight', dim, s1, s2, '$', 'Total'] += val
        if period in reportPeriod:
            summaryDict[sc, 'Cost', 'Freight', dim, s1, s2, '$', 'Report Total'] += val
        summaryDict[sc, 'Cost', 'Freight', dim, s1, s2, '$', period] = val
    for dim, s1, s2, period in dictCstDunnage:
        val = dictCstDunnage[dim, s1, s2, period]
        if period != 'Total' and period != 'Report Total':
            summaryDict[sc, 'Cost', 'Dunnage', dim, s1, s2, '$', 'Total'] += val
        if period in reportPeriod:
            summaryDict[sc, 'Cost', 'Dunnage', dim, s1, s2, '$', 'Report Total'] += val
        summaryDict[sc, 'Cost', 'Dunnage', dim, s1, s2, '$', period] = val
    for dim, s1, s2, period in dictCstPenalty: #here AG - TOP
        val = dictCstPenalty[dim, s1, s2, period]
        if period != 'Total' and period != 'Report Total':
            summaryDict[sc, 'Cost', 'Co-Man Penalties', dim, s1, s2, '$', 'Total'] += val
        if period in reportPeriod:
            summaryDict[sc, 'Cost', 'Co-Man Penalties', dim, s1, s2, '$', 'Report Total'] += val
        summaryDict[sc, 'Cost', 'Co-Man Penalties', dim, s1, s2, '$', period] = val
    for dim, s1, s2, period in dictTierCstPenalty: #here AG - Tier
        val = dictTierCstPenalty[dim, s1, s2, period]
        if period != 'Total' and period != 'Report Total':
            summaryDict[sc, 'Cost', 'Tier Penalties', dim, s1, s2, '$', 'Total'] += val
        if period in reportPeriod:
            summaryDict[sc, 'Cost', 'Tier Penalties', dim, s1, s2, '$', 'Report Total'] += val
        summaryDict[sc, 'Cost', 'Tier Penalties', dim, s1, s2, '$', period] = val
    for dim, s1, s2, period in dictRBCstPenalty: #here AG - Rebate
        val = dictRBCstPenalty[dim, s1, s2, period]
        if period != 'Total' and period != 'Report Total':
            summaryDict[sc, 'Cost', 'Rebate Savings', dim, s1, s2, '$', 'Total'] += (val * -1)
        if period in reportPeriod:
            summaryDict[sc, 'Cost', 'Rebate Savings', dim, s1, s2, '$', 'Report Total'] += (val * -1)
        summaryDict[sc, 'Cost', 'Rebate Savings', dim, s1, s2, '$', period] = (val * -1)
    for dim, s1, s2, period in dictCstHandling:
        val = dictCstHandling[dim, s1, s2, period]
        if period != 'Total' and period != 'Report Total':
            summaryDict[sc, 'Cost', 'Handling', dim, s1, s2, '$', 'Total'] += val
        if period in reportPeriod:
            summaryDict[sc, 'Cost', 'Handling', dim, s1, s2, '$', 'Report Total'] += val
        summaryDict[sc, 'Cost', 'Handling', dim, s1, s2, '$', period] = val
    for dim, s1, s2, period in dictCstStorage:
        val = dictCstStorage[dim, s1, s2, period]
        if period != 'Total' and period != 'Report Total':
            summaryDict[sc, 'Cost', 'Storage', dim, s1, s2, '$', 'Total'] += val
        if period in reportPeriod:
            summaryDict[sc, 'Cost', 'Storage', dim, s1, s2, '$', 'Report Total'] += val
        summaryDict[sc, 'Cost', 'Storage', dim, s1, s2, '$', period] = val
    for dim, s1, s2, period in dictCstStorageInit:
        val = dictCstStorageInit[dim, s1, s2, period]
        if period != 'Total' and period != 'Report Total':
            summaryDict[sc, 'Cost', 'Initial Storage', dim, s1, s2, '$', 'Total'] += val
        if period in reportPeriod:
            summaryDict[sc, 'Cost', 'Initial Storage', dim, s1, s2, '$', 'Report Total'] += val
        summaryDict[sc, 'Cost', 'Initial Storage', dim, s1, s2, '$', period] = val
    for dim, s1, s2, period in dictCstFreightSF:
        val = dictCstFreightSF[dim, s1, s2, period]
        if period != 'Total' and period != 'Report Total':
            summaryDict[sc, 'Cost', 'Loads SF', dim, s1, s2, '$', 'Total'] += val
        if period in reportPeriod:
            summaryDict[sc, 'Cost', 'Loads SF', dim, s1, s2, '$', 'Report Total'] += val
        summaryDict[sc, 'Cost', 'Loads SF', dim, s1, s2, '$', period] = val


    for dim, s1, s2, period in dictVolDelivered:
        val = dictVolDelivered[dim, s1, s2, period]
        if period != 'Total' and period != 'Report Total':
            summaryDict[sc, 'Volume', 'Demand', dim, s1, s2, 'cs', 'Total'] += val
        if period in reportPeriod:
            summaryDict[sc, 'Volume', 'Demand', dim, s1, s2, 'cs', 'Report Total'] += val
        summaryDict[sc, 'Volume', 'Demand', dim, s1, s2, 'cs', period] = val
    for dim, s1, s2, period in dictVolRequired:
        val = dictVolRequired[dim, s1, s2, period]
        if period != 'Total' and period != 'Report Total':
            summaryDict[sc, 'Volume', 'Demand', dim, s1, s2, 'cs', 'Total'] += val
        if period in reportPeriod:
            summaryDict[sc, 'Volume', 'Demand', dim, s1, s2, 'cs', 'Report Total'] += val
        summaryDict[sc, 'Volume', 'Demand', dim, s1, s2, 'cs', period] = val
    for dim, s1, s2, period in dictVolQty:
        if dim == 'Pd':
            val = dictVolQty[dim, s1, s2, period]
            if period != 'Total' and period != 'Report Total':
                summaryDict[sc, 'Volume', 'Quantity', dim, s1, s2, 'lt', 'Total'] += val
            if period in reportPeriod:
                summaryDict[sc, 'Volume', 'Quantity', dim, s1, s2, 'lt', 'Report Total'] += val
            summaryDict[sc, 'Volume', 'Quantity', dim, s1, s2, 'lt', period] = val
        else:
            val = dictVolQty[dim, s1, s2, period]
            if period != 'Total' and period != 'Report Total':
                summaryDict[sc, 'Volume', 'Quantity', dim, s1, s2, 'cs', 'Total'] += val
            if period in reportPeriod:
                summaryDict[sc, 'Volume', 'Quantity', dim, s1, s2, 'cs', 'Report Total'] += val
            summaryDict[sc, 'Volume', 'Quantity', dim, s1, s2, 'cs', period] = val
    for dim, s1, s2, period in dictVolSF: #here AG - TOP
        if dim == 'Pd':
            val = dictVolSF[dim, s1, s2, period]
            if period != 'Total' and period != 'Report Total':
                summaryDict[sc, 'Volume', 'Co-Man SF', dim, s1, s2, 'lt', 'Total'] += val
            if period in reportPeriod:
                summaryDict[sc, 'Volume', 'Co-Man SF', dim, s1, s2, 'lt', 'Report Total'] += val
            summaryDict[sc, 'Volume', 'Co-Man SF', dim, s1, s2, 'lt', period] = val
        else:
            val = dictVolSF[dim, s1, s2, period]
            if period != 'Total' and period != 'Report Total':
                summaryDict[sc, 'Volume', 'Co-Man SF', dim, s1, s2, 'cs', 'Total'] += val
            if period in reportPeriod:
                summaryDict[sc, 'Volume', 'Co-Man SF', dim, s1, s2, 'cs', 'Report Total'] += val
            summaryDict[sc, 'Volume', 'Co-Man SF', dim, s1, s2, 'cs', period] = val
    for dim, s1, s2, period in dictVolTier: #here AG - Tier
        val = dictVolTier[dim, s1, s2, period]
        if period != 'Total' and period != 'Report Total':
            summaryDict[sc, 'Volume', 'Tier Amount', dim, s1, s2, 'cs', 'Total'] += val
        if period in reportPeriod:
            summaryDict[sc, 'Volume', 'Tier Amount', dim, s1, s2, 'cs', 'Report Total'] += val
        summaryDict[sc, 'Volume', 'Tier Amount', dim, s1, s2, 'cs', period] = val
    for dim, s1, s2, period in dictVolRB: #here AG - Rebate
        val = dictVolRB[dim, s1, s2, period]
        if period != 'Total' and period != 'Report Total':
            summaryDict[sc, 'Volume', 'Rebate Volume', dim, s1, s2, 'cs', 'Total'] += val
        if period in reportPeriod:
            summaryDict[sc, 'Volume', 'Rebate Volume', dim, s1, s2, 'cs', 'Report Total'] += val
        summaryDict[sc, 'Volume', 'Rebate Volume', dim, s1, s2, 'cs', period] = val
    for dim, s1, s2, period in dictVolUtil:
        val = dictVolUtil[dim, s1, s2, period]
        summaryDict[sc, 'Volume', 'Utilization', dim, s1, s2, 'cs', period] = val
        if period == 'Total':
            summaryDict[sc, 'Volume', 'Utilization', dim, s1, s2, 'cs', period] = dictVolUtilMP[dim, s1, 'Usage', period] / dictVolUtilMP[dim, s1, 'Avail', period] * 100
        if period == 'Report Total':
            summaryDict[sc, 'Volume', 'Utilization', dim, s1, s2, 'cs', period] = dictVolUtilMP[dim, s1, 'Usage', period] / dictVolUtilMP[dim, s1, 'Avail', period] * 100



    for dim, s1, s2, period in dictHrsUsage:
        val = dictHrsUsage[dim, s1, s2, period]
        if period != 'Total' and period != 'Report Total':
            summaryDict[sc, 'Hours', 'Usage', dim, s1, s2, 'hrs', 'Total'] += val
        if period in reportPeriod:
            summaryDict[sc, 'Hours', 'Usage', dim, s1, s2, 'hrs', 'Report Total'] += val
        summaryDict[sc, 'Hours', 'Usage', dim, s1, s2, 'hrs', period] = val
    for dim, s1, s2, period in dictHrsAvail:
        val = dictHrsAvail[dim, s1, s2, period]
        if period != 'Total' and period != 'Report Total':
            summaryDict[sc, 'Hours', 'Availability', dim, s1, s2, 'hrs', 'Total'] += val
        if period in reportPeriod:
            summaryDict[sc, 'Hours', 'Availability', dim, s1, s2, 'hrs', 'Report Total'] += val
        summaryDict[sc, 'Hours', 'Availability', dim, s1, s2, 'hrs', period] = val
    for dim, s1, s2, period in dictHrsUtil:
        val = dictHrsUtil[dim, s1, s2, period]
        summaryDict[sc, 'Hours', 'Utilization', dim, s1, s2, '%', period] = val
        if period == 'Total':
            summaryDict[sc, 'Hours', 'Utilization', dim, s1, s2, '%', period] = dictHrsUtilMP[dim, s1, 'Usage', period] / dictHrsUtilMP[dim, s1, 'Avail', period] * 100
        if period == 'Report Total':
            summaryDict[sc, 'Hours', 'Utilization', dim, s1, s2, '%', period] = dictHrsUtilMP[dim, s1, 'Usage', period] / dictHrsUtilMP[dim, s1, 'Avail', period] * 100
    for dim, s1, s2, period in dictHrsUsageParent:
        val = dictHrsUsageParent[dim, s1, s2, period]
        if period != 'Total' and period != 'Report Total':
            summaryDict[sc, 'Hours', 'Usage', dim, s1, s2, 'hrs', 'Total'] += val
        if period in reportPeriod:
            summaryDict[sc, 'Hours', 'Usage', dim, s1, s2, 'hrs', 'Report Total'] += val
        summaryDict[sc, 'Hours', 'Usage', dim, s1, s2, 'hrs', period] = val
    for dim, s1, s2, period in dictHrsAvailParent:
        val = dictHrsAvailParent[dim, s1, s2, period]
        if period != 'Total' and period != 'Report Total':
            summaryDict[sc, 'Hours', 'Availability', dim, s1, s2, 'hrs', 'Total'] += val
        if period in reportPeriod:
            summaryDict[sc, 'Hours', 'Availability', dim, s1, s2, 'hrs', 'Report Total'] += val
        summaryDict[sc, 'Hours', 'Availability', dim, s1, s2, 'hrs', period] = val
    for dim, s1, s2, period in dictHrsUtilParent:
        val = dictHrsUtilParent[dim, s1, s2, period]
        if period != 'Total' and period != 'Report Total':
            summaryDict[sc, 'Hours', 'Utilization', dim, s1, s2, '%', 'Total'] = 0
        if period in reportPeriod:
            summaryDict[sc, 'Hours', 'Utilization', dim, s1, s2, '%', 'Report Total'] = 0
        summaryDict[sc, 'Hours', 'Utilization', dim, s1, s2, '%', period] = val


    for dim, s1, s2, period in dictLoadsQty:
        val = dictLoadsQty[dim, s1, s2, period]
        if period != 'Total' and period != 'Report Total':
            summaryDict[sc, 'Loads', 'Quantity', dim, s1, s2, 'No.', 'Total'] += val

            if val > 0 and dim == 'FG_Cm':
                dictKPILoads['FG_Cm', 'Total Miles', 'Total'] += val * D_Lanes_data[s1][s2]['Mileage']
                dictKPILoads['FG_Cm', 'Total Loads', 'Total'] += val
        if period in reportPeriod:
            summaryDict[sc, 'Loads', 'Quantity', dim, s1, s2, 'No.', 'Report Total'] += val

            if val > 0 and dim == 'FG_Cm':
                dictKPILoads['FG_Cm', 'Total Miles', 'Report Total'] += val * D_Lanes_data[s1][s2]['Mileage']
                dictKPILoads['FG_Cm', 'Total Loads', 'Report Total'] += val
        summaryDict[sc, 'Loads', 'Quantity', dim, s1, s2, 'No.', period] = val
        if val > 0 and dim == 'FG_Cm':
            dictKPILoads['FG_Cm', 'Total Miles', period] += val * D_Lanes_data[s1][s2]['Mileage']
            dictKPILoads['FG_Cm', 'Total Loads', period] += val
    if enableLoadSF_Full == "Yes" or enableLoadSF_MinFract == 'Yes':
        for dim, s1, s2, period in dictLoadsSF:
            val = dictLoadsSF[dim, s1, s2, period]
            if period != 'Total' and period != 'Report Total':
                summaryDict[sc, 'Loads', 'Short Fall', dim, s1, s2, 'No.', 'Total'] += val
            if period in reportPeriod:
                summaryDict[sc, 'Loads', 'Short Fall', dim, s1, s2, 'No.', 'Report Total'] += val
            summaryDict[sc, 'Loads', 'Short Fall', dim, s1, s2, 'No.', period] = val



    for period in period_lstExt:
        dim = 'FG_Cm'
        totMiles = dictKPILoads[dim, 'Total Miles', period]
        totLoads = dictKPILoads[dim, 'Total Loads', period]
        summaryDict[sc, 'KPI', 'Avg. Miles/Load', dim, 'Total', 'Total', 'No.', period] = totMiles / totLoads


    Summary_soln = pnd.DataFrame(summaryDict.items()).set_index(0)
    Summary_soln.index = pnd.MultiIndex.from_tuples(Summary_soln.index, names=['Code', 'H1', 'H2', 'H3', 'H4', 'H5', 'UOM', 'Period'])


    objDict = {'Objective Fn': model.objective() * scalingCost}
    objDict['Scenario Description'] = cpDescription_data
    objDict['Scenario Code'] = cpScenario_data
    objDict['Model Version'] = model_version
    objDict['Model Version'] = model_version

    for tab in cpScenarioNotes_data['Note']:
        objDict[tab + ' Notes'] = cpScenarioNotes_data['Note'][tab]

    for data in cpValSettings_data['VALUE']:
        objDict[data] = cpValSettings_data['VALUE'][data]

    for data in cpFnSettings_data['VALUE']:
        objDict[data] = cpFnSettings_data['VALUE'][data]

    for data in cpSiteExcl_data:
        tempLst = []
        for site in cpSiteExcl_data[data]:
            if pnd.notna(site):
                tempLst.append(site)
        objDict['Excluded Sites: ' + data] = tempLst

    for data in cpPenaltyExcl_data:
        tempLst = []
        for site in cpPenaltyExcl_data[data]:
            if pnd.notna(site):
                tempLst.append(site)
        objDict['Excluded Penalties: ' + data] = tempLst

    for data in cpConstraints_data:
        tempString = ''
        for site in cpConstraints_data[data]:
            tempString += str(site) + str(cpConstraints_data[data][site]['Operator']) + str(cpConstraints_data[data][site]['Value (per Period)']) + '|'
        objDict['Capacity Constraints: ' + data] = tempString



    obj_soln = pnd.DataFrame(objDict.items())
    obj_soln.rename(columns={obj_soln.columns[0]: "H1", obj_soln.columns[1]: "H2"}, inplace=True)
    obj_soln = obj_soln.set_index('H1')

    #Tag Reno: Output file
    with pnd.ExcelWriter(r'excelFiles/solved/' + scenarioId + ".xlsx", engine='xlsxwriter') as writer:

        sheetDim = {}

        #Tag Reno - Chaning scenario status
        db_cur.execute('UPDATE public."Scenarios" SET scenario_status = %s, output_filename = %s WHERE id = %s', (3, outputFileName,scenarioId))
        db_conn.commit()
        #Tag Reno - Leave print here
        print('*** Writing output file ***')

        # region Tag Reno - DB Table: Model (NEW ANGELO)
        obj_soln.to_excel(writer, sheet_name='Model', index=True, merge_cells=False)
        sheetDim['Model'] = (len(obj_soln) + 1, obj_soln.index.nlevels + len(obj_soln.columns) - 1)
        index = 0
        sql = 'INSERT INTO public."Model"(scenario_id, h1, h2) VALUES(%s, %s, %s)'
        for row in obj_soln.index:
            h1 = row
            h2 = obj_soln.loc[row, 'H2']  # Access the corresponding value in the DataFrame for h2
            index += 1
            db_cur.execute(sql, (scenarioId, h1, h2))
        db_conn.commit()
        # endregion

        # region Tag Reno - DB Table: Summary (NEW ANGELO)
        Summary_soln.unstack(level=-1).droplevel(0, axis=1).reindex(columns=period_lstExt).to_excel(writer,sheet_name='Summary',index=True,merge_cells=False)
        sheetDim['Summary'] = (len(Summary_soln) + 1, Summary_soln.index.nlevels + len(Summary_soln.columns) - 1)
        sql = 'INSERT INTO public."Summary"(scenario_id, h1, h2, h3, h4, h5, uom, total, report_total) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)'

        for row in Summary_soln.itertuples():
            total_type = row.Index[7]

            if(total_type != 'Total' and total_type != 'Report Total'):
                continue
            
            h1 = row.Index[1]
            h2 = row.Index[2]
            h3 = row.Index[3]
            h4 = row.Index[4]
            h5 = row.Index[5]
            uom = row.Index[6]
            total = None
            report_total = None

            if(total_type == 'Total'):
                total = row[1]
                report_total = None  
            elif(total_type == 'Report Total'):
                report_total = row[1]
                total = None

            db_cur.execute(sql, (scenarioId, h1, h2, h3, h4, h5, uom, total, report_total))
        db_conn.commit()
        # endregion

        # region Tag Reno - DB Table: PdS_Pd
        PdS_Pd_soln.to_excel(writer, sheet_name='PdS_Pd', index=True, merge_cells=False)
        sheetDim['PdS_Pd'] = (len(PdS_Pd_soln) + 1, PdS_Pd_soln.index.nlevels + len(PdS_Pd_soln.columns) - 1)
        index = 0
        sql = 'INSERT INTO public."PdS_Pd"(scenario_id, prod_supplier, prod_site, raw_material, period, quantity) VALUES(%s, %s, %s, %s, %s, %s)'
        for row in PdS_Pd_soln.index:
            prod_supplier = row[0]
            prod_site = row[1]
            raw_material = row[2]
            period = row[3]
            quantity = float(PdS_Pd_soln['Quantity'].iloc[index])
            index += 1
            db_cur.execute(sql, (scenarioId, prod_supplier, prod_site, raw_material, period, quantity))
        db_conn.commit()
        # endregion

        # region Tag Reno - DB Table: PkS_Pk
        PkS_Pk_soln.to_excel(writer, sheet_name='PkS_Pk', index=True, merge_cells=False)
        sheetDim['PkS_Pk'] = (len(PkS_Pk_soln) + 1, PkS_Pk_soln.index.nlevels + len(PkS_Pk_soln.columns) - 1)
        index = 0
        sql = 'INSERT INTO public."PkS_Pk"(scenario_id, packaging_supplier, pack_site, raw_material, period, quantity) VALUES(%s, %s, %s, %s, %s, %s)'
        for row in PkS_Pk_soln.index:
            pack_supplier = row[0]
            pack_site = row[1]
            raw_material = row[2]
            period = row[3]
            quantity = float(PkS_Pk_soln['Quantity'].iloc[index])
            index += 1
            db_cur.execute(sql, (scenarioId, pack_supplier, pack_site, raw_material, period, quantity))
        db_conn.commit()
        # endregion

        # region Tag Reno - DB Table: rPkS_rPk
        rPkS_rPk_soln.to_excel(writer, sheet_name='rPkS_rPk', index=True, merge_cells=False)
        sheetDim['rPkS_rPk'] = (len(rPkS_rPk_soln) + 1, rPkS_rPk_soln.index.nlevels + len(rPkS_rPk_soln.columns) - 1)
        index = 0
        sql = 'INSERT INTO public."rPkS_rPk"(scenario_id, repack_supplier, raw_material, period, quantity, repack_site) VALUES(%s, %s, %s, %s, %s, %s)'
        for row in rPkS_rPk_soln.index:
            repack_supplier = row[0]
            repack_site = row[1]
            raw_material = row[2]
            period = row[3]
            quantity = float(rPkS_rPk_soln['Quantity'].iloc[index])
            index += 1
            db_cur.execute(sql, (scenarioId, repack_supplier, raw_material, period, quantity, repack_site))
        db_conn.commit()
        # endregion

        # region Tag Reno - DB Table: Pd_Pk
        Pd_Pk_soln.to_excel(writer, sheet_name='Pd_Pk', index=True, merge_cells=False)
        sheetDim['Pd_Pk'] = (len(Pd_Pk_soln) + 1, Pd_Pk_soln.index.nlevels + len(Pd_Pk_soln.columns) - 1)
        index = 0
        sql = 'INSERT INTO public."Pd_Pk"(scenario_id, prod_site, pack_site, sku, period, litres, dist_cost, route_miles, truck_loads, sku_group, parent_sku_group, child_sku_group) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        for row in Pd_Pk_soln.index:
            prod_site = row[1]
            pack_site = row[2]
            sku = row[3]
            period = row[4]
            litres = float(Pd_Pk_soln['Liters'].iloc[index])
            dist_cost = float(Pd_Pk_soln['Dist. Cost ($/Cs)'].iloc[index])
            route_miles = float(Pd_Pk_soln['Route Miles'].iloc[index])
            truck_loads = float(Pd_Pk_soln['Truck Loads'].iloc[index])
            sku_group = Pd_Pk_soln['SKU Group'].iloc[index]
            parent_sku_group = "nan"
            child_sku_group = "nan"
            db_cur.execute(sql, (
            scenarioId, prod_site, pack_site, sku, period, litres, dist_cost, route_miles, truck_loads, sku_group,
            parent_sku_group, child_sku_group))
        db_conn.commit()
        # endregion

        # region Tag Reno - DB Table: Pk_FG
        Pk_FG_soln.to_excel(writer, sheet_name='Pk_FG', index=True, merge_cells=False)
        sheetDim['Pk_FG'] = (len(Pk_FG_soln) + 1, Pk_FG_soln.index.nlevels + len(Pk_FG_soln.columns) - 1)
        index = 0
        sql = 'INSERT INTO public."Pk_FG"(scenario_id, pack_site, fg_warehouse, sku, period, cases, dist_cost, wh_handling, route_miles, truck_loads, sku_group, parent_sku_group, child_sku_group) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        for row in Pk_FG_soln.index:
            pack_site = row[1]
            fg_warehouse = row[2]
            sku = row[3]
            period = row[4]
            cases = float(Pk_FG_soln['Cases'].iloc[index])
            dist_cost = float(Pk_FG_soln['Dist. Cost ($/Cs)'].iloc[index])
            wh_handling = float(Pk_FG_soln['WH Handling ($/Cs)'].iloc[index])
            route_miles = float(Pk_FG_soln['Route Miles'].iloc[index])
            truck_loads = float(Pk_FG_soln['Truck Loads'].iloc[index])
            sku_group = Pk_FG_soln['SKU Group'].iloc[index]
            parent_sku_group = str(Pk_FG_soln['Parent SKU Grp'].iloc[index])
            child_sku_group = str(Pk_FG_soln['Child SKU Grp'].iloc[index])
            index += 1
            db_cur.execute(sql, (
            scenarioId, pack_site, fg_warehouse, sku, period, cases, dist_cost, wh_handling, route_miles, truck_loads,
            sku_group, parent_sku_group, child_sku_group))
        db_conn.commit()
        # endregion

        # region Tag Reno - DB Table: Pk_WIP
        Pk_WIP_soln.to_excel(writer, sheet_name='Pk_WIP', index=True, merge_cells=False)
        sheetDim['Pk_WIP'] = (len(Pk_WIP_soln) + 1, Pk_WIP_soln.index.nlevels + len(Pk_WIP_soln.columns) - 1)
        sql = 'INSERT INTO public."Pk_WIP"(scenario_id, pack_site, wip_warehouse, sku, period, cases, dist_cost, wh_handling, route_miles, truck_loads, sku_group, parent_sku_group, child_sku_group) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        index = 0
        for row in Pk_WIP_soln.index:
            pack_site = row[1]
            wip_warehouse = row[2]
            sku = row[3]
            period = row[4]
            cases = Pk_WIP_soln['Cases'].iloc[index]
            dist_cost = float(Pk_WIP_soln['Dist. Cost ($/Cs)'].iloc[index])
            wh_handling = float(Pk_WIP_soln['WH Handling ($/Cs)'].iloc[index])
            route_miles = float(Pk_WIP_soln['Route Miles'].iloc[index])
            truck_loads = float(Pk_WIP_soln['Truck Loads'].iloc[index])
            sku_group = Pk_WIP_soln['SKU Group'].iloc[index]
            parent_sku_group = str(Pk_WIP_soln['Parent SKU Grp'].iloc[index])
            child_sku_group = str(Pk_WIP_soln['Child SKU Grp'].iloc[index])
            index += 1
            db_cur.execute(sql, (
            scenarioId, pack_site, wip_warehouse, sku, period, cases, dist_cost, wh_handling, route_miles, truck_loads,
            sku_group, parent_sku_group, child_sku_group))
        db_conn.commit()
        # endregion

        # region Tag Reno - DB Table: WIP_rPk
        WIP_rPk_soln.to_excel(writer, sheet_name='WIP_rPk', index=True, merge_cells=False)
        sheetDim['WIP_rPk'] = (len(WIP_rPk_soln) + 1, WIP_rPk_soln.index.nlevels + len(WIP_rPk_soln.columns) - 1)
        sql = 'INSERT INTO public."WIP_rPK"(scenario_id, repack_site, wip_warehouse, sku, period, cases, dist_cost, wh_handling, route_miles, truck_loads, sku_group, parent_sku_group, child_sku_group) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        index = 0
        for row in WIP_rPk_soln.index:
            wip_warehouse = row[1]
            repack_site = row[2]
            sku = row[3]
            period = row[4]
            cases = float(WIP_rPk_soln['Cases'].iloc[index])
            dist_cost = float(WIP_rPk_soln['Dist. Cost ($/Cs)'].iloc[index])
            # Angelo - Key Error. This wh_handling value will be equal to Pk_WIP.wh_handling value
            # wh_handling = float(WIP_rPk_soln['WH Handling ($/Cs)'].iloc[index])
            route_miles = float(WIP_rPk_soln['Route Miles'].iloc[index])
            truck_loads = float(WIP_rPk_soln['Truck Loads'].iloc[index])
            sku_group = WIP_rPk_soln['SKU Group'].iloc[index]
            parent_sku_group = str(WIP_rPk_soln['Parent SKU Grp'].iloc[index])
            child_sku_group = str(WIP_rPk_soln['Child SKU Grp'].iloc[index])
            index += 1
            db_cur.execute(sql, (
            scenarioId, repack_site, wip_warehouse, sku, period, cases, dist_cost, wh_handling, route_miles,
            truck_loads, sku_group, parent_sku_group, child_sku_group))
        db_conn.commit()
        # endregion

        # region Tag Reno - DB Table: rPk_FG
        rPk_FG_soln.to_excel(writer, sheet_name='rPk_FG', index=True, merge_cells=False)
        sheetDim['rPk_FG'] = (len(rPk_FG_soln) + 1, rPk_FG_soln.index.nlevels + len(rPk_FG_soln.columns) - 1)
        sql = 'INSERT INTO public."rPk_FG"(scenario_id, repack_site, fg_warehouse, sku, period, cases, dist_cost, wh_handling, route_miles, truck_loads, sku_group, parent_sku_group, child_sku_group) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        index = 0
        for row in rPk_FG_soln.index:
            repack_site = row[1]
            fg_warehouse = row[2]
            sku = row[3]
            period = row[4]
            cases = float(rPk_FG_soln['Cases'].iloc[index])
            dist_cost = float(rPk_FG_soln['Dist. Cost ($/Cs)'].iloc[index])
            wh_handling = float(rPk_FG_soln['WH Handling ($/Cs)'].iloc[index])
            route_miles = float(rPk_FG_soln['Route Miles'].iloc[index])
            truck_loads = float(rPk_FG_soln['Truck Loads'].iloc[index])
            sku_group = rPk_FG_soln['SKU Group'].iloc[index]
            parent_sku_group = str(rPk_FG_soln['Parent SKU Grp'].iloc[index])
            child_sku_group = str(rPk_FG_soln['Child SKU Grp'].iloc[index])
            index += 1
            db_cur.execute(sql, (
            scenarioId, repack_site, fg_warehouse, sku, period, cases, dist_cost, wh_handling, route_miles, truck_loads,
            sku_group, parent_sku_group, child_sku_group))
        db_conn.commit()
        # endregion

        # region Tag Reno - DB Table: FG_Cm
        FG_Cm_soln.to_excel(writer, sheet_name='FG_Cm', index=True, merge_cells=False)
        sheetDim['FG_Cm'] = (len(FG_Cm_soln) + 1, FG_Cm_soln.index.nlevels + len(FG_Cm_soln.columns) - 1)
        sql = 'INSERT INTO public."FG_Cm"(scenario_id, fg_warehouse, distributor, sku, cases, dist_cost,  route_miles, truck_loads, sku_group, parent_sku_group, child_sku_group, period) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        index = 0
        for row in FG_Cm_soln.index:
            distributor = row[2]
            fg_warehouse = row[1]
            sku = row[3]
            period = row[4]
            cases = float(FG_Cm_soln['Cases'].iloc[index])
            dist_cost = float(FG_Cm_soln['Dist. Cost ($/Cs)'].iloc[index])
            route_miles = float(FG_Cm_soln['Route Miles'].iloc[index])
            truck_loads = float(FG_Cm_soln['Truck Loads'].iloc[index])
            sku_group = FG_Cm_soln['SKU Group'].iloc[index]
            parent_sku_group = str(FG_Cm_soln['Parent SKU Grp'].iloc[index])
            child_sku_group = str(FG_Cm_soln['Child SKU Grp'].iloc[index])
            index += 1
            db_cur.execute(sql, (
            scenarioId, fg_warehouse, distributor, sku, cases, dist_cost, route_miles, truck_loads, sku_group,
            parent_sku_group, child_sku_group, period))
        db_conn.commit()
        # endregion

        # region Tag Reno - DB Table: PdStQ
        PdStQ_soln.to_excel(writer, sheet_name='PdStQ', index=True, merge_cells=False)
        sheetDim['PdStQ'] = (len(PdStQ_soln) + 1, PdStQ_soln.index.nlevels + len(PdStQ_soln.columns) - 1)
        sql = 'INSERT INTO public."PdStQ"(scenario_id, prod_site, stream, sku, litres, var_cost, eff_capacity, rqd_stream_hours, site_group, sku_group, parent_sku_group, child_sku_group, period) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        index = 0
        for row in PdStQ_soln.index:
            prod_site = row[1]
            stream = row[2]
            sku = row[3]
            period = row[4]
            litres = float(PdStQ_soln['Liters'].iloc[index])
            var_cost = float(PdStQ_soln['Var Cost ($/Lt)'].iloc[index])
            eff_capacity = float(PdStQ_soln['Eff. Capacity (Lt/Hrs)'].iloc[index])
            rqd_stream_hours = float(PdStQ_soln['Rqd. Stream Hrs'].iloc[index])
            site_group = PdStQ_soln['Site Group'].iloc[index]
            sku_group = PdStQ_soln['SKU Group'].iloc[index]
            parent_sku_group = str(PdStQ_soln['Parent SKU Grp'].iloc[index])
            child_sku_group = str(PdStQ_soln['Child SKU Grp'].iloc[index])
            index += 1
            db_cur.execute(sql, (
            scenarioId, prod_site, stream, sku, litres, var_cost, eff_capacity, rqd_stream_hours, site_group, sku_group,
            parent_sku_group, child_sku_group, period))
        db_conn.commit()
        # endregion

        # region Tag Reno - DB Table: PkLnQ
        PkLnQ_soln.to_excel(writer, sheet_name='PkLnQ', index=True, merge_cells=False)
        sheetDim['PkLnQ'] = (len(PkLnQ_soln) + 1, PkLnQ_soln.index.nlevels + len(PkLnQ_soln.columns) - 1)
        sql = 'INSERT INTO public."PkLnQ"(scenario_id, pack_site, line, sku, cases, var_cost, eff_capacity, rqd_lines_hours, site_group, sku_group, parent_sku_group, child_sku_group, period)VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        index = 0
        for row in PkLnQ_soln.index:
            pack_site = row[1]
            line = row[2]
            sku = row[3]
            period = row[4]
            cases = float(PkLnQ_soln['Cases'].iloc[index])
            var_cost = float(PkLnQ_soln['Var Cost ($/Cs)'].iloc[index])
            eff_capacity = float(PkLnQ_soln['Eff. Capacity (Cs/Hrs)'].iloc[index])
            rqd_lines_hours = float(PkLnQ_soln['Rqd. Line Hours'].iloc[index])
            site_group = PkLnQ_soln['Site Group'].iloc[index]
            sku_group = PkLnQ_soln['SKU Group'].iloc[index]
            parent_sku_group = str(PkLnQ_soln['Parent SKU Grp'].iloc[index])
            child_sku_group = str(PkLnQ_soln['Child SKU Grp'].iloc[index])
            index += 1
            db_cur.execute(sql, (
            scenarioId, pack_site, line, sku, cases, var_cost, eff_capacity, rqd_lines_hours, site_group, sku_group,
            parent_sku_group, child_sku_group, period))
        db_conn.commit()
        # endregion

        # region Tag Reno - DB Table: rPkLnQ
        rPkLnQ_soln.to_excel(writer, sheet_name='rPkLnQ', index=True, merge_cells=False)
        sheetDim['rPkLnQ'] = (len(rPkLnQ_soln) + 1, rPkLnQ_soln.index.nlevels + len(rPkLnQ_soln.columns) - 1)
        sql = 'INSERT INTO public."rPkLnQ"(scenario_id, repack_site, line, sku, cases, var_cost, eff_capacity, rqd_line_hours, site_group, sku_group, parent_sku_group, child_sku_group, period)VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        index = 0
        for row in rPkLnQ_soln.index:
            repack_site = row[1]
            line = row[2]
            sku = row[3]
            period = row[4]
            cases = float(rPkLnQ_soln['Cases'].iloc[index])
            var_cost = float(rPkLnQ_soln['Var Cost ($/Cs)'].iloc[index])
            eff_capacity = float(rPkLnQ_soln['Eff. Capacity (Cs/Hrs)'].iloc[index])
            rqd_lines_hours = float(rPkLnQ_soln['Rqd. Line Hours'].iloc[index])
            site_group = rPkLnQ_soln['Site Group'].iloc[index]
            sku_group = rPkLnQ_soln['SKU Group'].iloc[index]
            parent_sku_group = rPkLnQ_soln['Parent SKU Grp'].iloc[index]
            child_sku_group = rPkLnQ_soln['Child SKU Grp'].iloc[index]
            index += 1
            db_cur.execute(sql, (
            scenarioId, repack_site, line, sku, cases, var_cost, eff_capacity, rqd_lines_hours, site_group, sku_group,
            parent_sku_group, child_sku_group, period))
        db_conn.commit()
        # endregion

        # region Tag Reno - DB Table: FGSQ
        FGSQ_soln.to_excel(writer, sheet_name='FGSQ', index=True, merge_cells=False)
        sheetDim['FGSQ'] = (len(FGSQ_soln) + 1, FGSQ_soln.index.nlevels + len(FGSQ_soln.columns) - 1)
        sql = 'INSERT INTO public."FGSQ"(scenario_id, fg_warehouse, sku, cases, var_cost, var_period_cost, sku_group, parent_sku_group, child_sku_group, period)VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        index = 0
        for row in FGSQ_soln.index:
            fg_warehouse = row[1]
            sku = row[2]
            period = row[3]
            cases = float(FGSQ_soln['Cases'].iloc[index])
            var_cost = float(FGSQ_soln['Var Cost ($/Cs/Hr)'].iloc[index])
            var_period_cost = float(FGSQ_soln['Var Period Cost($/Cs)'].iloc[index])
            sku_group = FGSQ_soln['SKU Group'].iloc[index]
            parent_sku_group = FGSQ_soln['Parent SKU Grp'].iloc[index]
            child_sku_group = FGSQ_soln['Child SKU Grp'].iloc[index]
            index += 1
            db_cur.execute(sql, (
            scenarioId, fg_warehouse, sku, cases, var_cost, var_period_cost, sku_group, parent_sku_group,
            child_sku_group, period))
        db_conn.commit()
        # endregion

        # region Tag Reno - DB Table: WIPSQ
        WIPSQ_soln.to_excel(writer, sheet_name='WIPSQ', index=True, merge_cells=False)
        sheetDim['WIPSQ'] = (len(WIPSQ_soln) + 1, WIPSQ_soln.index.nlevels + len(WIPSQ_soln.columns) - 1)
        sql = 'INSERT INTO public."WIPSQ"(scenario_id, sku, cases, var_cost, var_period_cost, sku_group, parent_sku_group, child_sku_group, period, wip_warehouse)VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        index = 0
        for row in WIPSQ_soln.index:
            wip_warehouse = row[1]
            sku = row[2]
            period = row[3]
            cases = float(WIPSQ_soln['Cases'].iloc[index])
            var_cost = float(WIPSQ_soln['Var Cost ($/Cs/Hr)'].iloc[index])
            var_period_cost = float(WIPSQ_soln['Var Period Cost($/Cs)'].iloc[index])
            sku_group = WIPSQ_soln['SKU Group'].iloc[index]
            parent_sku_group = WIPSQ_soln['Parent SKU Grp'].iloc[index]
            child_sku_group = WIPSQ_soln['Child SKU Grp'].iloc[index]
            index += 1
            db_cur.execute(sql, (
            scenarioId, sku, cases, var_cost, var_period_cost, sku_group, parent_sku_group, child_sku_group, period,
            wip_warehouse))
        db_conn.commit()
        # endregion

        if Pk_Cm_Route == "Yes":
            Pk_Cm_soln.to_excel(writer, sheet_name='Pk_Cm', index=True, merge_cells=False)
            sheetDim['Pk_Cm'] = (len(Pk_Cm_soln) + 1, Pk_Cm_soln.index.nlevels + len(Pk_Cm_soln.columns) - 1)


        UnitMaster_tb.to_excel(writer, sheet_name='UnitMaster', index=False, merge_cells=False)
        sheetDim['UnitMaster'] = (len(UnitMaster_tb) + 1, UnitMaster_tb.index.nlevels + len(UnitMaster_tb.columns) - 2)

        CmMaster_tb.to_excel(writer, sheet_name='CmMaster', index=False, merge_cells=False)
        sheetDim['CmMaster'] = (len(CmMaster_tb) + 1, CmMaster_tb.index.nlevels + len(CmMaster_tb.columns) - 2)

        ZIPMaster_tb.to_excel(writer, sheet_name='ZIPMaster', index=False, merge_cells=False)
        sheetDim['ZIPMaster'] = (len(ZIPMaster_tb) + 1, ZIPMaster_tb.index.nlevels + len(ZIPMaster_tb.columns) - 2)


        workbook = writer.book
        worksheet = writer.sheets['Model']
        formatSummary = workbook.add_format()
        formatSummary.set_align('left')
        worksheet.set_column(1, 1, 100, formatSummary)
        worksheet.set_column(0, 0, 25, formatSummary)


        worksheet = writer.sheets['Summary']

        (max_row, max_col) = Summary_soln.unstack(level=-1).shape
        formatSummary = workbook.add_format()
        formatSummary.set_align('left')
        worksheet.set_column(0, max_col + 6, 15, formatSummary)

        worksheet.autofilter(0, 0, max_row, max_col + 6)
        formatSummary2 = workbook.add_format({'num_format': 41})
        worksheet.set_column(5, max_col + 6, 15, formatSummary2)

        if Pk_Cm_Route == "Yes":
            excel_sheet_distn = ['PdS_Pd', 'PkS_Pk', 'rPkS_rPk', 'Pd_Pk', 'Pk_Cm', 'Pk_FG', 'Pk_WIP', 'WIP_rPk', 'rPk_FG',
                            'FG_Cm', 'UnitMaster', 'CmMaster', 'ZIPMaster']
        else:
            excel_sheet_distn = ['PdS_Pd', 'PkS_Pk', 'rPkS_rPk', 'Pd_Pk', 'Pk_FG', 'Pk_WIP', 'WIP_rPk', 'rPk_FG',
                            'FG_Cm', 'UnitMaster', 'CmMaster', 'ZIPMaster']
        excel_sheet_mnf = ['PdStQ', 'PkLnQ', 'rPkLnQ']
        excel_sheet_storage = ['FGSQ', 'WIPSQ']

        for sht in excel_sheet_distn:
            worksheet = writer.sheets[sht]
            format = workbook.add_format({'num_format': 41})
            worksheet.autofilter(0, 0, sheetDim[sht][0], sheetDim[sht][1])
            worksheet.set_column(0, sheetDim[sht][1], 15, format)

        for sht in excel_sheet_mnf:
            worksheet = writer.sheets[sht]
            format = workbook.add_format({'num_format': 41})
            worksheet.autofilter(0, 0, sheetDim[sht][0], sheetDim[sht][1])
            worksheet.set_column(0, sheetDim[sht][1], 15, format)

        for sht in excel_sheet_storage:
            worksheet = writer.sheets[sht]
            format = workbook.add_format({'num_format': 41})
            worksheet.autofilter(0, 0, sheetDim[sht][0], sheetDim[sht][1])
            worksheet.set_column(0, sheetDim[sht][1], 15, format)

    #Tag Reno: Add solver date time to DB
    dt = datetime.now(timezone.utc)
    # Tag Reno: Added to make sure that Gurobi gets some time to rest before next solver is added
    time.sleep(10)
    db_cur.execute('UPDATE public."Scenarios" SET scenario_status = %s, solved_date = %s WHERE id = %s', (4, dt, scenarioId))
    db_conn.commit()
#Tag Reno: Leave print
print('*** Solver Successful with Scenario_ID: ' + scenarioId + " ***")
