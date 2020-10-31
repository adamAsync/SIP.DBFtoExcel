# -*- coding: utf-8 -*-
"""
@author: Adam Reinhold Von Fisher - https://www.linkedin.com/in/adamrvfisher/
"""

#This is a .xslx concatenation routine for data stored in .dbf
#Pay special attention to directory locations

#Import modules
import pandas as pd
import os
import time as t
import re
from dbfread import DBF

#Variable Assignment
AllDBFs = []
FittedDBFs = []
AAIIData = pd.DataFrame()
Aggregate = pd.DataFrame

#Start timer
Timer1Start = t.time()

#Change working directory
os.chdir('H:\\Users\\Username\\Directory\\WorkingDirectory')
#Confirm working directory 
print('Working directory is ' + os.getcwd())
print('-' * 70)

#Change SIPro directory - this is the location of the SI Pro Database files
SIProDirectoryLocation = "Z:\\Program Files (x86)\\Stock Investor\\Professional"
#Confirm working directory 
print('SI Pro directory is ' + SIProDirectoryLocation)
print('-' * 70)

#Destination for .xlsx file output, also put ExistingDatabase.xlsx in this folder
DestinationDirectory = 'H:\\Users\\Username\\Directory\\DestinationDirectory'

#Create directory for data storage - separate from SIpro directory
if not os.path.exists(DestinationDirectory):
    os.makedirs(DestinationDirectory)

#Read in existing .xslx database
ExistingDatabase = pd.read_excel(DestinationDirectory + '\\ExistingDatabase.xlsx', index_col=0)  
print('Destination directory is ' + DestinationDirectory)
print('-' * 70)

#Directory with DBFs inside
DBFLocationI = os.listdir((SIProDirectoryLocation + '\\Datadict'))
#DBFs addresses only
DBFsI = [str(SIProDirectoryLocation + '\\Datadict\\' + dbf) for dbf in DBFLocationI if '.dbf' in dbf]

#Directory with DBFs inside
DBFLocationII = os.listdir((SIProDirectoryLocation + '\\Dbfs'))
#DBFs addresses only
DBFsII = [str(SIProDirectoryLocation + '\\Dbfs\\' + dbf) for dbf in DBFLocationII if '.dbf' in dbf]

#Directory with DBFs inside
DBFLocationIII = os.listdir((SIProDirectoryLocation + '\\Static'))
#DBFs addresses only
DBFsIII = [str(SIProDirectoryLocation + '\\Static\\' + dbf) for dbf in DBFLocationIII if '.dbf' in dbf]

#Directory with DBFs inside
DBFLocationIV = os.listdir((SIProDirectoryLocation + '\\User'))
#DBFs addresses only
DBFsIV = [str(SIProDirectoryLocation + '\\User\\' + dbf) for dbf in DBFLocationIV if '.dbf' in dbf]

#Append to master list
AllDBFs = DBFsI + DBFsII + DBFsIII + DBFsIV

#Table grab for dimension specification 
temp = pd.DataFrame(iter(DBF(SIProDirectoryLocation + '\\Static\\si_bsa.dbf')))

#Dataframes for all DBFs
for dbf in AllDBFs:
    try:
        tempVariableName = dbf[dbf.rfind('\\') + 1:-4] 
        globals()[tempVariableName] = pd.DataFrame(iter(DBF(dbf)))
        if globals()[tempVariableName].shape[0] == temp.shape[0]:
            FittedDBFs.append(str(tempVariableName))
        print('Finished ' + tempVariableName + ' database read.')
    except UnicodeDecodeError: 
        tempVariableName = dbf[dbf.rfind('\\') + 1:-4] 
        globals()[tempVariableName] = pd.DataFrame(iter(DBF(dbf, encoding = 'latin')))
        if globals()[tempVariableName].shape[0] == temp.shape[0]:
            FittedDBFs.append(str(tempVariableName))
        print('Finished ' + tempVariableName + ' database read.')
        
print('-' * 32)

#Concatenate fitted DBFs
for dbf in FittedDBFs:
    globals()[dbf] = globals()[dbf].set_index('COMPANY_ID')
    globals()[dbf] = globals()[dbf].sort_index(axis = 0)
    AAIIData = pd.concat([AAIIData, globals()[dbf]], axis = 1)
    print('Adding ' + dbf + ' to dataframe.')
    
print('-' * 64)    
    
#Remove duplicate columns
AAIIData = AAIIData.loc[:,~AAIIData.columns.duplicated()]

#Set Index
AAIIData = AAIIData.set_index('TICKER')

#Order columns and index
AAIIData = AAIIData.sort_index(axis = 1)
AAIIData = AAIIData.sort_index(axis = 0)

#Drop SIPRO Columns
AAIIData = AAIIData.drop(['_NullFlags'], axis = 1)

#End timer
Timer1End = t.time()
#Timer stats for dbf read + formatting
print('Took ' + str(Timer1End - Timer1Start) + ' seconds for database read and formatting.')
print('-' * 64)
#Start timer
Timer2Start = t.time()

#Expand column abbreviations - create list object to modify
AAIIDataColumnList = list(AAIIData.columns)
#Capture column names to be replaced
AAIIDataOldColumnList = list(AAIIData.columns)

#==============================================================================
# #Modify column names
#==============================================================================
for column in AAIIDataColumnList: 
    if re.search('^ADJUST+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'AdjustmentsToIncome' + column[6:]
        continue
    if re.search('^ADR$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'AmericanDepositoryReceipt'
        continue    
    if re.search('^ANALYST_FN$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'AnalystFootnotes'
        continue
    if re.search('^AP_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'AccountsPayable' + column[2:]    
        continue
    if re.search('^ARTURN_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'ReceivablesTurnover' + column[6:]
        continue
    if re.search('^AR_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'AccountsReceivable' + column[2:] 
        continue
    if re.search('^ASSETS_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'TotalAssets' + column[6:]  
        continue
    if re.search('^AVD_10D$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'AverageDailyVolume10DayLookback' 
        continue
    if re.search('^AVM_03M$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'AverageMonthlyVolume3MonthLookback'    
        continue
    if re.search('^BETA', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'Beta'     
        continue
    if re.search('^BPG_EPS', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'BuffetPriceGrowthEarningsPerShareGrowth'
        continue
    if re.search('^BPG_SUS', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'BuffetPriceGrowthSustainableGrowth'
        continue
    if re.search('^BUSINESS', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'Business'
        continue
    if re.search('^BVPS_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'BookValuePerShare' + column[4:] 
        continue
    if re.search('^CASH_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'Cash' + column[4:]  
        continue
    if re.search('^CA_..$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'CurrentAssets' + column[2:]  
        continue        
    if re.search('^CE_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'CapitalExpenditures' + column[2:]  
        continue
    if re.search('^CFPS_12M', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'CashFlowPerShare_12M' 
        continue
    if re.search('^CFPS_G1T', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'CashFlowPerShareGrowth_12M' 
        continue
    if re.search('^CFPS_G.F', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'CashFlowPerShareGrowth_Y' + column[6:7] 
        continue
    if re.search('^CFPS_Q+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'CashFlowPerShare' + column[4:] 
        continue
    if re.search('^CFPS_Y+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'CashFlowPerShare' + column[4:] 
        continue
    if re.search('^CFPS_VAL', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'CashFlowPerShareValuation'
        continue
    if re.search('^CGS_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'CostOfGoodsSold' + column[3:]        
        continue
    if re.search('^CITY', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'City'        
        continue
    if re.search('^CL_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'CurrentLiabilities' + column[2:]          
        continue
    if re.search('^COMPANY', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'Company' 
        continue
    if re.search('^COUNTRY', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'Country'  
        continue
    if re.search('^CPS_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'CashPerShare' + column[3:]    
        continue
    if re.search('^CURR_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'CurrentRatio' + column[4:]            
        continue
    if re.search('^DATE_EQ0', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'DateQuarterEnds'
        continue
    if re.search('^DATE_EY0', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'DateFiscalYearEnds'       
        continue
    if re.search('^DCFBS_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'DualCashFlowBalanceSheet' + column[5:]   
        continue
    if re.search('^DCFO_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'DualOperatingCashFlow' + column[4:]   
        continue
    if re.search('^DCF_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'DualCashFlow' + column[3:]           
        continue
    if re.search('^DEP_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'Depreciation' + column[3:]         
        continue
    if re.search('^DIV_Y7Y1', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'NumberOfDividendIncreasesIn7YLookback'         
        continue
    if re.search('^DOW', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'IsInDowJones' 
        continue
    if re.search('^DPS_12M', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'DividendsPerShare_12M'     
        continue
    if re.search('^DPS_G1T', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'DividendsPerShareGrowth_12M'
        continue
    if re.search('^DPS_G.F', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'DividendsPerShareGrowth_Y' + column[5:6] 
        continue
    if re.search('^DPS_IND', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'DividendsPerShareIndicated'      
        continue
    if re.search('^DPS_Q+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'DividendsPerShare' + column[3:]
        continue
    if re.search('^DPS_VAL', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'DividendsPerShareValuation'
        continue
    if re.search('^DPS_Y+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'DividendsPerShare' + column[3:]   
        continue
    if re.search('^DRP_AVAIL', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'DividendReinvestmentPlanAvailable'   
        continue
    if re.search('^EMPLOYEES', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'NumberOfEmployees'    
        continue
    if re.search('^ENTVAL_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EnterpriseValue' + column[6:]   
        continue
    if re.search('^EPS3M_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EarningsPerShare3Month' + column[5:]    
        continue
    if re.search('^EPSCON_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EarningsPerShareContinuingOperations' + column[6:] 
        continue
    if re.search('^EPSC_G1T', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EarningsPerShareContinuingOperationsGrowth_12M'
        continue
    if re.search('^EPSC_G.F', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EarningsPerShareContinuingOperationsGrowth_Y' + column[6:7]
        continue
    if re.search('^EPSC_G+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EarningsPerShareContinuingOperationsGrowth' + column[6:]      
        continue
    if re.search('^EPSDC_G1T', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EarningsPerShareDilutedContinuingOperationsGrowth_12M' 
        continue
    if re.search('^EPSDC_G.F', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EarningsPerShareDilutedContinuingOperationsGrowth_Y' + column[7:8]
        continue
    if re.search('^EPSDC_G+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EarningsPerShareDilutedContinuingOperationsGrowth_' + column[7:]
        continue
    if re.search('^EPSDC_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EarningsPerShareDilutedContinuingOperations' + column[5:]
        continue
    if re.search('^EPSDMP_EQ0', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareMonthlyChangeAverageConsensusCurrentQuarter'
        continue
    if re.search('^EPSDMP_EQ1', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareMonthlyChangeAverageConsensusNextQuarter'
        continue
    if re.search('^EPSDMP_EY0', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareMonthlyChangeAverageConsensusCurrentYear'        
        continue
    if re.search('^EPSDMP_EY1', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareMonthlyChangeAverageConsensusNextYear'
        continue
    if re.search('^EPSDMP_EY2', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareMonthlyChangeAverageConsensusYearAfterNextYear'
        continue
    if re.search('^EPSDM_EG5', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareNumberOfDownwardRevisions1M_LongTermGrowth'    
        continue
    if re.search('^EPSDM_EQ0', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareNumberOfDownwardRevisions1M_CurrentQuarter'    
        continue
    if re.search('^EPSDM_EQ1', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareNumberOfDownwardRevisions1M_NextQuarter'            
        continue
    if re.search('^EPSDM_EY0', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareNumberOfDownwardRevisions1M_CurrentYear'            
        continue
    if re.search('^EPSDM_EY1', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareNumberOfDownwardRevisions1M_NextYear'            
        continue
    if re.search('^EPSDM_EY2', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareNumberOfDownwardRevisions1M_YearAfterNextYear' 
        continue
    if re.search('^EPSD_G1T', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EarningsPerShareDilutedTotalOperationsGrowth_12M' 
        continue
    if re.search('^EPSD_G.F', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EarningsPerShareDilutedTotalOperationsGrowth_Y' + column[6:7]
        continue
    if re.search('^EPSD_G+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EarningsPerShareDilutedTotalOperationsGrowth_' + column[6:] 
        continue
    if re.search('^EPSD_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EarningsPerShareDilutedTotalOperations' + column[4:]  
        continue
    if re.search('^EPSH_EG5', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareHighestLongTermGrowth'   
        continue
    if re.search('^EPSH_EQ0', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareHighestCurrentQuarter' 
        continue
    if re.search('^EPSH_EQ1', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareHighestNextQuarter' 
        continue
    if re.search('^EPSH_EY0', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareHighestCurrentYear' 
        continue
    if re.search('^EPSH_EY1', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareHighestNextYear' 
        continue
    if re.search('^EPSH_EY2', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareHighestYearAfterNextYear' 
        continue
    if re.search('^EPSL_EG5', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareLowestLongTermGrowth'   
        continue
    if re.search('^EPSL_EQ0', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareLowestCurrentQuarter' 
        continue
    if re.search('^EPSL_EQ1', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareLowestNextQuarter' 
        continue
    if re.search('^EPSL_EY0', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareLowestCurrentYear' 
        continue
    if re.search('^EPSL_EY1', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareLowestNextYear' 
        continue
    if re.search('^EPSL_EY2', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareLowestYearAfterNextYear'         
        continue
    if re.search('^EPSND_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EarningsPerShare' + column[3:]  
        continue
    if re.search('^EPSN_EG5', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareNumberOfAnalystsLongTermGrowth'   
        continue
    if re.search('^EPSN_EQ0', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareNumberOfAnalystsCurrentQuarter' 
        continue
    if re.search('^EPSN_EQ1', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareNumberOfAnalystsNextQuarter' 
        continue
    if re.search('^EPSN_EY0', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareNumberOfAnalystsCurrentYear' 
        continue
    if re.search('^EPSN_EY1', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareNumberOfAnalystsNextYear' 
        continue
    if re.search('^EPSN_EY2', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareNumberOfAnalystsYearAfterNextYear' 
        continue
    if re.search('^EPSPM_EG5', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerSharePreviousMonthLongTermGrowth'   
        continue
    if re.search('^EPSPM_EQ0', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerSharePreviousMonthCurrentQuarter' 
        continue
    if re.search('^EPSPM_EQ1', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerSharePreviousMonthNextQuarter' 
        continue
    if re.search('^EPSPM_EY0', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerSharePreviousMonthCurrentYear' 
        continue
    if re.search('^EPSPM_EY1', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerSharePreviousMonthNextYear' 
        continue
    if re.search('^EPSPM_EY2', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerSharePreviousMonthYearAfterNextYear'
        continue
    if re.search('^EPSSD_EG5', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareStandardDeviationLongTermGrowth'   
        continue
    if re.search('^EPSSD_EQ0', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareStandardDeviationCurrentQuarter' 
        continue
    if re.search('^EPSSD_EQ1', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareStandardDeviationNextQuarter' 
        continue
    if re.search('^EPSSD_EY0', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareStandardDeviationCurrentYear' 
        continue
    if re.search('^EPSSD_EY1', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareStandardDeviationNextYear' 
        continue
    if re.search('^EPSSD_EY2', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareStandardDeviationYearAfterNextYear'
        continue
    if re.search('^EPSUM_EG5', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareNumberOfUpwardRevisions1M_LongTermGrowth'   
        continue
    if re.search('^EPSUM_EQ0', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareNumberOfUpwardRevisions1M_CurrentQuarter' 
        continue
    if re.search('^EPSUM_EQ1', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareNumberOfUpwardRevisions1M_NextQuarter' 
        continue
    if re.search('^EPSUM_EY0', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareNumberOfUpwardRevisions1M_CurrentYear' 
        continue
    if re.search('^EPSUM_EY1', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareNumberOfUpwardRevisions1M_NextYear' 
        continue
    if re.search('^EPSUM_EY2', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareNumberOfUpwardRevisions1M_YearAfterNextYear'          
        continue
    if re.search('^EPS_EG5', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareLongTermGrowth' 
        continue
    if re.search('^EPS_EVAL', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShareValuation'  
        continue
    if re.search('^EPS_E+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EstimatedEarningsPerShare' + column[5:]        
        continue
    if re.search('^EPS_G1T', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EarningsPerShareGrowth_12M'     
        continue
    if re.search('^EPS_G.F', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EarningsPerShareGrowth_Y' + column[5:6] 
        continue
    if re.search('^EPS_G...$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EarningsPerShareGrowth_Q' + column[5:] 
        continue        
    if re.search('^EPS_G+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EarningsPerShareGrowth_' + column[5:]             
        continue
    if re.search('^EPS_VAL', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EarningsPerShareValuation' 
        continue
    if re.search('^EPS_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EarningsPerShare' + column[3:]     
        continue
    if re.search('^EQUITY+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'Equity' + column[6:]     
        continue
    if re.search('^ERBV', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EarningsRetainedToBookValue'
        continue
    if re.search('^ERE_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'ExchangeRateEffect' + column[3:]        
        continue
    if re.search('^EXCHANGE', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'StockExchange'
        continue
    if re.search('^EYIELD_12M', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'EarningsYield_12M' 
        continue
    if re.search('^FCFPS_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'FreeCashFlowPerShare' + column[5:]          
        continue
    if re.search('^FCFPS_G1T', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'FreeCashFlowPerShareGrowth_12M'
        continue
    if re.search('^FCFPS_G.F', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'FreeCashFlowPerShareGrowth_Y' + column[7:8]      
        continue
    if re.search('^FCFPS_VAL', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'FreeCashFlowPerShareValuation'
        continue
    if re.search('^FCFPS_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'FreeCashFlowPerShare' + column[5:]  
        continue
    if re.search('^FLOAT', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'Floatation'        
        continue
    if re.search('^GOPINC_G.F', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'GrossOperatingIncomeGrowth_Y' + column[8:9]    
        continue
    if re.search('^GOPINC_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'GrossOperatingIncome' + column[6:]  
        continue
    if re.search('^GOPIN_G1T', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'GrossOperatingIncomeGrowth_12M'
        continue
    if re.search('^GOPIN_G+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'GrossOperatingIncomeGrowth_Q' + column[7:]
        continue
    if re.search('^GPM_A5Y', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'GrossProfitMarginAverage5Y'  
        continue
    if re.search('^GPM_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'GrossProfitMargin' + column[3:]          
        continue
    if re.search('^GROSS_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'GrossIncome' + column[5:] 
        continue
    if re.search('^GWI_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'GoodwillAndIntangibles' + column[3:]         
        continue
    if re.search('^IAC_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'IncomeForPrimaryEPS' + column[3:]         
        continue
    if re.search('^INCTAX_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'IncomeTax' + column[6:] 
        continue
    if re.search('^IND_2_DIG', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'Industry2DigitCode'
        continue
    if re.search('^IND_3_DIG', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'Industry3DigitCode'
        continue
    if re.search('^INSDNS', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'InsidersNetSharesPurchased'  
        continue
    if re.search('^INSDPS', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'InsidersSharesPurchased' 
        continue
    if re.search('^INSDPT', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'InsidersBuyTrades'
        continue
    if re.search('^INSDSS', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'InsidersSharesSold' 
        continue
    if re.search('^INSDST', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'InsidersSellTrades'  
        continue
    if re.search('^INSTPS', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'InstitutionsSharesPurchased'
        continue
    if re.search('^INSTSS', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'InstitutionsSharesSold'  
        continue
    if re.search('^INS_PR_SHR', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'InsidersSharesPurchasedToSharesOutstanding' 
        continue
    if re.search('^INTNO_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'InterestExpenseNonOperating' + column[5:] 
        continue
    if re.search('^INT_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'InterestExpense' + column[3:] 
        continue
    if re.search('^INVTRN_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'InventoryTurnover' + column[6:] 
        continue
    if re.search('^INV_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'Inventory' + column[3:] 
        continue
    if re.search('^IW_EE', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'Inve$tWareEarningsPerShareEstimate4YearsForward'         
        continue
    if re.search('^IW_EPH', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'Inve$tWareEstimatedHighPrice5YearsForward'  
        continue
    if re.search('^IW_EPL', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'Inve$tWareEstimatedLowPrice5YearsForward'
        continue
    if re.search('^IW_EYIELD', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'Inve$tWareEstimatedDividendYield' 
        continue
    if re.search('^IW_PAR', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'Inve$tWarePARValuation' 
        continue
    if re.search('^IW_PBUY', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'Inve$tWareBuyPrice'
        continue
    if re.search('^IW_PLH', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'Inve$tWareLowToHighValuation'  
        continue
    if re.search('^IW_RV', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'Inve$tWareRelativeValue' 
        continue
    if re.search('^IW_SGB', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'Inve$tWareSalesGrowthBenchmark' 
        continue
    if re.search('^IW_TR', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'Inve$tWareTotalReturn' 
        continue
    if re.search('^LIAB_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'TotalLiabilities' + column[4:] 
        continue
    if re.search('^LTDEBT_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'LongTermDebt' + column[6:] 
        continue
    if re.search('^LTD_EQ_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'LongTermDebtToEquity' + column[6:] 
        continue
    if re.search('^LTD_TC_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'LongTermDebtToTotalCapital' + column[6:] 
        continue
    if re.search('^LTD_WC_Q1', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'LongTermDebtToWorkingCapital_Q1'
        continue
    if re.search('^LTINV_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'LongTermInvestments' + column[5:] 
        continue
    if re.search('^MKTCAP', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'MarketCapitalization' + column[6:] 
        continue
    if re.search('^NCC_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'NetCashFlow' + column[3:] 
        continue
    if re.search('^NCPS_Q1', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'NetCashPerShare'
        continue
    if re.search('^NETINC_G.F', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'NetIncomeGrowth_Y' + column[8:9] 
        continue
    if re.search('^NETINC_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'NetIncome' + column[6:] 
        continue
    if re.search('^NETIN_G1T', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'NetIncomeGrowth_12M'
        continue
    if re.search('^NETIN_G+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'NetIncomeGrowth_Q' + column[7:] 
        continue
    if re.search('^NIT_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'IncomeAfterTaxes' + column[3:] 
        continue
    if re.search('^NPM_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'NetProfitMargin' + column[3:] 
        continue
    if re.search('^NPPE_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'NetFixedAssets' + column[4:] 
        continue
    if re.search('^OCA_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'OtherCurrentAssets' + column[3:]
        continue
    if re.search('^OCL_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'OtherCurrentLiabilities' + column[3:] 
        continue
    if re.search('^OLTA_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'OtherLongTermAssets' + column[4:] 
        continue
    if re.search('^OLTL_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'OtherLongTermLiabilities' + column[4:] 
        continue
    if re.search('^OPINC_G+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'GrossOperatingIncomeGrowth_Y' + column[7:8] 
        continue
    if re.search('^OPIN_G1T', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'GrossIncomeGrowth_12M' 
        continue
    if re.search('^OPIN_G+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'GrossIncomeGrowth_Q' + column[6:] 
        continue
    if re.search('^OPM_A3Y', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'OperatingMarginAverage3Y'
        continue
    if re.search('^OPM_A5Y', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'OperatingMarginAverage5Y'
        continue
    if re.search('^OPM_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'OperatingMargin' + column[3:] 
        continue
    if re.search('^OPTIONABLE', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'Optionable' 
        continue
    if re.search('^OTHINC_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'OtherIncome' + column[6:] 
        continue
    if re.search('^PAYOUT_A7Y', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PayoutRatioAverage7Y'
        continue
    if re.search('^PAYOUT_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PayoutRatio' + column[6:] 
        continue
    if re.search('^PBVPS$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToBookValuePerShare' 
        continue
    if re.search('^PBVPSA_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToBookValuePerShareAverage' + column[6:] 
        continue
    if re.search('^PBVPS_1T', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToBookValuePerShare_12M'  
        continue
    if re.search('^PBVPS_A+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToBookValuePerShare' + column[5:] 
        continue
    if re.search('^PCFPS$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToCashFlowPerShare' 
        continue
    if re.search('^PCFPSA_VAL$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToBookValuePerShareValuation'
        continue
    if re.search('^PCFPSA_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToBookValuePerShareAverage' + column[6:] 
        continue
    if re.search('^PCFPS_1T', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToCashFlowPerShare_12M'
        continue
    if re.search('^PCFPS_A+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToCashFlowPerShareAverage' + column[7:] 
        continue
    if re.search('^PE$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToEarningsRatio'
        continue
    if re.search('^PEA_EVAL', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToEarningsRatioAverageEstimatedValuation' 
        continue
    if re.search('^PEA_VAL', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToEarningsRatioAverageValuation' 
        continue
    if re.search('^PEA_Y+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToEarningsRatioAverage' + column[3:] 
        continue
    if re.search('^PEH_A+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToEarningsRatioHighAverage' + column[5:] 
        continue
    if re.search('^PEH_Y+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToEarningsRatioHigh' + column[3:] 
        continue
    if re.search('^PEL_A+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToEarningsRatioLowAverage' + column[5:] 
        continue
    if re.search('^PEL_Y+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToEarningsRatioLow' + column[3:]         
        continue
    if re.search('^PERAPE', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToEarningsRelativeAdjusted'
        continue
    if re.search('^PERA_5Y', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToEarningsRelativeAverage5Y'
        continue
    if re.search('^PEREND_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PeriodEndDate' + column[6:] 
        continue
    if re.search('^PERH_A5Y', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToEarningsRelativeHighAverage5Y'
        continue
    if re.search('^PERLEN_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PeriodLength' + column[6:] 
        continue
    if re.search('^PERL_A5Y', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToEarningsRelativeLowAverage5Y'        
        continue
    if re.search('^PERV$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToEarningsRelativeValuation'
        continue
    if re.search('^PERVP$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToEarningsRelativeValuationToPrice'
        continue
    if re.search('^PE_1T$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToEarningsRatio_Y1'
        continue
    if re.search('^PE_A..$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToEarningsAverage' + column[4:] 
        continue
    if re.search('^PE_AE+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToEarningsAverageEarningsPerShare3Y'
        continue
    if re.search('^PE_E+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToEarningsRatioEstimateForward_' + column[4:] 
        continue
    if re.search('^PE_TO_DG5F', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToDividendAdjustedEarningsGrowth5Y'
        continue
    if re.search('^PE_TO_G5E', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToEstimatedEarningsGrowth5Y'      
        continue
    if re.search('^PE_TO_G5F', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToEarningsGrowth5Y'  
        continue
    if re.search('^PE_TO_YG5E', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToDividendAdjustedEarningsGrowth' 
        continue
    if re.search('^PFCPS$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToFreeCashFlowPerShare' 
        continue
    if re.search('^PFCPSA_VAL$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceAverageToFreeCashFlowPerShareValuation' 
        continue
    if re.search('^PFCPSA_..$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToFreeCashFlowPerShareAverage' + column[6:] 
        continue
    if re.search('^PFCPS_1T$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToFreeCashFlowPerShareAverage12M'
        continue
    if re.search('^PFCPS_A+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToFreeCashFlowPerShareAverage' + column[7:] 
        continue
    if re.search('^PGFPS$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToGrowthFlowPerShare' 
        continue
    if re.search('^PHONE$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PhoneNumber'
        continue
    if re.search('^PRCHG_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceChange' + column[5:] 
        continue
    if re.search('^PREF_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PreferredStock' + column[4:] 
        continue
    if re.search('^PRICE$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'Price' 
        continue
    if re.search('^PRICEDM+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'DateOfLastTradingDayOfMonth_' + column[7:] 
        continue
    if re.search('^PRICED_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'DateOfLastTradingDay' + column[6:] 
        continue
    if re.search('^PRICEHM+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceMonthlyHigh_' + column[6:] 
        continue
    if re.search('^PRICEH_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceHigh' + column[6:]        
        continue
    if re.search('^PRICELM+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceMonthlyLow_' + column[6:] 
        continue
    if re.search('^PRICEL_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceLow' + column[6:] 
        continue
    if re.search('^PRICEVM+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'VolumeMonthly_' + column[6:] 
        continue
    if re.search('^PRICEV_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'VolumeAnnual' + column[6:] 
        continue
    if re.search('^PRICE_DATE', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'DateOfPrice' 
        continue
    if re.search('^PRICE_M+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceMonthlyClose' + column[5:] 
        continue
    if re.search('^PRICE_Y+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceAnnualClose' + column[5:] 
        continue
    if re.search('^PRP_2YH$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PricePercentOfTwoYearHigh'
        continue
    if re.search('^PR_PRH_52W$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PricePercentOf52WeekHigh'
        continue
    if re.search('^PSPS$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToSalesPerShare' 
        continue
    if re.search('^PSPSA_VAL$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToSalesPerShareAverageValuation'
        continue
    if re.search('^PSPSA_Y+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToSalesPerShareAverage' + column[5:] 
        continue
    if re.search('^PSPS_1T$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToSalesPerShareAverage12M' 
        continue
    if re.search('^PSPS_A+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PriceToSalesPerShareAverage' + column[6:] 
        continue
    if re.search('^PTI_12M$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PreTaxIncome_12M' 
        continue
    if re.search('^PTI_G1T$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PreTaxIncomeGrowth_12M' 
        continue    
    if re.search('^PTI_G.F', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PreTaxIncomeGrowth_Y' + column[5:6] 
        continue
    if re.search('^PTI_G...$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PreTaxIncomeGrowth_Q' + column[:]
        continue
    if re.search('^PTI_..$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PreTaxIncome' + column[3:] 
        continue
    if re.search('^PTM_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PreTaxMargin' + column[3:]
        continue
    if re.search('^QS_DATE$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'QuarterlySurpriseDate' 
        continue
    if re.search('^QS_DIFF$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'QuarterlySurpriseDifference'  
        continue
    if re.search('^QS_EPS$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'QuarterlySurpriseEarningsPerShare' 
        continue
    if re.search('^QS_PERC$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'QuarterlySurpriseEarningsPerSharePercent' 
        continue
    if re.search('^QS_SD$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'QuarterlySurpriseStandardDeviation'  
        continue
    if re.search('^QS_SUE_Q1$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'QuarterlySurpriseStandardizedUnanticipatedEarnings' 
        continue
    if re.search('^QUICK_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'QuickRatio' + column[5:] 
        continue
    if re.search('^RDM_12M$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'ResearchAndDevelopmentPercentOfSales_12M' 
        continue
    if re.search('^RD_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'ResearchAndDevelopment' + column[2:] 
        continue
#==============================================================================
#     #Undefined columns, includes (maybe) relative metrics
#==============================================================================
    if re.search('^RARTURN_12$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeAccountsReceivableTurnover_12M'
        continue
    if re.search('^RARTURN_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeAccountsReceivableTurnover' + column[7:]
        continue
    if re.search('^RASSETS_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeTotalAssets' + column[7:]
        continue        
    if re.search('^RAVD_10D$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeAverageDailyVolume10DayLookback' 
        continue
    if re.search('^RAVM_03M$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeAverageMonthlyVolume3MonthLookback'
        continue
    if re.search('^RBETA$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeBeta'
        continue    
    if re.search('^RCFPS_G1T$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeCashFlowPerShareGrowth_12M'
        continue
    if re.search('^RCFPS_G.F$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeCashFlowPerShareGrowth_Y' + column[7:8]
        continue
    if re.search('^RCURR_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeCurrentRatio' + column[5:]
        continue
    if re.search('^RDPS_G1T$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeDividendPerShareGrowth_12M' 
        continue    
    if re.search('^RDPS_G.F$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeDividendPerShareGrowth_Y' + column[6:7] 
        continue        
    if re.search('^REPSC_G1T$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeEarningsPerShareContinuingOperationsGrowth_12M'
        continue
    if re.search('^REPSC_G.F', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeEarningsPerShareContinuingOperationsGrowth_Y' + column[7:8]
        continue
    if re.search('^REPSDC_G1T$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeEarningsPerShareDiscontinuedOperationsGrowth_12M'
        continue
    if re.search('^REPSDC_G.F', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeEarningsPerShareDiscontinuedOperationsGrowth_Y' + column[8:9] 
        continue
    if re.search('^REPS_EG5$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeEstimatedEarningsPerShareLongTermGrowth'
        continue
    if re.search('^REPS_G1T$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeEstimatedEarningsPerShare_12M'  
        continue
    if re.search('^REPS_G.F', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeEstimatedEarningsPerShare_Y' + column[6:7] 
        continue
    if re.search('^RETEARN_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RetainedEarnings' + column[7:] 
        continue
    if re.search('^RFCFPS_G1T$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeFreeCashFlowPerShareGrowth_12M'
        continue
    if re.search('^RFCFPS_G.F', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeFreeCashFlowPerShareGrowth_Y' + column[8:9] 
        continue
    if re.search('^RFLOAT$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeFloat'
        continue
    if re.search('^RGOPINC_G+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeGrossOperatingIncomeGrowth_Y' + column[9:] 
        continue
    if re.search('^RGPM_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeGrossProfitMargin' + column[4:] 
        continue
    if re.search('^RINVTRN_12$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeInventoryTurnover_12M' 
        continue
    if re.search('^RINVTRN_Y+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeInventoryTurnover' + column[7:] 
        continue
    if re.search('^RLTD_EQ_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeLongTermDebtToEquity' + column[7:] 
        continue
    if re.search('^RLTD_TC_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeLongTermDebtToTotalCapital' + column[7:] 
        continue
    if re.search('^RLTD_WC_Q1$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeLongTermDebtToWorkingCapital_Q1' 
        continue
    if re.search('^RMKTCAP$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeMarketCapitalization' 
        continue
    if re.search('^RNETINC_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeNetIncomeGrowth_Y' + column[9:] 
        continue
    if re.search('^RNPM_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeNetProfitMargin' + column[4:] 
        continue
    if re.search('^ROPM_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeOperatingMargin' + column[4:]        
        continue
    if re.search('^RPAYOUT_12$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePayoutRatio_12M'
        continue
    if re.search('^RPAYOUT_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePayoutRatio' + column[7:] 
        continue        
    if re.search('^RPBVPS$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePriceToBookValuePerShare' 
        continue
    if re.search('^RPBVPS_1T$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePriceToBookValuePerShare_12M' 
        continue
    if re.search('^RPBVPS_A..$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePriceToBookValuePerShareAverage' + column[8:] 
        continue
    if re.search('^RPCFPS$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePriceToCashFlowPerShare' 
        continue
    if re.search('^RPCFPS_1T$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePriceToCashFlowPerShare_12M' 
        continue
    if re.search('^RPCFPS_A..$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePriceToCashFlowPerShareAverage' + column[8:] 
        continue
    if re.search('^RPE$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePriceToEarningsRatio' + column[:] 
        continue
    if re.search('^RPEA_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePriceToEarningsAverage' + column[4:] 
        continue
    if re.search('^RPEH_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePriceToEarningsRatioHighAverage' + column[6:] 
        continue
    if re.search('^RPEL_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePriceToEarningsRatioLowAverage' + column[6:] 
        continue
    if re.search('^RPE_1T$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePriceToEarningsRatio_12M'
        continue
    if re.search('^RPE_A..$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePriceToEarningsRatioAverage' + column[5:] 
        continue
    if re.search('^RPE_AE+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePriceToEarningsAverageEarningsPerShare3Y' 
        continue
    if re.search('^RPE_E+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePriceToEarningsRatioEstimateForward_' + column[5:] 
        continue
    if re.search('^RPE_TO_DG5$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePriceToDividendAdjustedEarningsGrowth5Y' 
        continue
    if re.search('^RPE_TO_G5E$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePriceToEstimatedEarningsGrowth5Y' 
        continue
    if re.search('^RPE_TO_G5F$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePriceToEarningsGrowth5Y'
        continue
    if re.search('^RPFCPS$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePriceToFreeCashFlowPerShare' 
        continue
    if re.search('^RPFCPS_1T$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePriceToFreeCashFlowPerShare_12M' 
        continue
    if re.search('^RPFCPS_A..$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePriceToFreeCashFlowPerShareAverage' + column[8:]  
        continue
    if re.search('^RPRICE', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePrice' 
        continue
    if re.search('^RPRICEH_52$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePriceHigh' 
        continue
    if re.search('^RPRICEL_52$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePriceLow'
        continue
    if re.search('^RPR_PRH_52$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePricePercentOf52WeekHigh'
        continue
    if re.search('^RPSPS$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePriceToSalesPerShare' 
        continue
    if re.search('^RPSPS_1T$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePriceToSalesPerShare_12M' 
        continue
    if re.search('^RPSPS_A..$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePriceToSalesPerShareAverage' + column[7:] 
        continue
    if re.search('^RQUICK_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeQuickRatio' + column[6:] 
        continue
    if re.search('^RROA_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeReturnOnAssets' + column[4:] 
        continue
    if re.search('^RROE_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeReturnOnEquity' + column[4:] 
        continue        
    if re.search('^RRSW_4Q$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeRelativeStrengthWeighted_4Q'
        continue        
    if re.search('^RRS_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeRelativeStrength' + column[3:] 
        continue
    if re.search('^RSALES_12M$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeSales_12M' 
        continue    
    if re.search('^RSALES_G1T$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeSalesGrowth_12M' 
        continue
    if re.search('^RSALES_G.F$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeSalesGrowth_Y' + column[8:9] 
        continue
    if re.search('^RSALES_..$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeSales' + column[6:] 
        continue
    if re.search('^RSHRINSD$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePercentInsiderOwnership' 
        continue        
    if re.search('^RSHRINST$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativePercentInstitutionalOwnership' 
        continue
    if re.search('^RSHRINSTN$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeNumberInstitutionalShareholders'  
        continue
    if re.search('^RTA_TRN_12$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeTotalAssetTurnover_12M' 
        continue
    if re.search('^RTA_TRN_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeTotalAssetTurnover' + column[8:] 
        continue
    if re.search('^RTIE_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeTimesInterestEarned' + column[4:] 
        continue
    if re.search('^RTL_TA_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeTotalLiabilitiesToTotalAssets' + column[6:] 
        continue
    if re.search('^RYIELD$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeDividendYield'
        continue
    if re.search('^RYIELD_1T$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeDividendYield_12M'     
        continue
    if re.search('^RYIELD_A+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeDividendYieldAverage' + column[8:]            
        continue
    #Defined columns        
    if re.search('^ROA+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'ReturnOnAssets' + column[3:] 
        continue
    if re.search('^ROE_A5Y$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'ReturnOnEquityAverage5Y'
        continue
    if re.search('^ROE_A7Y$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'ReturnOnEquityAverage7Y' 
        continue
    if re.search('^ROE_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'ReturnOnEquity' + column[3:] 
        continue
    if re.search('^ROIC_A5Y$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'ReturnOnInvestedCapitalAverage5Y'  
        continue
    if re.search('^ROIC_..$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'ReturnOnInvestedCapital' + column[4:]   
        continue
    if re.search('^RSW_4Q$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeStrengthWeighted_4Q'
        continue
    if re.search('^RS_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RelativeStrength' + column[2:]        
        continue
    if re.search('^SALES_12M$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'Sales_12M'   
        continue
    if re.search('^SALES_G1T$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'SalesGrowth_12M'    
        continue
    if re.search('^SALES_G.F$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'SalesGrowth_Y' + column[7:8]    
        continue
    if re.search('^SALES_G.Q.$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'SalesGrowth_Q' + column[7:]    
        continue
    if re.search('^SALES_G.LS', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'SalesGrowthLeastSquares_Y' + column[7:8]    
        continue
    if re.search('^SALES_G7R2$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'SalesGrowthRSquared_Y7'   
        continue
    if re.search('^SALES_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'Sales' + column[5:]    
        continue
    if re.search('^SHRINSD$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'PercentInsiderOwnership'     
        continue
    if re.search('^SHRINST$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'InstitutionalOwnership'   
        continue
    if re.search('^SHRINSTN$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'NumberInstitutionalShareholders'    
        continue
    if re.search('^SHR_A+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'SharesOutstandingAverage' + column[5:]    
        continue
    if re.search('^SHR_D+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'Shares' + column[3:]    
        continue
    if re.search('^SHSPERADR$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'SharesPerADR' 
        continue
    if re.search('^SIC$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'StandardIndustrialClassification'
        continue
    if re.search('^SP$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'IsInS&P500'   
        continue
    if re.search('^SPLIT_DATE$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'StockSplitLastDate'    
        continue
    if re.search('^SPLIT_FACT$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'StockSplitFactor'    
        continue
    if re.search('^SPS_VAL$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'SalesPerShareValuation'
        continue
    if re.search('^STATE$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'State'
        continue
    if re.search('^STDEBT_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'ShortTermDebt' + column[6:]    
        continue
    if re.search('^STINV_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'ShortTermInvestments' + column[5:]    
        continue
    if re.search('^STREET$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'Street'    
        continue
    if re.search('^SUS_G7F$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'SustainableGrowthRate7Y'   
        continue
    if re.search('^TA_TRN_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'TotalAssetTurnover' + column[6:]    
        continue
    if re.search('^TCF_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'CashFlowsFinancing' + column[3:]    
        continue
    if re.search('^TCI_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'CashFlowsInvesting' + column[3:]
        continue
    if re.search('^TCO_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'CashFlowsOperations' + column[3:]    
        continue
    if re.search('^THUMB$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'RuleOfThumbValue' 
        continue
    if re.search('^TIE_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'TimesInterestEarned' + column[3:]    
        continue
    if re.search('^TL_TA_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'TotalLiabilitiesToTotalAssetsRatio' + column[5:]    
        continue
    if re.search('^TOTEXP_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'TotalOperatingExpenses' + column[6:]    
        continue
    if re.search('^TOTLOE_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'TotalLiabilitiesAndEquity' + column[6:]    
        continue
    if re.search('^UNINC_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'UnusualIncome' + column[5:]    
        continue
    if re.search('^WEB_ADDR$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'WebAddress'    
        continue
    if re.search('^XORD_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'NonrecurrringItems' + column[4:]    
        continue
    if re.search('^YIELD$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'DividendYield'    
        continue
    if re.search('^YIELDA_VAL$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'DividendYieldAverageValuation'   
        continue
    if re.search('^YIELDA_+.', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'DividendYieldAverage' + column[6:]    
        continue
    if re.search('^YIELDH_A7Y$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'DividendYieldHighAverage7Y'
        continue
    if re.search('^YIELDL_A7Y$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'DividendYieldLowAverage7Y'   
        continue
    if re.search('^YIELD_1T$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'DividendYield_12M'  
        continue
    if re.search('^YIELD_A.Y$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'DividendYieldAverage' + column[7:]    
        continue
    if re.search('^ZIP$', column):
        AAIIDataColumnList[AAIIDataColumnList.index(column)] = 'ZIPCode'                
        continue
    
#Column mapping   
ColumnMapping = pd.DataFrame(list(zip(AAIIDataOldColumnList, AAIIDataColumnList)), columns =['OldColumns', 'NewColumns'])     

#Output old column to new column 
print('Exporting columns.')
print('-' * 28)
ColumnMapping.to_excel(DestinationDirectory + '\\ColumnMapping.xlsx', index=False)
    
#Add SIPro_ tag to all column tags
Prefix = 'SIPro_'
AAIIDataColumnList = [Prefix + i for i in AAIIDataColumnList]
      
#Reassign column names
AAIIData.columns = AAIIDataColumnList 

#Drop columns
for column in AAIIDataColumnList:        
    if re.search('^SIPro_UDEF+.', column):
        AAIIData = AAIIData.drop([column], axis = 1)
        print('Dropped ' + column + ' column.')
print('-' * 55)    
#End timer
Timer2End = t.time()
#Timer stats
print('Took ' + str(Timer2End - Timer2Start) + ' seconds for column reformatting.')
print('-' * 55)
#Start timer
Timer3Start = t.time()

#Concatenate to database
OutputFile = pd.concat([AAIIData, ExistingDatabase], axis = 1)
#Reorder columns 
OutputFile = OutputFile.sort_index(axis = 1)

#Output new .xlsx database
print('Exporting entire database.')
print('-' * 70)
OutputFile.to_excel(DestinationDirectory + '\\OutputFile.xlsx')

#End timer
Timer3End = t.time()
#Timer stats
print('Took ' + str(Timer3End - Timer3Start) + ' seconds for concatenation, sort, and export.')
print('-' * 70)
print('Took ' + str(Timer3End - Timer1Start) + ' seconds for full process.')