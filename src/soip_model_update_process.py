"""
The purpose of this script is to replace the following set of "SOIP Model Update Process" Alteryx 
workflows:
    
    'SOIP Model Update Process - 000 - Main.yxwz',
    'SOIP Model Update Process - 010 - Depot Costs and Attributes v2.yxmc',
    'SOIP Model Update Process - 015 - Set SOIP Solve Flag.yxmd',
    'SOIP Model Update Process - 017 - Issue and Return Location Details.yxmd',
    'SOIP Model Update Process - 020 - Lane Attributes.yxmd',
    'SOIP Model Update Process - 030 - NPD Percentage Penalty.yxmc',
    'SOIP Model Update Process - 060 - Transportation Rates Historical.yxmc',
    'SOIP Model Update Process - 070 - Trans Load Size.yxmc',
    'SOIP Model Update Process - 090 - Flag Multi-Source Options.yxmc',
    'SOIP Model Update Process - 100 - Transfer Matrix Update.yxmc',
    'SOIP Model Update Process - 110 - Renter Dist Sort Pref Depot.yxmc'
    
This script is part of the monthly SOIP process. Specifically, this script downloads a set of 
data tables from a Cosmic Frog optimization model, updates values in those tables based on data in 
a multiple local Excel workbooks and data pulled from PECO's data warehouse, then reuploads the 
edited data tables back to the same Cosmic Frog optimization model.


"""

# Imports
import sqlalchemy as sal
import pandas as pd
import numpy as np
import warnings
import os
import sys
import csv
import time
from optilogic import pioneer
from io import StringIO

# Start time.
t0=time.time()

# Set display format for pandas.
pd.options.display.float_format = "{:,.2f}".format

# Add project root to PATH to allow for relative imports. 
ROOT = os.path.abspath(os.path.join('..'))
if ROOT not in sys.path:
    sys.path.append(ROOT)

# Import SQL Statements
from sql_statements import tbl_tab_Location_sql, nbr_of_depots_sql, \
    trans_load_size_sql, trans_load_counts_sql_raw, trans_costs_sql_raw
    
# Import User-Input data.
from user_inputs import USER_NAME, APP_KEY, INPUT_DB_NAME, OUTPUT_DB_NAME, RepairCapacityNotes, \
    MinInventoryNotes, DepotCapacityNotes, BeginningInvNotes, ReturnsProductionNotes, \
    ProductionPolicyRepairBOMName, NewPalletCost, Avg_Load_Size_Issues, Avg_Load_Size_Returns, \
    Avg_Load_Size_Transfers, Fuel_Surcharge, Duty_Rate_US_to_Canada, Duty_Rate_Canada_to_US 
    
# Import Excel IO function.
from excel_data_validation import pull_data_from_excel

# Define functions to pull data from Cosmic Frog and PECO's data warehouse.

def pull_data_from_cosmic_frog(USER_NAME, APP_KEY, INPUT_DB_NAME, tables_we_want):
    # Note: This syntax is compatible with SQLAlchemy 2.0.
    print('\nPulling data from Cosmic Frog...')
    
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")     # Ignore the Cosmic Frog API warning.
    
        # Code that makes connection to the Cosmic Frog database.
        api = pioneer.Api(auth_legacy = False, un=USER_NAME, appkey=APP_KEY)
        connection_str = api.sql_connection_info(INPUT_DB_NAME)
        #connection_string = 'postgresql://'+connection_str['raw']['user']+':'+ \
        #    connection_str['raw']['password']+'@'+connection_str['raw']['host']+':'+ \
        #    str(connection_str['raw']['port'])+'/'+connection_str['raw']['dbname']+'?sslmode=require'
        connection_string = connection_str['connectionStrings']['url']
        engine = sal.create_engine(connection_string)
        insp = sal.inspect(engine)
    
    # List of all Cosmic Frog Model tables
    db_tables = insp.get_table_names()
    
    # Create a dictionary to store the cosmic frog data frames.
    data_dict = {}
    
    for i in db_tables:
        with engine.connect() as conn:
            trans = conn.begin()
            if i in tables_we_want:
                print(f'\tReading table: {i}')
                data = pd.read_sql_query(sal.text(f"SELECT * FROM {i}"), con=conn)
                if 'id' in data.columns:
                    del data['id']
                data_dict[i] = data
            trans.commit()
    del data
    
    return data_dict

def pull_data_from_data_warehouse(sql_name_dict):
    # Note: This syntax is compatible with SQLAlchemy 2.0.
    print("\nPulling data from PECO's data warehouse...")
    connection_string = 'DRIVER={SQL Server};SERVER=10.0.17.62;;UID=tabconnection;PWD=password'
    connection_url = sal.engine.URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = sal.create_engine(connection_url)
    
    # Create a dictionary to store the cosmic frog data frames.
    data_dict = {}
    
    try:
        for name, sql_statement in sql_name_dict.items():
            with engine.connect() as conn:
                trans = conn.begin()
                print(f'\tExecuting SQL statement: {name}')
                data_dict[name] = pd.read_sql(sal.text(sql_statement), con=conn)
                trans.commit()
    
    except Exception as e:
        print('\n',e,'\n')
        print("\nEnsure you are connected to PECO's VPN.\n")
    
    return data_dict

def scac_sql_preprocessing(sql_statement, scac_types):
    cpu = scac_types.loc[scac_types['Carrier_Type']=='CPU', 'TMS_CarrierSCAC']
    ded = scac_types.loc[scac_types['Carrier_Type']=='Dedicated', 'TMS_CarrierSCAC']
    
    cpu_scacs = str(tuple(cpu))
    ded_scacs = str(tuple(ded))
    cpu_ded_scacs = str(tuple(pd.concat([cpu, ded])))
    
    sql_statement = sql_statement.replace('__CPU_SCACS__', cpu_scacs)
    sql_statement = sql_statement.replace('__DED_SCACS__', ded_scacs)
    sql_statement = sql_statement.replace('__CPU_DED_SCACS__', cpu_ded_scacs)
    
    return sql_statement


# Pull data from Excel.
excel_data, error_count = pull_data_from_excel()
# Exit the program. Ensure the errors in the Excel data are correct before moving on.
if error_count > 0:
    exit()

# Pull data from PECO's data warehouse.
# Update transportation SQL with SCAC to Carrier Type mapping.
print("\nAdding SCAC codes to transportation SQL statements...")
trans_load_counts_sql = scac_sql_preprocessing(trans_load_counts_sql_raw, excel_data['SCAC Types'])
trans_costs_sql = scac_sql_preprocessing(trans_costs_sql_raw, excel_data['SCAC Types'])
print("\tDone.")

sql_name_dict = {'tbl_tab_Location':tbl_tab_Location_sql,
                 'nbr_of_depots':nbr_of_depots_sql,
                 'transport_rates_hist_load_counts':trans_load_counts_sql,
                 'transport_rates_hist_costs':trans_costs_sql,
                 'transport_load_size':trans_load_size_sql}

data_warehouse_data = pull_data_from_data_warehouse(sql_name_dict)
    
# Pull data from Cosmic Frog.
tables_we_want  = ['customerdemand',
                   'customerfulfillmentpolicies',
                   'customers',
                   'facilities',
                   'groups',
                   'inventoryconstraints',
                   'inventorypolicies',
                   'periods',
                   'productionconstraints',
                   'productionpolicies',
                   'replenishmentpolicies',
                   'transportationpolicies',
                   'warehousingpolicies',
                   ]
cosmic_frog_data = pull_data_from_cosmic_frog(USER_NAME, APP_KEY, INPUT_DB_NAME, tables_we_want)
print('Done pulling data.\n')


# Cosmic Frog Data
customerdemand = cosmic_frog_data['customerdemand'].copy()
customerfulfillmentpolicies = cosmic_frog_data['customerfulfillmentpolicies'].copy()
customers = cosmic_frog_data['customers'].copy()
facilities = cosmic_frog_data['facilities'].copy()
groups = cosmic_frog_data['groups'].copy()
inventoryconstraints = cosmic_frog_data['inventoryconstraints'].copy()
inventorypolicies = cosmic_frog_data['inventorypolicies'].copy()
periods = cosmic_frog_data['periods'].copy()
productionconstraints = cosmic_frog_data['productionconstraints'].copy()
productionpolicies = cosmic_frog_data['productionpolicies'].copy()
replenishmentpolicies = cosmic_frog_data['replenishmentpolicies'].copy()
transportationpolicies = cosmic_frog_data['transportationpolicies'].copy()
warehousingpolicies = cosmic_frog_data['warehousingpolicies'].copy()

# =============================================================================
# # NOTE: We will change this during development and compare to the unedited dataframes. 
# #       Remove this section after development is complete.
# customerdemand_orig = cosmic_frog_data['customerdemand'].copy()
# customerfulfillmentpolicies_orig = cosmic_frog_data['customerfulfillmentpolicies'].copy()
# customers_orig = cosmic_frog_data['customers'].copy()
# facilities_orig = cosmic_frog_data['facilities'].copy()
# groups_orig = cosmic_frog_data['groups'].copy()
# inventoryconstraints_orig = cosmic_frog_data['inventoryconstraints'].copy()
# inventorypolicies_orig = cosmic_frog_data['inventorypolicies'].copy()
# periods_orig = cosmic_frog_data['periods'].copy()
# productionconstraints_orig = cosmic_frog_data['productionconstraints'].copy()
# productionpolicies_orig = cosmic_frog_data['productionpolicies'].copy()
# replenishmentpolicies_orig = cosmic_frog_data['replenishmentpolicies'].copy()
# transportationpolicies_orig = cosmic_frog_data['transportationpolicies'].copy()
# warehousingpolicies_orig = cosmic_frog_data['warehousingpolicies'].copy()
# =============================================================================

# Data Warehouse Data
tbl_tab_Location = data_warehouse_data['tbl_tab_Location'].copy()
nbr_of_depots = data_warehouse_data['nbr_of_depots'].copy()
transport_rates_hist_load_counts = data_warehouse_data['transport_rates_hist_load_counts'].copy()
transport_rates_hist_costs = data_warehouse_data['transport_rates_hist_costs'].copy()
transport_load_size = data_warehouse_data['transport_load_size'].copy()

# =============================================================================
# # NOTE: We will change this during development and compare to the unedited dataframes. 
# #       Remove this section after development is complete.
# tbl_tab_Location_orig = data_warehouse_data['tbl_tab_Location'].copy()
# nbr_of_depots_orig = data_warehouse_data['nbr_of_depots'].copy()
# transport_rates_hist_load_counts_orig = data_warehouse_data['transport_rates_hist_load_counts'].copy()
# transport_rates_hist_costs_orig = data_warehouse_data['transport_rates_hist_costs'].copy()
# transport_load_size_orig = data_warehouse_data['transport_load_size'].copy()
# =============================================================================


#%% Subprocesses.
print('Executing subprocesses prior to editing Cosmic Frog data...\n')
########################################## Number of Depots - (For 020-Lane Attributes)
print('\tNumber of depots...')

nod = nbr_of_depots.copy()
nod['ModelID'] = np.nan
nod.set_index(['movetype', 'customer'], inplace=True)

cus = customers[['customername', 'loccode']].copy()
cus.rename(columns={'customername':'ModelID', 'loccode':'customer'}, inplace=True)
cus['movetype'] = 'Issue'
cus.set_index(['movetype', 'customer'], inplace=True)

fac = facilities.loc[facilities['facilityname'].str.startswith('R_'),
                     ['facilityname', 'loccode']].copy()
fac.rename(columns={'facilityname':'ModelID', 'loccode':'customer'}, inplace=True)
fac['movetype'] = 'Return'
fac.set_index(['movetype', 'customer'], inplace=True)

nod.update(cus)
nod.update(fac)
nod.reset_index(inplace=True)
nod.dropna(inplace=True)
print('\tDone.\n')

########################################## Transport Load Size - (For 070-Trans Load Size)
print('\tTransport Load Size...')
iss = transport_load_size[transport_load_size['movetype']=='Issue'].copy()
oth = transport_load_size[transport_load_size['movetype']!='Issue'].copy()
iss = iss.merge(customers[['loccode', 'customername']], how='left', left_on='customer_Loc_Code', right_on='loccode')
oth = oth.merge(facilities[['loccode', 'facilityname']], how='left', left_on='customer_Loc_Code', right_on='loccode')
iss.rename(columns={'customername':'ModelID'}, inplace=True)
oth.rename(columns={'facilityname':'ModelID'}, inplace=True)

tls = pd.concat([iss, oth])
tls['originname'] = tls.apply(lambda row: row['ModelID'] if row['movetype'] == 'Return' else None, axis=1)
tls['destinationname'] = tls.apply(lambda row: row['ModelID'] if row['movetype'] == 'Issue' else None, axis=1)
print('\tDone.\n')

########################################## Multi-Source Options - (For 090-Flag Multi-Source Options)
print('\tMulti-Source Options...')
msl = excel_data['MultiSource List'].copy()
msl = msl.merge(customers[['loccode', 'customername']], how='left', left_on='CustomerCode', right_on='loccode')
msl = msl.merge(facilities[['loccode', 'facilityname']], how='left', left_on='CustomerCode', right_on='loccode', suffixes=('_issu', '_retu'))
msl = msl.merge(facilities[['loccode', 'facilityname']], how='left', left_on='DepotCode', right_on='loccode', suffixes=('_retu', '_depot'))

cond = msl['MoveType']=='Issue'
msl.loc[cond, 'OModelID']  = msl.loc[cond, 'facilityname_depot']
msl.loc[~cond, 'OModelID'] = msl.loc[~cond, 'facilityname_retu']
msl.loc[cond, 'DModelID']  = msl.loc[cond, 'customername']
msl.loc[~cond, 'DModelID'] = msl.loc[~cond, 'facilityname_depot']

msl = msl[msl['OK to Include SOIP'].isin(['YES', 'Yes', 'Y', 'y'])]
print('\tDone.\n')

#%% Update Depot Costs and Attributes (Alteryx workflow 010)
print('Executing : Update Depot Costs and Attributes (Alteryx workflow 010)...')

###################################################################### Customer Fulfillment Policies
cols = ['ModelID', 'Type', 'Paint Upd']
cfp = excel_data['Depot Assumptions'][cols].copy()
cfp.index = cfp['ModelID']

cfp['unitcost'] = cfp['Paint Upd'].fillna(0)
cfp['depottype'] = cfp['Type']

# Set existing values prior to updating.
cols = ['unitcost']
customerfulfillmentpolicies[cols] = 0
customerfulfillmentpolicies.set_index('sourcename')
customerfulfillmentpolicies.update(cfp)
customerfulfillmentpolicies.reset_index(inplace=True)


###################################################################### Facilities
cols = ['ModelID', 'Type', 'Closed', 'Heat Treat', 'Fixed Upd']
fac = excel_data['Depot Assumptions'][cols].copy()
fac.index = fac['ModelID']

fac['fixedoperatingcost'] = fac['Fixed Upd'].fillna(0)
fac['heat_treatment_rqmt'] = fac['Heat Treat'].fillna('N')
fac['depottype'] = fac['Type']
fac['closed'] = fac['Closed']

def label_status(row):
    if row['ModelID'][0] != 'D':
        return np.nan
    else:
        return 'Include' if row['Closed'] == "NO" else 'Exclude'
    
fac['status'] = fac.apply(lambda row: label_status(row), axis=1)

# Set existing values prior to updating.
cols = ['fixedstartupcost', 'fixedclosingcost', 'fixedoperatingcost']
facilities[cols] = 0
facilities.set_index('facilityname', inplace=True)
facilities.update(fac)
facilities.reset_index(inplace=True)



###################################################################### Inventory Constraints
cols = ['ModelID', 'Minimum Inv', 'Storage', 'Yard Space', 'Temp Storage']
ivc = excel_data['Depot Assumptions'][cols].copy()
ivc.fillna(0, inplace=True)

ic_min = inventoryconstraints.loc[inventoryconstraints['notes']==MinInventoryNotes,
                                  ['facilityname', 'notes']].copy()
ic_min = ic_min.merge(ivc, how='left', left_on='facilityname', right_on='ModelID')
ic_min['constraintvalue'] = ic_min['Minimum Inv']

ic_max = inventoryconstraints.loc[inventoryconstraints['notes']==DepotCapacityNotes,
                                  ['facilityname', 'notes']].copy()
ic_max = ic_max.merge(ivc, how='left', left_on='facilityname', right_on='ModelID')
ic_max['constraintvalue'] = ic_max['Storage'] + ic_max['Yard Space'] + ic_max['Temp Storage']

ic = pd.concat([ic_min, ic_max])

index_cols = ['facilityname', 'notes']
ic.set_index(index_cols, inplace=True)
ic = ic[~ic.index.duplicated()]
inventoryconstraints.set_index(index_cols, inplace=True)
inventoryconstraints.update(ic)
inventoryconstraints.reset_index(inplace=True)



###################################################################### Inventory Policies
cols = ['ModelID', 'DmgRate', 'BegInv_RFU', 'BegInv_WIP', 'BegInv_MIX']
ivp = excel_data['Depot Assumptions'][cols].copy()
nonneg_cols = ['DmgRate', 'BegInv_RFU', 'BegInv_WIP', 'BegInv_MIX']
ivp[nonneg_cols] = ivp[nonneg_cols].clip(0)

ip = inventorypolicies.loc[inventorypolicies['notes']==BeginningInvNotes,
                           ['facilityname','notes','productname']].copy()
ip = ip.merge(ivp, how='left', left_on='facilityname', right_on='ModelID')
ip.fillna(0, inplace=True)

def set_initial_inv(row):
    rfu_count = row['BegInv_RFU'] + (1-row['DmgRate'])*(row['BegInv_MIX'])
    mix_count = row['BegInv_WIP'] + (  row['DmgRate'])*(row['BegInv_MIX'])
    
    return rfu_count if row['productname'] == 'RFU' else mix_count

ip['initialinventory'] = ip.apply(set_initial_inv, axis=1)

index_cols = ['facilityname', 'notes', 'productname']
ip.set_index(index_cols, inplace=True)
ip = ip[~ip.index.duplicated()]
inventorypolicies.set_index(index_cols, inplace=True)
inventorypolicies.update(ip)
inventorypolicies.reset_index(inplace=True)




###################################################################### Production Constraints
cols = ['ModelID', 'Repair / Day']
pcs = excel_data['Depot Assumptions'][cols].copy()

pc = productionconstraints.loc[productionconstraints['notes']==RepairCapacityNotes,
                               ['facilityname', 'periodname', 'notes']].copy()
workdays = periods[['periodname', 'workingdays']].copy()
pc = pc.merge(workdays, how='left', on='periodname')

pc = pc.merge(pcs, how='left', left_on='facilityname', right_on='ModelID')
pc['Repair / Day'] = pc['Repair / Day'].astype('float').fillna(0)
pc['workingdays'] = pc['workingdays'].astype('float')
pc['constraintvalue'] = pc['workingdays']*pc['Repair / Day']

index_cols = ['facilityname', 'periodname', 'notes']
pc.set_index(index_cols, inplace=True)
pc = pc[~pc.index.duplicated()]
productionconstraints.set_index(index_cols, inplace=True)
productionconstraints.update(pc)
productionconstraints.reset_index(inplace=True)



###################################################################### Production Policies
cols = ['ModelID', 'Type', 'Repair Upd']
pps = excel_data['Depot Assumptions'][cols].copy()

pps['bomname'] = ProductionPolicyRepairBOMName
pps['unitcost'] = pps['Repair Upd'].fillna(0)
pps['facilityname'] = pps['ModelID']

index_cols = ['facilityname', 'bomname']
pps.set_index(index_cols, inplace=True)
pps = pps[~pps.index.duplicated()]
productionpolicies.set_index(index_cols, inplace=True)
productionpolicies.update(pps)
productionpolicies.reset_index(inplace=True)


###################################################################### Replenishment Policies
cols = ['ModelID', 'Type']
loc = excel_data['Depot Assumptions'][cols].copy()
loc.index = loc['ModelID']

loc['odepottype'] = loc['Type']
replenishmentpolicies.index = replenishmentpolicies['sourcename']
replenishmentpolicies.update(loc)

del(loc['odepottype'])
loc['ddepottype'] = loc['Type']
replenishmentpolicies.set_index('facilityname')
replenishmentpolicies.update(loc)


###################################################################### Warehousing Policies
cols = ['ModelID', 'Handling In Upd', 'Handling Out Upd', 'Sort Upd']
whp = excel_data['Depot Assumptions'][cols].copy()
whp.index = whp['ModelID']
whp['inboundhandlingcost'] = whp['Handling In Upd'].fillna(0) + whp['Sort Upd'].fillna(0)
whp['outboundhandlingcost'] = whp['Handling Out Upd']

# Set existing values prior to updating.
cols = ['inboundhandlingcost', 'outboundhandlingcost']
warehousingpolicies[cols] = 0
warehousingpolicies.set_index('facilityname')
warehousingpolicies.update(whp)
warehousingpolicies.reset_index(inplace=True)
print('\tDone.\n')


#%% Set SOIP Solve Flag (Alteryx workflow 015)
print('Executing : Set SOIP Solve Flag (Alteryx workflow 015)...')

###################################################################### Customer Fulfillment Policies
cols = ['Oname', 'RevisedDName']
cfp = excel_data['Depot Assignments'][cols].copy()
cfp['customername'] = cfp['Oname']
cfp['sourcename'] = cfp['RevisedDName']

cfp['soipplan'] = 'Y'
cfp['soip_depot_id'] = cfp['RevisedDName']
cfp['status'] = 'Include'
cfp['notes']  = 'Baseline_Issues'

# Set existing values prior to updating.
customerfulfillmentpolicies['soipplan'] = 'N'
customerfulfillmentpolicies['status'] = 'Exclude'
customerfulfillmentpolicies['notes'] = 'NewAllToAll_Issues'

index_cols = ['customername', 'sourcename']
cfp.set_index(index_cols, inplace=True)
cfp = cfp[~cfp.index.duplicated()]
customerfulfillmentpolicies.set_index(index_cols, inplace=True)
customerfulfillmentpolicies.update(cfp)
customerfulfillmentpolicies.reset_index(inplace=True)



###################################################################### Customers
cols = ['customername', 'quantity']
cd = customerdemand[cols].copy()
cd['quantity'] = cd['quantity'].astype('float')
cd = cd.groupby(by='customername').sum()
cd['status'] = cd.apply(lambda row: 'Include' if row['quantity'] > 0 else 'Exclude', axis=1)
cd['insoip'] = cd.apply(lambda row: 'Y' if row['quantity'] > 0 else 'N', axis=1)

# Set existing values prior to updating.
customers['status'] = 'Exclude'
customers['insoip'] = 'N'
customers.set_index('customername', inplace=True)
customers.update(cd)
customers.reset_index(inplace=True)



###################################################################### Facilities
cols = ['facilityname', 'constraintvalue']
pc = productionconstraints.loc[productionconstraints['notes']==ReturnsProductionNotes, cols].copy()
pc['constraintvalue'] = pc['constraintvalue'].astype('float')
pc = pc.groupby(by='facilityname').sum()

pc['status'] = pc.apply(lambda row: 'Include' if row['constraintvalue'] > 0 else 'Exclude', axis=1)
pc['insoipmodel'] = pc.apply(lambda row: 'Y' if row['constraintvalue'] > 0 else 'N', axis=1)

facilities.loc[facilities['facilityname'].str.startswith('R_'), 'status'] = 'Exclude'
facilities.loc[facilities['facilityname'].str.startswith('R_'), 'insoipmodel'] = 'N'
facilities.set_index('facilityname', inplace=True)
facilities.update(pc)
facilities.reset_index(inplace=True)



###################################################################### Replenishment Policies
cols = ['Oname', 'RevisedDName']
rps = excel_data['Depot Assignments'][cols].copy()
rps['sourcename'] = rps['Oname']
rps['facilityname'] = rps['RevisedDName']

rps['soipplan'] = 'Y'
rps['soip_depot_id'] = rps['RevisedDName']
rps['status'] = 'Include'
rps['notes']  = 'Baseline_Returns'

# Set existing values prior to updating.
replenishmentpolicies['soipplan'] = 'N'
replenishmentpolicies['status'] = replenishmentpolicies.apply(lambda row: 'Exclude' if row['sourcename'][0] == 'R' else row['status'], axis=1)
replenishmentpolicies['notes']  = replenishmentpolicies.apply(lambda row: 'NewAllToAll_Returns' if row['sourcename'][0] == 'R' else row['notes'], axis=1)

index_cols = ['sourcename', 'facilityname']
rps.set_index(index_cols, inplace=True)
rps = rps[~rps.index.duplicated()]
replenishmentpolicies.set_index(index_cols, inplace=True)
replenishmentpolicies.update(rps)
replenishmentpolicies.reset_index(inplace=True)
print('\tDone.\n')

#%% Issue and Return Location Details (Alteryx workflow 017)
print('Executing : Issue and Return Location Details (Alteryx workflow 017)...')

###################################################################### Customers
renters = tbl_tab_Location.loc[(tbl_tab_Location['RL Location Type'] == 'Renter') | (tbl_tab_Location['RL Location Type'].isnull()),
                               ['Code', 'Corporate Code', 'Corporate Name']]

cols = ['customername', 'loccode']
cs = customers[cols].copy()
cs = cs.merge(renters, how='left', left_on='loccode', right_on='Code')

cols = ['customername', 'quantity']
dm = customerdemand.loc[customerdemand['periodname'].str[-2:].astype(int) <= 12, cols].copy()
dm['quantity'] = dm['quantity'].astype(float)
dm = dm.groupby(by='customername').sum()

cs = cs.merge(dm, how='left', on='customername')
cs['quantity'].fillna(0, inplace=True)
cs['corpcode'] = cs['Corporate Code']
cs['corpname'] = cs['Corporate Name']
cs['soipquantity'] = cs['quantity']
cs['issueqty'] = cs['quantity']

index_cols = ['customername']
cs.set_index(index_cols, inplace=True)
cs = cs[~cs.index.duplicated()]
customers.set_index(index_cols, inplace=True)
customers.update(cs)
customers.reset_index(inplace=True)


###################################################################### Facilities
r_types = ['Distributor', 'Recovery', 'NPD']
returners_dw = tbl_tab_Location.loc[tbl_tab_Location['RL Location Type'].isin(r_types),['Code', 'Corporate Code', 'Corporate Name']]

cols = ['facilityname', 'constraintvalue']
return_fcst = productionconstraints.loc[(productionconstraints['notes'] == ReturnsProductionNotes) & 
                                        (productionconstraints['periodname'].str[-2:].astype(int) <= 12),
                                        cols]
return_fcst['constraintvalue'] = return_fcst['constraintvalue'].astype(float)
return_fcst = return_fcst.groupby('facilityname').sum()

returners_cf = facilities.loc[facilities['facilityname'].str.startswith('R_'), ['facilityname', 'loccode']]
returners_cf = returners_cf.merge(returners_dw, how='left', left_on='loccode', right_on='Code')
returners_cf = returners_cf.merge(return_fcst, how='left', on='facilityname')

returners_cf['constraintvalue'].fillna(0, inplace=True)
returners_cf['corpcode'] = returners_cf['Corporate Code']
returners_cf['corpname'] = returners_cf['Corporate Name']
returners_cf['returnqty'] = returners_cf['constraintvalue']

index_cols = ['facilityname']
returners_cf.set_index(index_cols, inplace=True)
facilities.set_index(index_cols, inplace=True)
facilities.update(returners_cf)
facilities.reset_index(inplace=True)
print('\tDone.\n')

#%% Lane Attributes (Alteryx workflow 020)
print('Executing : Lane Attributes (Alteryx workflow 020)...')

###################################################################### Customer Fulfillment Policies
cols = ['customername', 'sourcename', 'soipplan', 'distance', 'greenfieldcandidate', 'cpudedicated']
cfp = customerfulfillmentpolicies[cols].copy()
cfp['distance'] = cfp['distance'].astype(float)

cols = ['customername', 'country', 'georegion', 'pecoregion', 'pecosubregion', 'zone']
cus = customers[cols].copy().add_suffix('_cust')

cols = ['facilityname', 'country', 'depottype', 'georegion', 'pecoregion', 'pecosubregion', 'zone']
fac = facilities[cols].copy().add_suffix('_depo')

cols = ['customername', 'quantity']
dem = customerdemand.loc[customerdemand['periodname'].str[-2:].astype(int) <= 12,
                         cols].copy()
dem['quantity'] = dem['quantity'].astype(float)
dem = dem.groupby('customername').mean()

cfp = cfp.merge(cus, how='left', left_on='customername', right_on='customername_cust')
cfp = cfp.merge(fac, how='left', left_on='sourcename', right_on='facilityname_depo')
cfp = cfp.merge(dem, how='left', on='customername')
cfp['quantity'].fillna(0, inplace=True)

bins = [-np.inf,50,100,150,200,300,400,500,1000,1500,2000,np.inf]
labs = ['LT0050','GT0050','GT0100','GT0150','GT0200','GT0300','GT0400','GT0500','GT1000','GT1500','GT2000']

cfp['depottype'] = cfp['depottype_depo']
cfp['oregion'] = cfp['georegion_depo']
cfp['dregion'] = cfp['georegion_cust']
cfp['ocountry'] = cfp['country_depo']
cfp['dcountry'] = cfp['country_cust']
cfp['ozone'] = cfp['zone_depo']
cfp['dzone'] = cfp['zone_cust']
cfp['opecoregion'] = cfp['pecoregion_depo']
cfp['dpecoregion'] = cfp['pecoregion_cust']
cfp['opecosubregion'] = cfp['pecosubregion_depo']
cfp['dpecosubregion'] = cfp['pecosubregion_cust']
cfp['mileageband'] = pd.cut(cfp['quantity'], bins, right=False, labels=labs)
cfp['monthly_avg'] = cfp['quantity']
cfp['monthlypalletband'] = cfp.apply(lambda row: 'LT3500' if row['quantity']<=3500 else 'GT3500', axis=1)

cfp = cfp.merge(nod[['ModelID', 'number_of_depots']], how='left', left_on='customername', right_on='ModelID')
cfp['number_of_depots_served'] = cfp['number_of_depots']
cfp['nbrdepotsband'] = cfp.apply(lambda row: 'LT01' if row['number_of_depots'] <= 1 else 'GT01', axis=1)

def add_to_model(row):
    if (row['soipplan'] == 'N' and 
        row['distance'] <= 1000 and
        row['greenfieldcandidate'] == 'Y' and
        row['cpudedicated'] is None and
        (row['monthly_avg'] <= 3500 or row['number_of_depots'] >= 2)
        ):
        return 'Y'
    else:
        return 'N'
cfp['addtomodel'] = cfp.apply(add_to_model, axis=1)

# Closest Depot Identifier
cfp = cfp.merge(facilities[['facilityname', 'status']], how='left', left_on='sourcename', right_on='facilityname')
cfp.rename(columns={'status':'DepotStatus'}, inplace=True)

closest_depot = cfp[(cfp['DepotStatus']=='Include') & (cfp['depottype'].isin(['Full Service', 'Sort Only']))].copy()
closest_depot = closest_depot.sort_values(['distance', 'depottype', 'sourcename']).groupby('customername').head(1)
closest_depot = closest_depot[['sourcename', 'customername']]
closest_depot['closestdepot'] = 'Y'
cfp = cfp.merge(closest_depot, how='left', on=['sourcename', 'customername'])
cfp['closestdepot'].fillna('N', inplace=True)

index_cols = ['customername', 'sourcename']
cfp.set_index(index_cols, inplace=True)
cfp = cfp[~cfp.index.duplicated()]
customerfulfillmentpolicies.set_index(index_cols, inplace=True)
customerfulfillmentpolicies.update(cfp)
customerfulfillmentpolicies.reset_index(inplace=True)


###################################################################### Replenishment Policies

cols = ['facilityname', 'productname', 'sourcename', 'soipplan', 'distance', 'greenfieldcandidate', 'cpudedicated']
rps = replenishmentpolicies[cols].copy()
rps['distance'] = rps['distance'].astype(float)

cols = ['facilityname', 'country', 'depottype', 'georegion', 'pecoregion', 'pecosubregion', 'zone']
org = facilities[cols].copy().add_suffix('_orig')
dst = facilities[cols].copy().add_suffix('_dest')

cols = ['facilityname', 'constraintvalue']
fst = productionconstraints.loc[(productionconstraints['periodname'].str[-2:].astype(int) <= 12) & 
                                (productionconstraints['notes'] == ReturnsProductionNotes),
                                cols].copy()
fst['constraintvalue'] = fst['constraintvalue'].astype(float)
fst = fst.groupby('facilityname').mean()

rps = rps.merge(org, how='left', left_on='sourcename', right_on='facilityname_orig')
rps = rps.merge(dst, how='left', left_on='facilityname', right_on='facilityname_dest')
rps = rps.merge(fst, how='left', left_on='sourcename', right_on='facilityname')

bins = [-np.inf,50,100,150,200,300,400,500,1000,1500,2000,np.inf]
labs = ['LT0050','GT0050','GT0100','GT0150','GT0200','GT0300','GT0400','GT0500','GT1000','GT1500','GT2000']

rps['odepottype'] = rps['depottype_orig']
rps['ddepottype'] = rps['depottype_dest']
rps['ocountry'] = rps['country_orig']
rps['dcountry'] = rps['country_dest']
rps['oregion'] = rps['georegion_orig']
rps['dregion'] = rps['georegion_dest']
rps['opecoregion'] = rps['pecoregion_orig']
rps['dpecoregion'] = rps['pecoregion_dest']
rps['opecosubregion'] = rps['pecosubregion_orig']
rps['dpecosubregion'] = rps['pecosubregion_dest']
rps['mileageband'] = pd.cut(rps['constraintvalue'], bins, right=False, labels=labs)
rps['monthly_avg'] = rps['constraintvalue']
rps['monthlypalletband'] = rps.apply(lambda row: 'LT3500' if row['constraintvalue']<=3500 else 'GT3500', axis=1)

rps = rps.merge(nod[['ModelID', 'number_of_depots']], how='left', left_on='sourcename', right_on='ModelID')
rps['number_of_depots_served'] = rps['number_of_depots']
rps['nbrdepotsband'] = rps.apply(lambda row: 'LT01' if row['number_of_depots'] <= 1 else 'GT01', axis=1)

def add_to_model(row):
    if (row['sourcename'].startswith('R_') and
        row['soipplan'] == 'N' and 
        row['distance'] <= 1000 and
        row['greenfieldcandidate'] == 'Y' and
        row['cpudedicated'] is None and
        (row['monthly_avg'] <= 3500 or row['number_of_depots'] >= 2)
        ):
        return 'Y'
    else:
        return 'N'
rps['addtomodel'] = rps.apply(add_to_model, axis=1)

# Closest depot identifier.
rps = rps.merge(facilities[['facilityname', 'status']], how='left', on='facilityname')
rps.rename(columns={'status':'DepotStatus'}, inplace=True)

closest_depot = rps[(rps['DepotStatus']=='Include') & 
                    (rps['ddepottype'].isin(['Full Service', 'Sort Only'])) &
                    (rps['sourcename'].str.startswith('R'))].copy()
closest_depot = closest_depot.sort_values(['distance', 'ddepottype', 'facilityname']).groupby('sourcename').head(1)
closest_depot = closest_depot[['facilityname', 'sourcename']]
closest_depot['closestdepot'] = 'Y'
rps = rps.merge(closest_depot, how='left', on=['facilityname', 'sourcename'])
rps['closestdepot'].fillna('N', inplace=True)

index_cols = ['facilityname', 'sourcename']
rps.set_index(index_cols, inplace=True)
rps = rps[~rps.index.duplicated()]
replenishmentpolicies.set_index(index_cols, inplace=True)
replenishmentpolicies.update(rps)
replenishmentpolicies.reset_index(inplace=True)


###################################################################### Transportation Policies
cols = ['originname', 'destinationname']
tps = transportationpolicies[cols].copy()

cols = ['facilityname', 'country', 'loccode']
fac = facilities[cols]

tps = tps.merge(fac.add_suffix('_orig'), how='left', left_on='originname', right_on='facilityname_orig')
tps = tps.merge(fac.add_suffix('_dest'), how='left', left_on='destinationname', right_on='facilityname_dest')

tps['ocountry'] = tps['country_orig']
tps['dcountry'] = tps['country_dest']
tps['oloccode'] = tps['loccode_orig']
tps['dloccode'] = tps['loccode_dest']

index_cols = ['originname', 'destinationname']
tps.set_index(index_cols, inplace=True)
tps = tps[~tps.index.duplicated()]
transportationpolicies.set_index(index_cols, inplace=True)
transportationpolicies.update(tps)
transportationpolicies.reset_index(inplace=True)
print('\tDone.\n')

#%% NPD Percentage Penalty (Alteryx workflow 030)
print('Executing : NPD Percentage Penalty (Alteryx workflow 030)...')

###################################################################### Customer Fulfillment Policies
cols = ['ModelID', 'NPD %']
cfp = excel_data['Renter Assumptions'][cols].copy().drop_duplicates()
cfp['customername'] = cfp['ModelID']
cfp['depottype'] = 'Manufacturing'
cfp['unitcost'] = (cfp['NPD %']*NewPalletCost).fillna(0)

index_cols = ['customername', 'depottype']
cfp.set_index(index_cols, inplace=True)
customerfulfillmentpolicies.set_index(index_cols, inplace=True)
customerfulfillmentpolicies.update(cfp)
customerfulfillmentpolicies.reset_index(inplace=True)
print('\tDone.\n')

#%% Transportation Rates Historical (Alteryx workflow 060)
print('Executing : Transportation Rates Historical (Alteryx workflow 060)...')

# Shipment History Process.

# Calculate the average cost per load for a given OD pair, movetype. 
# Want to put a SCAC & carrier_type to that OD pair as a reference point, 
# so pick the one that is the most common for that OD pair.
# Calculate the avg cost per load cost across all carriers.

shp_hist = transport_rates_hist_costs.copy()

cond = (shp_hist['Ttl_LH_Cost']>100) | (shp_hist['Carrier_Type']=='CPU')
shp_hist = shp_hist[cond]

groupby_cols = ['movetype', 'Lane_ID', 'Depot', 'Customer', 'SCAC', 'Carrier_Type']
shp_hist = shp_hist.groupby(groupby_cols)[['Total_Loads', 'Ttl_LH_Cost']].sum()

cond = shp_hist['Total_Loads'] > 5
shp_hist = shp_hist[cond].reset_index()

groupby_cols = ['movetype', 'Lane_ID']
shp_cst_per_load_avg = shp_hist.groupby(groupby_cols)[['Total_Loads', 'Ttl_LH_Cost']].sum().reset_index()

groupby_cols = ['movetype', 'Lane_ID', 'SCAC', 'Carrier_Type']
shp_top_carrier = shp_hist.groupby(groupby_cols)[['Total_Loads']].sum().reset_index()
shp_top_carrier = shp_top_carrier.sort_values(['Total_Loads', 'SCAC'], ascending=[False, True]).groupby(['movetype', 'Lane_ID']).first().reset_index()

merge_cols = ['movetype', 'Lane_ID']
shp_hist_final = shp_cst_per_load_avg.merge(shp_top_carrier, how='inner', on=merge_cols,
                                            suffixes=('', '_carrier_max'))
shp_hist_final['CostPerLoadAvg'] = shp_hist_final['Ttl_LH_Cost']/shp_hist_final['Total_Loads']

cols_to_keep = ['movetype','Lane_ID','SCAC','Carrier_Type','CostPerLoadAvg']
cost_per_load = shp_hist_final[cols_to_keep].copy()   # Renaming to 'cost_per_load' as this is more descriptive.

# History - Issues, Returns, and Transfers by Lane Type (loads by lane type)
lblt = transport_rates_hist_load_counts.copy()
lblt = lblt.dropna()

# Join Loads by Lane Type to Cost per Load
lblt = lblt.merge(cost_per_load, how='outer', on=['movetype', 'Lane_ID'])

# Update 'Type' based on conditions.
def label_type(row):
    split = row['Split_Carrier_Type_Flag']=='Split'
    cpu = row['CPU_Loads']
    ded = row['Dedicated_Loads']
    oth = row['Other_Loads']
    
    if not split:
        if cpu > 0: return 'CPU'
        elif ded > 0: return 'Dedicated'
        else: return 'Contract Carrier'
    
    else:
        if cpu > ded and cpu > oth: return 'CPU'
        elif ded > cpu and ded > oth: return 'Dedicated'
        else: return 'Contract Carrier'
        
lblt['Type'] = lblt.apply(label_type, axis=1)   

# Bring in ModelID's
lblt_iss = lblt[lblt['movetype']=='Issue'].copy()
lblt_ret = lblt[lblt['movetype']=='Return'].copy()
lblt_trs = lblt[~lblt['movetype'].isin(['Issue', 'Return'])].copy()

# Bring in Model ID's for the Origin and Destination Locations
locs_issue  = customers[['customername', 'loccode']].drop_duplicates()
locs_depot  = facilities.loc[facilities['facilityname'].str.startswith('D_'), ['facilityname', 'loccode']].drop_duplicates()
locs_return = facilities.loc[facilities['facilityname'].str.startswith('R_'), ['facilityname', 'loccode']].drop_duplicates()


lblt_iss = lblt_iss.merge(locs_issue, how='left', left_on='Customer', right_on='loccode')
lblt_iss = lblt_iss.merge(locs_depot, how='left', left_on='Depot', right_on='loccode')
lblt_iss.rename(columns={'facilityname':'originname', 'customername':'destinationname'}, inplace=True)
lblt_iss.drop(columns=['loccode_x', 'loccode_y'], inplace=True)

lblt_ret = lblt_ret.merge(locs_return, how='left', left_on='Customer', right_on='loccode')
lblt_ret = lblt_ret.merge(locs_depot, how='left', left_on='Depot', right_on='loccode')
lblt_ret.rename(columns={'facilityname_x':'originname', 'facilityname_y':'destinationname'}, inplace=True)
lblt_ret.drop(columns=['loccode_x', 'loccode_y'], inplace=True)

lblt_trs['orig'] = lblt_trs['Lane_ID'].str[0:5]
lblt_trs['dest'] = lblt_trs['Lane_ID'].str[-5:]
lblt_trs = lblt_trs.merge(locs_depot, how='left', left_on='orig', right_on='loccode')
lblt_trs = lblt_trs.merge(locs_depot, how='left', left_on='dest', right_on='loccode')
lblt_trs.rename(columns={'facilityname_x':'originname', 'facilityname_y':'destinationname'}, inplace=True)
lblt_trs.drop(columns=['loccode_x', 'loccode_y'], inplace=True)

cols_to_keep = list(lblt.columns)+['originname', 'destinationname']

lblt = pd.concat([lblt_iss[cols_to_keep], lblt_ret[cols_to_keep], lblt_trs[cols_to_keep]]).reset_index(drop=True)
lblt.dropna(subset=['origin_Name', 'Destination_Name'], inplace=True)

# Rename to match transportationpolicies column names.
lblt.rename(columns={'CostPerLoadAvg':'histrate', 'SCAC':'scac', 'Type':'scaccarriertype'}, inplace=True)


###################################################################### Transportation Policies
# NOTE: Need to add a new column to transportationpolicies (rfq_rate).
cols = ['originname','productname','destinationname','ocountry','dcountry','oloccode','dloccode',
        'histrate','marketrate','rateused','fixedcost','scac','scaccarriertype','cpu','unitcost',
        'dutyrate']
tps = transportationpolicies[cols].copy()

# Reset all columns that we want to update.
cols_to_update = [i for i in cols if i not in ['originname','destinationname','ocountry',
                                               'dcountry','oloccode','dloccode','marketrate']]

# Reset values prior to updating.
tps[cols_to_update] = None
tps['rfqrate'] = None     # Adding here and not above becasue currently rfqrate isn't a column in transportationpolicies.

# Update oloccode and dloccode
tps['oloccode'] = tps['originname'].str[-5:]
tps['dloccode'] = tps['destinationname'].str[-5:]

# Get rfqrate from Excel input data.
trans_rfq_rates = excel_data['Trans RFQ Rates'].copy()
trans_rfq_rates.rename(columns={'Final Rate Award':'rfqrate'}, inplace=True)
trans_rfq_rates['oloccode'] = trans_rfq_rates['Lane Name'].str[0:5]
trans_rfq_rates['dloccode'] = trans_rfq_rates['Lane Name'].str[-5:]

cols = ['oloccode', 'dloccode', 'rfqrate']
tps = tps.merge(trans_rfq_rates[cols], how='left', on=['oloccode', 'dloccode'], suffixes=('_drop', ''))
tps.drop(columns=[c for c in tps.columns if '_drop' in c], inplace=True)

# Update histrate, scac, scaccarriertype, and cpu
cols = ['originname', 'destinationname', 'histrate', 'scac', 'scaccarriertype']
tps = tps.merge(lblt[cols], how='left', on=['originname', 'destinationname'], suffixes=('_drop', ''))
tps.drop(columns=[c for c in tps.columns if '_drop' in c], inplace=True)

# =============================================================================
# Rate priority:
#     if carriertype = 'CPU':
#         fixedcost = 0
#         rateused = 'CPU'
#         cpu = 'C'
# 
#     else: 
#         'D' if carriertype = 'Dedicated' else None
#         fixedcost = rfqrate => histrate => Market (should always have a value)
#         rateused based on fixedcost selection
# =============================================================================

tps['histrate'] = tps['histrate'].astype(float)
tps['rfqrate'] = tps['rfqrate'].astype(float)
tps['marketrate'] = tps['marketrate'].astype(float)

# Set rates for CPUs
cpu = tps['scaccarriertype']=='CPU'
tps.loc[cpu, ['rateused', 'fixedcost', 'cpu']] = ['CPU', 0, 'C']

# Set cpu flag for dedicated.
ded = tps['scaccarriertype']=='Dedicated'
tps.loc[ded, 'cpu'] = 'D'

# Choose the appropriate rate to use.
# Use RFQ rate.
rfq = ~tps['rfqrate'].isna()
tps.loc[~cpu & rfq, 'fixedcost'] = tps['rfqrate']
tps.loc[~cpu & rfq, 'rateused'] = 'rfqrate'

# Use historical rate.
hst = ~tps['histrate'].isna()
tps.loc[~cpu & ~rfq & hst, 'fixedcost'] = tps['histrate']
tps.loc[~cpu & ~rfq & hst, 'rateused'] = 'HistRate'

# Use market rate.
tps.loc[~cpu & ~rfq & ~hst, 'fixedcost'] = tps['marketrate']
tps.loc[~cpu & ~rfq & ~hst, 'rateused'] = 'MarketRate'

# Set fuel surcharge.
tps.loc[~cpu, 'unitcost'] = Fuel_Surcharge

# Set the duty rates.
us_to_can = (tps['ocountry']=='USA') & (tps['dcountry']=='CAN')
can_to_us = (tps['ocountry']=='CAN') & (tps['dcountry']=='USA')
tps.loc[us_to_can, 'dutyrate'] = Duty_Rate_US_to_Canada
tps.loc[can_to_us, 'dutyrate'] = Duty_Rate_Canada_to_US

index_cols = ['originname', 'destinationname', 'productname']
tps.set_index(index_cols, inplace=True)
tps = tps[~tps.index.duplicated()]
transportationpolicies.set_index(index_cols, inplace=True)
transportationpolicies.update(tps)
transportationpolicies.reset_index(inplace=True)


###################################################################### Customer Fulfillment Policies

### Update Customer Fulfillment Policies and Replenishment Policies using the new 
### Transportation Policies data.
cols = ['originname', 'destinationname', 'cpu']
tps = transportationpolicies[cols].copy()
tps = tps[tps['cpu'].isin(['C', 'D'])].drop_duplicates()

cols = ['sourcename', 'customername']
cfp = customerfulfillmentpolicies[cols].copy().drop_duplicates()

# Identify customers that have customer pickup issue lanes.
cfp = cfp.merge(tps, how='left', left_on='customername', right_on='destinationname')

# Label these customers with an "N" in front of their CPU flag.
flag = ~cfp['cpu'].isna()
cfp.loc[flag, 'cpudedicated'] = 'N'+cfp['cpu']

# Bring in the specific CPU and dedicated lanes that are in the transportation policies.
cfp = cfp.merge(tps, how='left', left_on=['customername', 'sourcename'], 
                                 right_on=['destinationname', 'originname'],
                                 suffixes=('_drop', ''))

# Update these OD pairs with their CPU flag.
flag = ~cfp['cpu'].isna()
cfp.loc[flag, 'cpudedicated'] = cfp['cpu']

# Drop unneeded columns
cols = [c for c in cfp.columns if '_drop' in c] + ['originname', 'destinationname', 'cpu']
cfp.drop(columns=cols, inplace=True)

# Update Customer Fulfillment Policies
index_cols = ['customername', 'sourcename']
cfp.set_index(index_cols, inplace=True)
cfp = cfp[~cfp.index.duplicated()]
customerfulfillmentpolicies.set_index(index_cols, inplace=True)
customerfulfillmentpolicies.update(cfp)
customerfulfillmentpolicies.reset_index(inplace=True)


###################################################################### Replenishment Policies
cols = ['sourcename', 'facilityname']
rps = replenishmentpolicies[cols].copy().drop_duplicates()

# Identify customers that have customer pickup return lanes.
rps = rps.merge(tps, how='left', left_on='sourcename', right_on='originname')

# Label these customers with an "N" in front of their CPU flag.
flag = ~rps['cpu'].isna()
rps.loc[flag, 'cpudedicated'] = 'N'+rps['cpu']

# Bring in the specific CPU and dedicated lanes that are in the transportation policies.
rps = rps.merge(tps, how='left', left_on=['facilityname', 'sourcename'], 
                                 right_on=['destinationname', 'originname'],
                                 suffixes=('_drop', ''))

# Update these OD pairs with their CPU flag.
flag = ~rps['cpu'].isna()
rps.loc[flag, 'cpudedicated'] = rps['cpu']

# Drop unneeded columns
cols = [c for c in rps.columns if '_drop' in c] + ['originname', 'destinationname', 'cpu']
rps.drop(columns=cols, inplace=True)

# Update Customer Fulfillment Policies
index_cols = ['sourcename', 'facilityname']
rps.set_index(index_cols, inplace=True)
rps = rps[~rps.index.duplicated()]
replenishmentpolicies.set_index(index_cols, inplace=True)
replenishmentpolicies.update(rps)
replenishmentpolicies.reset_index(inplace=True)
print('\tDone.\n')


#%% Transportation Load Size (Alteryx workflow 070)
print('Executing : Transportation Load Size (Alteryx workflow 070)...')


###################################################################### Customers
cs = tls.loc[tls['movetype']=='Issue',['destinationname', 'Average_Cube']].copy()
cs.rename(columns={'destinationname':'customername'}, inplace=True)

cus = customers['customername'].copy().to_frame().drop_duplicates()
cus = cus.merge(cs, how='left', on='customername')
cus['avgloadsz'] = cus['Average_Cube'].fillna(Avg_Load_Size_Issues)

index_cols = ['customername']
cus.set_index(index_cols, inplace=True)
customers.set_index(index_cols, inplace=True)
customers.update(cus)
customers.reset_index(inplace=True)


###################################################################### Facilities
fs = tls.loc[tls['movetype']=='Return',['originname', 'Average_Cube']].copy()
fs.rename(columns={'originname':'facilityname'}, inplace=True)

fac = facilities['facilityname'].copy().to_frame().drop_duplicates()
fac = fac.merge(fs, how='left', on='facilityname')

fac.loc[fac['facilityname'].str.startswith('R_'), 'defaultloadsz'] = Avg_Load_Size_Returns
fac.loc[fac['facilityname'].str.startswith('D_'), 'defaultloadsz'] = Avg_Load_Size_Transfers
fac['avgloadsz'] = fac[['Average_Cube', 'defaultloadsz']].bfill(axis=1).iloc[:,0]

index_cols = ['facilityname']
fac.set_index(index_cols, inplace=True)
facilities.set_index(index_cols, inplace=True)
facilities.update(fac)
facilities.reset_index(inplace=True)


###################################################################### Transportation Policies
cols = ['originname', 'destinationname', 'productname', 'modename']
tps = transportationpolicies[cols].dropna(subset=['originname', 'destinationname']).copy()

choices = ['Issue', 'Return']
conditions = [tps['destinationname'].str.startswith('I_'),
              tps['originname'].str.startswith('R_')]
tps['movetype'] = np.select(conditions, choices, default='Transfer')

issues    = tps[tps['movetype']=='Issue'].copy()
returns   = tps[tps['movetype']=='Return'].copy()
transfers = tps[tps['movetype']=='Transfer'].copy()

issues = issues.merge(tls[['movetype', 'destinationname', 'Average_Cube']], how='left', 
                      on=['movetype', 'destinationname'])
issues['averageshipmentsize'] = issues['Average_Cube'].fillna(Avg_Load_Size_Issues)

returns = returns.merge(tls[['movetype', 'originname', 'Average_Cube']], how='left', 
                        on=['movetype', 'originname'])
returns['averageshipmentsize'] = returns['Average_Cube'].fillna(Avg_Load_Size_Returns)

transfers['averageshipmentsize'] = Avg_Load_Size_Transfers

tps = pd.concat([issues, returns, transfers])

index_cols = ['originname', 'destinationname', 'productname', 'modename']
tps.set_index(index_cols, inplace=True)
tps = tps[~tps.index.duplicated()]
transportationpolicies.set_index(index_cols, inplace=True)
transportationpolicies.update(tps)
transportationpolicies.reset_index(inplace=True)
print('\tDone.\n')


#%% Flag Multi-Source Options (Alteryx workflow 090)
print('Executing : Flag Multi-Source Options (Alteryx workflow 090)...')

###################################################################### Customer Fulfillment Policies
cols = ['sourcename', 'customername']
cfp = customerfulfillmentpolicies[cols].copy()

iss = msl.loc[msl['MoveType']=='Issue', 'DModelID'].drop_duplicates().to_frame()
cfp = cfp.merge(iss, how='left', left_on='customername', right_on='DModelID')
cols = ['OModelID', 'DModelID']
cfp = cfp.merge(msl[cols], how='left', left_on=['sourcename', 'customername'], 
                                       right_on=['OModelID', 'DModelID'], suffixes=('', '_msl'))

cond_N = ~cfp['DModelID'].isna() &  cfp['OModelID'].isna()
cond_Y = ~cfp['DModelID'].isna() & ~cfp['OModelID'].isna()

cfp.loc[cond_N, 'multi_source_option'] = 'N'
cfp.loc[cond_Y, 'multi_source_option'] = 'Y'

index_cols = ['customername', 'sourcename']
cfp.set_index(index_cols, inplace=True)
cfp = cfp[~cfp.index.duplicated()]
customerfulfillmentpolicies.set_index(index_cols, inplace=True)
customerfulfillmentpolicies.update(cfp)
customerfulfillmentpolicies.reset_index(inplace=True)


###################################################################### Groups

#Return locations
ret = msl.loc[msl['MoveType']=='Return', 'OModelID'].drop_duplicates().to_frame() # 37
ret_locs = facilities.loc[facilities['facilityname'].str.startswith('R_'),
                          'facilityname'].drop_duplicates().to_frame()
ret_locs = ret_locs.merge(ret, how='left', left_on='facilityname', right_on='OModelID')

cond = ~ret_locs['OModelID'].isnull()
ret_locs.loc[cond, 'groupname']  = 'SplitSource_Distributor_AllowToMultiSource'
ret_locs.loc[~cond, 'groupname'] = 'SplitSource_Distributor_KeepSingleSource'
ret_locs['grouptype'] = 'Facilities'
ret_locs.rename(columns={'facilityname':'membername'}, inplace=True)

# Issue locations
iss = msl.loc[msl['MoveType']=='Issue', 'DModelID'].drop_duplicates().to_frame() # 49
iss_locs = customers['customername'].drop_duplicates().to_frame()
iss_locs = iss_locs.merge(iss, how='left', left_on='customername', right_on='DModelID')

cond = ~iss_locs['DModelID'].isnull()
iss_locs.loc[cond, 'groupname']  = 'SplitSource_Renter_AllowToMultiSource'
iss_locs.loc[~cond, 'groupname'] = 'SplitSource_Renter_KeepSingleSource'
iss_locs['grouptype'] = 'Customers'
iss_locs.rename(columns={'customername':'membername'}, inplace=True)

cols = ['membername', 'groupname', 'grouptype']
grp = pd.concat([iss_locs[cols], ret_locs[cols]])
#grp['status'] = 'ADDED'   # For testing only. Comment this out.
#grp['notes'] = 'ADDED'    # For testing only. Comment this out.

# Note: Can't use DataFrame.update for groups, as the primary keys of dataset are what is being 
# updated. Need to make a new Groups dataframe.
cond = groups['groupname'].isin(['SplitSource_Distributor_KeepSingleSource',
                                 'SplitSource_Distributor_AllowToMultiSource',
                                 'SplitSource_Renter_KeepSingleSource',
                                 'SplitSource_Renter_AllowToMultiSource'])
# This is the new Groups table.
groups = pd.concat([grp, groups[~cond]])


###################################################################### Replenishment Policies
cols = ['sourcename', 'facilityname']
rps = replenishmentpolicies[cols].copy()

ret = msl.loc[msl['MoveType']=='Return', 'OModelID'].drop_duplicates().to_frame()
rps = rps.merge(ret, how='left', left_on='sourcename', right_on='OModelID')
cols = ['OModelID', 'DModelID']
rps = rps.merge(msl[cols], how='left', left_on=['sourcename', 'facilityname'], 
                                       right_on=['OModelID', 'DModelID'], suffixes=('', '_msl'))

cond_N = ~rps['OModelID'].isna() &  rps['DModelID'].isna()
cond_Y = ~rps['OModelID'].isna() & ~rps['DModelID'].isna()

rps.loc[cond_N, 'multi_source_option'] = 'N'
rps.loc[cond_Y, 'multi_source_option'] = 'Y'

index_cols = ['facilityname', 'sourcename']
rps.set_index(index_cols, inplace=True)
rps = rps[~rps.index.duplicated()]
replenishmentpolicies.set_index(index_cols, inplace=True)
replenishmentpolicies.update(rps)
replenishmentpolicies.reset_index(inplace=True)
print('\tDone.\n')

#%% Transfer Matrix Update (Alteryx workflow 100)
print('Executing : Transfer Matrix Update (Alteryx workflow 100)...')

###################################################################### Replenishment Policies
cols = ['facilityname', 'sourcename', 'odepottype', 'ddepottype', 'status']
rps = replenishmentpolicies[cols].copy()

transfers = rps[rps['sourcename'].str.startswith('D') & rps['facilityname'].str.startswith('D')].index
rps.drop(transfers, inplace=True)

rps['status'] = 'Include'

# mfg_lanes
rps.loc[(rps['odepottype']=='Manufacturing') & 
        (rps['ddepottype'].isin(['Manufacturing', 'DO NOT USE', 'Distributor Sort', 
                                 'Renter Sort', 'Repair Only', 'Sort Only'])),
        'status'] = 'Exclude'

# fsd_lanes
rps.loc[(rps['odepottype']=='Full Service') & 
        rps['ddepottype'].isin(['Manufacturing', 'DO NOT USE']),
        'status'] = 'Exclude'

# srt_lanes
rps.loc[(rps['odepottype']=='Sort Only') & 
        rps['ddepottype'].isin(['Manufacturing', 'DO NOT USE', 'Sort Only']),
        'status'] = 'Exclude'

# sto_lanes
rps.loc[(rps['odepottype']=='Storage') & 
        rps['ddepottype'].isin(['Manufacturing', 'DO NOT USE', 
                                'Distributor Sort', 'Renter Sort', 'Storage']),
        'status'] = 'Exclude'

# rep_lanes
rps.loc[(rps['odepottype']=='Repair Only') & 
        rps['ddepottype'].isin(['Manufacturing', 'DO NOT USE', 'Distributor Sort', 'Renter Sort']),
        'status'] = 'Exclude'

# dnu_lanes
rps.loc[rps['odepottype']=='DO NOT USE', 'status'] = 'Exclude'

# drs_lanes
rps.loc[rps['odepottype'].isin(['Distributor Sort', 'Renter Sort']) & 
        (rps['ddepottype'] != "Full Service"),
        'status'] = 'Exclude'

index_cols = ['facilityname', 'sourcename']
rps.set_index(index_cols, inplace=True)
rps = rps[~rps.index.duplicated()]
replenishmentpolicies.set_index(index_cols, inplace=True)
replenishmentpolicies.update(rps)
replenishmentpolicies.reset_index(inplace=True)
print('\tDone.\n')

#%% Renter Distributor Sort Preferred Depot (Alteryx worklfow 110)
print('Executing : Renter Distributor Sort Preferred Depot (Alteryx worklfow 110)...')

###################################################################### Replenishment Policies

cols = ['Ocode', 'Dcode']
rps = excel_data['RenterDistSort Preferred Depot'][cols]
o_d = rps.rename(columns={'Ocode':'facilityname', 'Dcode':'sourcename'}).copy()
d_o = rps.rename(columns={'Ocode':'sourcename', 'Dcode':'facilityname'}).copy()
rps = pd.concat([o_d, d_o]).drop_duplicates()
rps['rentdistsortprefassig'] = 'Y'

replenishmentpolicies['rentdistsortprefassig'] = 'N'

index_cols = ['facilityname', 'sourcename', 'rentdistsortprefassig']
rps.set_index(index_cols, inplace=True)
replenishmentpolicies.set_index(index_cols, inplace=True)
replenishmentpolicies.update(rps)
replenishmentpolicies.reset_index(inplace=True)
print('\tDone.\n')

#%% Upload new tables to Cosmic Frog

data_to_upload  = {'customerfulfillmentpolicies':customerfulfillmentpolicies,
                   'customers':customers,
                   'facilities':facilities,
                   'groups':groups,
                   'inventoryconstraints':inventoryconstraints,
                   'inventorypolicies':inventorypolicies,
                   'periods':periods,
                   'productionconstraints':productionconstraints,
                   'productionpolicies':productionpolicies,
                   'replenishmentpolicies':replenishmentpolicies,
                   'transportationpolicies':transportationpolicies,
                   'warehousingpolicies':warehousingpolicies,
                   }

def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)
        
def replace_data_in_cosmic_frog(USER_NAME, APP_KEY, OUTPUT_DB_NAME, data_to_upload):
    # Note: This syntax is compatible with SQLAlchemy 2.0.
    print('Connecting to Cosmic Frog to upload data.')
    
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")     # Ignore the Cosmic Frog API warning.
    
        ### Intializing API Connection
        api = pioneer.Api(auth_legacy=False, un=USER_NAME, appkey=APP_KEY)

        ### Create connection to the model database
        connection_str = api.sql_connection_info(OUTPUT_DB_NAME)
        connection_string = connection_str['connectionStrings']['url']
        engine = sal.create_engine(connection_string)
        conn = engine.connect()

        for table_name, table in data_to_upload.items():
            print(f'Deleting all rows from: {table_name}...')
            with engine.connect() as conn:
                conn.execute(sal.text(f'delete from {table_name}'))
                conn.commit()
            
            print(f'Uploading data to table: {table_name}...')
            if 'index' in table.columns:
                del table['index']
            table.to_sql(table_name, con=engine, if_exists='append', index=False, method=psql_insert_copy)
            print('\tDone.')
    
replace_data_in_cosmic_frog(USER_NAME, APP_KEY, OUTPUT_DB_NAME, data_to_upload)
t1=time.time()
print(f'DONE! The program took {round((t1-t0)/60)} minutes to complete.')
