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
a local Excel workbook and data pulled from PECO's data warehouse, then reuploads the edited data
tables back to the same Cosmis Frog optimization model.
"""

# Imports
import sqlalchemy as sal
import pandas as pd
import numpy as np
import warnings
import os
import sys
from optilogic import pioneer
from collections import defaultdict

# Set display format for pandas.
pd.options.display.float_format = "{:,.2f}".format

# Add project root to PATH to allow for relative imports. 
ROOT = os.path.abspath(os.path.join('..'))
if ROOT not in sys.path:
    sys.path.append(ROOT)

# Import SQL Statements
from sql_statements import tbl_tab_Location_sql, nbr_of_depots_sql, \
    transport_rates_hist_load_counts_sql, transport_rates_hist_costs_sql, transport_load_size_sql
    
# Import User-Input data.
from user_inputs import USER_NAME, APP_KEY, DB_NAME, RepairCapacityNotes, MinInventoryNotes, \
    DepotCapacityNotes, BeginningInvNotes, ReturnsProductionNotes, CustomerDemandNotes, \
    ProductionPolicyRepairBOMName, NewPalletCost, Avg_Load_Size_Issues, Avg_Load_Size_Returns, \
    Avg_Load_Size_Transfers
    
# Import Excel IO function.
from excel_data_validation import pull_data_from_excel

# Define functions to pull data.

def pull_data_from_cosmic_frog(USER_NAME, APP_KEY, DB_NAME, tables_we_want):
    # Note: This syntax is compatible with SQLAlchemy 2.0.
    print('\nPulling data from Cosmic Frog...')
    
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")     # Ignore the Cosmic Frog API warning.
    
        # Code that makes connection to the Cosmic Frog database.
        api = pioneer.Api(auth_legacy = False, un=USER_NAME, appkey=APP_KEY)
        connection_str = api.sql_connection_info(DB_NAME)
        connection_string = 'postgresql://'+connection_str['raw']['user']+':'+ \
            connection_str['raw']['password']+'@'+connection_str['raw']['host']+':'+ \
            str(connection_str['raw']['port'])+'/'+connection_str['raw']['dbname']+'?sslmode=require'
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

# Pull data from Excel.
excel_data, error_count = pull_data_from_excel()

# Exit the program. Ensure the errors in the Excel data are correct before moving on.
if error_count > 0:
    exit()
    
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
cosmic_frog_data = pull_data_from_cosmic_frog(USER_NAME, APP_KEY, DB_NAME, tables_we_want)

# Pull data from PECO's data warehouse.
sql_name_dict = {'tbl_tab_Location':tbl_tab_Location_sql,
                 'nbr_of_depots':nbr_of_depots_sql,
                 'transport_rates_hist_load_counts':transport_rates_hist_load_counts_sql,
                 'transport_rates_hist_costs':transport_rates_hist_costs_sql,
                 'transport_load_size':transport_load_size_sql}
data_warehouse_data = pull_data_from_data_warehouse(sql_name_dict)

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

# NOTE: We will change this during development and compare to the unedited dataframes. 
#       Remove this section after development is complete.
customerdemand_orig = cosmic_frog_data['customerdemand'].copy()
customerfulfillmentpolicies_orig = cosmic_frog_data['customerfulfillmentpolicies'].copy()
customers_orig = cosmic_frog_data['customers'].copy()
facilities_orig = cosmic_frog_data['facilities'].copy()
groups_orig = cosmic_frog_data['groups'].copy()
inventoryconstraints_orig = cosmic_frog_data['inventoryconstraints'].copy()
inventorypolicies_orig = cosmic_frog_data['inventorypolicies'].copy()
periods_orig = cosmic_frog_data['periods'].copy()
productionconstraints_orig = cosmic_frog_data['productionconstraints'].copy()
productionpolicies_orig = cosmic_frog_data['productionpolicies'].copy()
replenishmentpolicies_orig = cosmic_frog_data['replenishmentpolicies'].copy()
transportationpolicies_orig = cosmic_frog_data['transportationpolicies'].copy()
warehousingpolicies_orig = cosmic_frog_data['warehousingpolicies'].copy()



# Data Warehouse Data
tbl_tab_Location = data_warehouse_data['tbl_tab_Location'].copy()
nbr_of_depots = data_warehouse_data['nbr_of_depots'].copy()
transport_rates_hist_load_counts = data_warehouse_data['transport_rates_hist_load_counts'].copy()
transport_rates_hist_costs = data_warehouse_data['transport_rates_hist_costs'].copy()
transport_load_size = data_warehouse_data['transport_load_size'].copy()

# NOTE: We will change this during development and compare to the unedited dataframes. 
#       Remove this section after development is complete.
tbl_tab_Location_orig = data_warehouse_data['tbl_tab_Location'].copy()
nbr_of_depots_orig = data_warehouse_data['nbr_of_depots'].copy()
transport_rates_hist_load_counts_orig = data_warehouse_data['transport_rates_hist_load_counts'].copy()
transport_rates_hist_costs_orig = data_warehouse_data['transport_rates_hist_costs'].copy()
transport_load_size_orig = data_warehouse_data['transport_load_size'].copy()


#%%

########################################## Number of Depots - (For 020-Lane Attributes)
# uses nbr_of_depots
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

########################################## Transport Load Size - (For 070-Trans Load Size)
iss = transport_load_size[transport_load_size['movetype']=='Issue'].copy()
oth = transport_load_size[transport_load_size['movetype']!='Issue'].copy()
iss = iss.merge(customers[['loccode', 'customername']], how='left', left_on='customer_Loc_Code', right_on='loccode')
oth = oth.merge(facilities[['loccode', 'facilityname']], how='left', left_on='customer_Loc_Code', right_on='loccode')
iss.rename(columns={'customername':'ModelID'}, inplace=True)
oth.rename(columns={'facilityname':'ModelID'}, inplace=True)

tls = pd.concat([iss, oth])
tls['originname'] = tls.apply(lambda row: row['ModelID'] if row['movetype'] == 'Return' else None, axis=1)
tls['destinationname'] = tls.apply(lambda row: row['ModelID'] if row['movetype'] == 'Issue' else None, axis=1)

# Testing
#tls['Average_Cube'] = 10000

# tls[tls['ModelID'].isna()].groupby('movetype').size()
# NOTE: 298 Rows where ModelID is blank... 28 issue locations and 275 return locations are receiving
# or returning pallets and are not in the model.

########################################## SOIP Assumptions - (For 090-Flag Multi-Source Options)
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

#ret = msl.loc[msl['MoveType']=='Return', 'OModelID'].unique() # 37
#iss = msl.loc[msl['MoveType']=='Issue', 'DModelID'].unique() # 49


########################################## Update Customer Fulfillment Policies
#010 - Depot Costs and Attributes v2
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


#015 - Set SOIP Solve Flag
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
customerfulfillmentpolicies.set_index(index_cols, inplace=True)
customerfulfillmentpolicies.update(cfp)
customerfulfillmentpolicies.reset_index(inplace=True)

#020 - Lane Attributes
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
customerfulfillmentpolicies.set_index(index_cols, inplace=True)
customerfulfillmentpolicies.update(cfp)
customerfulfillmentpolicies.reset_index(inplace=True)

#030 - NPD Percentage Penalty
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


#060 - Transportation Rates Historical

#090 - Flag Multi-Source Options
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
customerfulfillmentpolicies.set_index(index_cols, inplace=True)
customerfulfillmentpolicies.update(cfp)
customerfulfillmentpolicies.reset_index(inplace=True)


########################################## Update Customers
#015 - Set SOIP Solve Flag
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

#017 - Issue and Return Location Details
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
customers.set_index(index_cols, inplace=True)
customers.update(cs)
customers.reset_index(inplace=True)


#070 - Trans Load Size
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


########################################## Update Facilities
#### 010 - Depot Costs and Attributes v2
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
facilities.set_index('facilityname')
facilities.update(fac)
facilities.reset_index(inplace=True)


#015 - Set SOIP Solve Flag
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

#017 - Issue and Return Location Details
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

#070 - Trans Load Size
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


########################################## Update Groups

#090 - Flag Multi-Source Options
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
grp['status'] = 'ADDED'
grp['notes'] = 'ADDED'

# Note: Can't use DataFrame.update for groups, as the primary keys of dataset are what is being 
# updated. Need to reform the Groups dataframe.
cond = groups['groupname'].isin(['SplitSource_Distributor_KeepSingleSource',
                                 'SplitSource_Distributor_AllowToMultiSource',
                                 'SplitSource_Renter_KeepSingleSource',
                                 'SplitSource_Renter_AllowToMultiSource'])
# This is the new Groups table.
grp = pd.concat([grp, groups[~cond]])

########################################## Update Inventory Constraints
#010 - Depot Costs and Attributes v2
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
inventoryconstraints.set_index(index_cols, inplace=True)
inventoryconstraints.update(ic)
inventoryconstraints.reset_index(inplace=True)


########################################## Update Inventory Policies
#010 - Depot Costs and Attributes v2
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
inventorypolicies.set_index(index_cols, inplace=True)
inventorypolicies.update(ip)
inventorypolicies.reset_index(inplace=True)

########################################## Update Production Constraints
#010 - Depot Costs and Attributes v2
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
productionconstraints.set_index(index_cols, inplace=True)
productionconstraints.update(pc)
productionconstraints.reset_index(inplace=True)

########################################## Update Production Policies
#010 - Depot Costs and Attributes v2
cols = ['ModelID', 'Type', 'Repair Upd']
pps = excel_data['Depot Assumptions'][cols].copy()

pps['bomname'] = ProductionPolicyRepairBOMName
pps['unitcost'] = pps['Repair Upd'].fillna(0)
pps['facilityname'] = pps['ModelID']

index_cols = ['facilityname', 'bomname']
pps.set_index(index_cols, inplace=True)
productionpolicies.set_index(index_cols, inplace=True)
productionpolicies.update(pps)
productionpolicies.reset_index(inplace=True)

########################################## Update Replenishment Policies
#010 - Depot Costs and Attributes v2
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

replenishmentpolicies.loc[replenishmentpolicies['sourcename'].str.startswith('R_'), 'odepottype'] = 'Return Location'
replenishmentpolicies.loc[replenishmentpolicies['facilityname'].str.startswith('R_'), 'ddepottype'] = 'Return Location'
replenishmentpolicies.reset_index(inplace=True)

#015 - Set SOIP Solve Flag
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
replenishmentpolicies.set_index(index_cols, inplace=True)
replenishmentpolicies.update(rps)
replenishmentpolicies.reset_index(inplace=True)

replenishmentpolicies.loc[replenishmentpolicies['notes']=='Baseline_Returns'].shape

#020 - Lane Attributes
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
replenishmentpolicies.set_index(index_cols, inplace=True)
replenishmentpolicies.update(rps)
replenishmentpolicies.reset_index(inplace=True)


#060 - Transportation Rates Historical

#090 - Flag Multi-Source Options
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
replenishmentpolicies.set_index(index_cols, inplace=True)
replenishmentpolicies.update(rps)
replenishmentpolicies.reset_index(inplace=True)


#100 - Transfer Matrix Update
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
replenishmentpolicies.set_index(index_cols, inplace=True)
replenishmentpolicies.update(rps)
replenishmentpolicies.reset_index(inplace=True)


#110 - Renter Dist Sort Pref Depot
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


########################################## Update Transportation Policies
#020 - Lane Attributes
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
transportationpolicies.set_index(index_cols, inplace=True)
transportationpolicies.update(tps)
transportationpolicies.reset_index(inplace=True)

#060 - Transportation Rates Historical

#070 - Trans Load Size
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


########################################## Update Warehousing Policies
#010 - Depot Costs and Attributes v2
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

#%% 060 Transportation Rates Historical

# Calculate the average cost per load for a given OD pair, movetype. 
# Want to put a SCAC & carrier_type to that OD pair as a reference point, 
# so pick the one that is the most common for that OD pair.
# Calculate the avg cost per load cost across all carriers.



# Shipment History Process.
shp_hist = transport_rates_hist_costs.copy()

cond = (shp_hist['Ttl_LH_Cost']>100) | (shp_hist['Carrier_Type']=='CPU')
shp_hist = shp_hist[cond]

groupby_cols = ['movetype', 'Lane_ID', 'Depot', 'Customer', 'SCAC', 'Carrier_Type']
shp_hist = shp_hist.groupby(groupby_cols)[['Total_Loads', 'Ttl_LH_Cost']].sum()

cond = shp_hist['Total_Loads'] > 5
shp_hist = shp_hist[cond].reset_index()

groupby_cols = ['movetype', 'Lane_ID', 'Depot', 'Customer']
shp_hist_ttl = shp_hist.groupby(groupby_cols).agg({'Total_Loads':['max','sum'],
                                                   'Ttl_LH_Cost':'sum'}).reset_index()
shp_hist_ttl.columns = shp_hist_ttl.columns.map(lambda x: f"{x[1]}{x[0]}")

shp_hist_joined = shp_hist_ttl.merge(shp_hist, how='inner', 
                                     left_on=['movetype', 'Lane_ID', 'maxTotal_Loads'],
                                     right_on=['movetype', 'Lane_ID', 'Total_Loads'],
                                     suffixes=('', '_drop'))

[c for c in shp_hist_joined.columns if '_drop' in c]
shp_hist_joined.drop(columns=['Depot_drop', 'Customer_drop', 'Total_Loads', 'Ttl_LH_Cost'], inplace=True)




#%%

# =============================================================================
# Create a workflow to do the following:
#     - Create an "rfq_rate" column in TransportationPolicies
#     - Update rfq_rate with Excel data provided by Dean
#     - Update 'rateused' according to the priority outlined below.
# 
# Rate priority:
#     RFQ => Historical (any rate that isn't 999,999) => Market (should always have a value)
# =============================================================================











#%% Compare original to updated dataframes.
# (Also compare to Alteryx output.)

# =============================================================================
# # Compare differences between dataframes
# df1 = pd.DataFrame(
#     {
#         "col1": ["a", "a", "b", "b", "a"],
#         "col2": [1.0, 2.0, 3.0, np.nan, 5.0],
#         "col3": [1.0, 2.0, 3.0, 4.0, 5.0]
#     },
#     columns=["col1", "col2", "col3"],
# )
# 
# df2 = df1.copy()
# df2.loc[0, 'col1'] = 'c'
# df2.loc[2, 'col3'] = 4.0
# 
# res = df1.compare(df2, result_names=('df1', 'df2'), align_axis=1, keep_equal=False)
# =============================================================================

#%% Reupload data to Cosmic Frog.















