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

# Add project root to PATH to allow for relative imports. 
ROOT = os.path.abspath(os.path.join('..'))
if ROOT not in sys.path:
    sys.path.append(ROOT)

# Import SQL Statements
from sql_statements import tbl_tab_Location_sql, lane_attributes_issues_sql, \
    transport_rates_hist_load_counts_sql, transport_rates_hist_costs_sql, transport_load_size_sql
    
# Import User-Input data.
from user_inputs import USER_NAME, APP_KEY, DB_NAME, SOIP_DEPOT_ASSIGNMENTS_FILENAME, \
    SOIP_OPT_ASSUMPTIONS_FILENAME #, RepairCapacityNotes, MinInventoryNotes, DepotCapacityNotes, \
    #BeginningInvNotes, ReturnsProductionNotes, CustomerDemandNotes

#%% Define functions to pull data.

def pull_data_from_cosmic_frog(USER_NAME, APP_KEY, DB_NAME, tables_we_want):
    # Note: This syntax is compatible with SQLAlchemy 2.0.
    print('Pulling data from Cosmic Frog...')
    
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
    print("Pulling data from PECO's data warehouse...")
    connection_string = 'DRIVER={SQL Server};SERVER=10.0.17.62;;UID=tabconnection;PWD=password'
    connection_url = sal.engine.URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = sal.create_engine(connection_url)
    
    # Create a dictionary to store the cosmic frog data frames.
    data_dict = {}
    
    for name, sql_statement in sql_name_dict.items():
        with engine.connect() as conn:
            trans = conn.begin()
            print(f'\tExecuting SQL statement: {name}')
            data_dict[name] = pd.read_sql(sal.text(sql_statement), con=conn)
            trans.commit()
    
    return data_dict

def pull_data_from_excel(filename_dict):
    print("Pulling data from Excel...")
    
    # Create a dictionary to store the Excel data frames.
    data_dict = {}
    
    for sheetname, filename in filename_dict.items():
        print(f'\tReading {sheetname} data from Excel.')
        data_dict[sheetname] = pd.read_excel(os.path.join('..', 'data', filename), sheet_name=sheetname)
        
    return data_dict       

#%% Pull Data

# Pull data from Cosmic Frog.
tables_we_want  = ['customerfulfillmentpolicies',
                   'customers',
                   'facilities',
                   'groups',
                   'inventoryconstraints',
                   'inventorypolicies',
                   'productionconstraints',
                   'productionpolicies',
                   'replenishmentpolicies',
                   'transportationpolicies',
                   'warehousingpolicies',]
cosmic_frog_data = pull_data_from_cosmic_frog(USER_NAME, APP_KEY, DB_NAME, tables_we_want)

# Pull data from PECO's data warehouse.
sql_name_dict = {'tbl_tab_Location':tbl_tab_Location_sql,
                 'lane_attributes_issues':lane_attributes_issues_sql,
                 'transport_rates_hist_load_counts':transport_rates_hist_load_counts_sql,
                 'transport_rates_hist_costs':transport_rates_hist_costs_sql,
                 'transport_load_size':transport_load_size_sql}
data_warehouse_data = pull_data_from_data_warehouse(sql_name_dict)

# Pull data from Excel.
filename_dict = {'Depot Assumptions':SOIP_OPT_ASSUMPTIONS_FILENAME,
                 'Renter Assumptions':SOIP_OPT_ASSUMPTIONS_FILENAME,
                 'MultiSource List':SOIP_OPT_ASSUMPTIONS_FILENAME,
                 'RenterDistSort Preferred Depot':SOIP_OPT_ASSUMPTIONS_FILENAME,
                 'Depot Assignments':SOIP_DEPOT_ASSIGNMENTS_FILENAME}
excel_data = pull_data_from_excel(filename_dict)

#%% Format Excel data

# Depot Assumptions. Set the first row to be the column names and drop that row.
excel_data['Depot Assumptions'].columns = excel_data['Depot Assumptions'].loc[1]
excel_data['Depot Assumptions'].drop([0,1], axis='index', inplace=True)
excel_data['Depot Assumptions'].reset_index(drop=True, inplace=True)

# Renter Assumptions. Set the third row to be the column names and drop rows 1-3.
excel_data['Renter Assumptions'].columns = excel_data['Renter Assumptions'].loc[2]
excel_data['Renter Assumptions'].drop([0,1,2], axis='index', inplace=True)
excel_data['Renter Assumptions'].reset_index(drop=True, inplace=True)

#%% Update Cosmic Frog data.

customerfulfillmentpolicies = cosmic_frog_data['customerfulfillmentpolicies']
customers = cosmic_frog_data['customers']
facilities = cosmic_frog_data['facilities']
groups = cosmic_frog_data['groups']
inventoryconstraints = cosmic_frog_data['inventoryconstraints']
inventorypolicies = cosmic_frog_data['inventorypolicies']
productionconstraints = cosmic_frog_data['productionconstraints']
productionpolicies = cosmic_frog_data['productionpolicies']
replenishmentpolicies = cosmic_frog_data['replenishmentpolicies']
transportationpolicies = cosmic_frog_data['transportationpolicies']
warehousingpolicies = cosmic_frog_data['warehousingpolicies']



##### Update Customer Fulfillment Policies

#010 - Depot Costs and Attributes v2

#015 - Set SOIP Solve Flag

#020 - Lane Attributes

#030 - NPD Percentage Penalty

#060 - Transportation Rates Historical

#090 - Flag Multi-Source Options





##### Update Customers

#015 - Set SOIP Solve Flag

#017 - Issue and Return Location Details

#070 - Trans Load Size





##### Update Facilities

#010 - Depot Costs and Attributes v2
cols = ['ModelID', 'Type', 'Closed', 'Heat Treat', 'Fixed Upd']
fac = excel_data['Depot Assumptions'][cols].copy()

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

fac[fac.index.duplicated()]

facilities_test = facilities.copy()

fac.index = fac['ModelID']
facilities_test.index = facilities_test['facilityname']

facilities_test.update(fac)


facilities_test_ = facilities_test[['fixedoperatingcost', 'heat_treatment_rqmt', 'depottype', 'closed']]
fac_ = facilities[['fixedoperatingcost', 'heat_treatment_rqmt', 'depottype', 'closed']]





#015 - Set SOIP Solve Flag

#017 - Issue and Return Location Details

#070 - Trans Load Size





##### Update Groups

#090 - Flag Multi-Source Options





##### Update Inventory Constraints

#010 - Depot Costs and Attributes v2





##### Update Inventory Policies

#010 - Depot Costs and Attributes v2





##### Update Production Constraints

#010 - Depot Costs and Attributes v2





##### Update Production Policies

#010 - Depot Costs and Attributes v2





##### Update Replenishment Policies

#010 - Depot Costs and Attributes v2

#015 - Set SOIP Solve Flag

#020 - Lane Attributes

#060 - Transportation Rates Historical

#090 - Flag Multi-Source Options

#100 - Transfer Matrix Update

#110 - Renter Dist Sort Pref Depot





##### Update Transportation Policies

#020 - Lane Attributes

#060 - Transportation Rates Historical

#070 - Trans Load Size





##### Update Warehousing Policies

#010 - Depot Costs and Attributes v2


#%% Reupload data to Cosmic Frog.


