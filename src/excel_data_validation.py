# =============================================================================
# The purpose of this script is to check the Excel input files and validate the data prior to 
# using it in downstream scripts. 
# =============================================================================

import pandas as pd
import os
import sys

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
    
    
def pull_data_from_excel(filename_dict):
    print("\nPulling data from Excel...")
    
    # Create a dictionary to store the Excel data frames.
    data_dict = {}
    
    for sheetname, filename in filename_dict.items():
        print(f'\tReading {sheetname} data from Excel.')
        data_dict[sheetname] = pd.read_excel(os.path.join('..', 'data', filename), sheet_name=sheetname)
        
    return data_dict      
    
# Pull data from Excel.
filename_dict = {'Depot Assumptions':SOIP_OPT_ASSUMPTIONS_FILENAME,
                 'Renter Assumptions':SOIP_OPT_ASSUMPTIONS_FILENAME,
                 'MultiSource List':SOIP_OPT_ASSUMPTIONS_FILENAME,
                 'RenterDistSort Preferred Depot':SOIP_OPT_ASSUMPTIONS_FILENAME,
                 'Depot Assignments':SOIP_DEPOT_ASSIGNMENTS_FILENAME}
excel_data = pull_data_from_excel(filename_dict)

#%% Validate data




'''
1. No rows with duplicate primary keys
'''























