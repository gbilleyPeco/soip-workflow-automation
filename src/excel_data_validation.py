# =============================================================================
# The purpose of this script is to check the Excel input files and validate the data prior to 
# using it in downstream scripts. 
# =============================================================================

import pandas as pd
import numpy as np
import os
import sys

# Add project root to PATH to allow for relative imports. 
ROOT = os.path.abspath(os.path.join('..'))
if ROOT not in sys.path:
    sys.path.append(ROOT)
    
# Import User-Input data.
from user_inputs import SOIP_DEPOT_ASSIGNMENTS_FILENAME, SOIP_OPT_ASSUMPTIONS_FILENAME, \
    SCAC_CARRIER_TYPE_FILENAME, TRANS_RFQ_RATES_FILENAME


def check_for_duplicate_keys(filename, sheetname, df, key_cols):
# =============================================================================
#     This function is called by pull_data_from_excel() and is one of the main validation
#     functions. 
#     Returns 0 if no errors, 1 if errors.
# =============================================================================
    error = 0    
    
    dups = df[df.duplicated(key_cols)][key_cols]
    if not dups.empty:
        print(f"Duplicate values found in '{filename}', '{sheetname}' tab.\n{dups}\n\n")
        error = 1
        
    return error


def check_for_one_to_one(filename, sheetname, df, loc_attributes):
# =============================================================================
#     This function is called by pull_data_from_excel() and is one of the main validation
#     functions.
#     Returns 0 if no errors, > 0 if errors.
# =============================================================================
    error = 0
    
    if loc_attributes is None:
        return error
    
    # For each group of 1:1 attributes...
    for atts in loc_attributes:
        # Filter df for only the attributes we care about, then drop duplicates.
        df_filtered = df[atts].drop_duplicates()
        
        # Check each attribute column in the group.
        for col in df_filtered.columns:
            duplicates = df_filtered[df_filtered.duplicated(col, keep=False)].copy()
            duplicates.sort_values(col, inplace=True)
            if not duplicates.empty:
                error += 1
                print(f"One-to-one rule broken. '{col}' values appear across multiple rows in:\n'{filename}', '{sheetname}' tab.\n{duplicates}\n\n")
   
    return error


def check_for_missing_values(filename, sheetname, df, nonempty_cols):
# =============================================================================
# This function is called by pull_data_from_excel() and is one of the main validation
# functions. 
# Returns 0 if no errors, > 0 if errors.
# =============================================================================
    error = 0
    
    # For each column that is supposed to have no empty rows...
    for col in nonempty_cols:
        if df[col].isna().any():
            error += 1
            print(f"Empty/NaN data found in:\n'{filename}', '{sheetname}' tab.\nColumn '{col}' contains empty or NaN rows.\n\n")
        
    return error

def check_for_only_allowed_values(filename, sheetname, df, allowed_vals):
# =============================================================================
# This function is called by pull_data_from_excel() and is one of the main validation
# functions. 
# Returns 0 if no errors, > 0 if errors.
# =============================================================================
    error = 0
    
    if allowed_vals is None:
        return error
    
    # For each column specified in the disallowed_val dictionary...
    for col in allowed_vals.keys():
        allowed = allowed_vals[col]
        actual  = df[col]
        
        disallowed_values = [actual_item for actual_item in actual if actual_item not in allowed]
        
        if len(disallowed_values) > 0:
            error += 1
            print(f"Values that are not allowed found in:\n'{filename}', '{sheetname}' tab.\nColumn '{col}'.\n{disallowed_values}\n\n")

    return error



def pull_data_from_excel():
# =============================================================================
#     This function reads data from specified Excel files, and stores the resulting dataframes
#     in a dictionary that is returned when the function is called. 
#
#     The paths to the Excel files are stored in the "FILENAME" imports from the user_inputs file.
#     The sheet names read from the excel files are the keys of the excel_info dictionary.
#     The values of the excel_info dictionary contain the path to each Excel file, the number
#     of rows to skip when reading the file, the column names that represent the primary keys
#     of the dataset, and the columns that represent location specific attributes.
#    
#     The primary key columns are converted to strings to avoid errors related to 
#     location codes being stored as numeric and/or strings in Excel.
#     
#     This function also performs validation on each Excel dataset, meant to catch common errors 
#     that happen when storing data in Excel. Specifically:
#         1) Make sure there are no rows with duplicated primary keys.
#         2) Ensure one-to-one relationships between location attributes (codes, names, etc.)
# =============================================================================
    
    excel_info = {
        'Depot Assumptions':{'filename' : SOIP_OPT_ASSUMPTIONS_FILENAME, 
                             'start_row': 2,
                             'key_cols' : ['ModelID'],
                             'loc_attributes' : [['ModelID', 'LocCode', 'Loc Description']],
                             'nonempty_cols' : ['ModelID', 'LocCode', 'Loc Description',
                                                'Type', 'Closed'],
                             'allowed_vals' : None
                             },
                  
        'Renter Assumptions':{'filename' : SOIP_OPT_ASSUMPTIONS_FILENAME, 
                              'start_row' : 3,
                              'key_cols' : ['ModelID'],
                              'loc_attributes' : [['Loc Code', 'ModelID', 'Loc Desc']],
                              'nonempty_cols' : ['Loc Code', 'ModelID', 'Loc Desc', 'Postal', 'City',
                                                 'State', 'Country',
                                                 'Corp Name', 'Corp Code'],
                              'allowed_vals' : None
                              },
                  
        'MultiSource List':{'filename' : SOIP_OPT_ASSUMPTIONS_FILENAME,
                            'start_row' : 1,
                            'key_cols' : ['MoveType', 'CustomerCode', 'DepotCode'],
                            'loc_attributes' : [['CustomerCode', 'CustName'],
                                                ['DepotCode', 'DepotName']],
                            'nonempty_cols' : ['MoveType', 'CustomerCode', 'CustName', 'Cust Zone',
                                               'Cust Region', 'DepotCode', 'DepotName', 'Depot Type',
                                               'Total', 'Pct of Location Volume', 'NbrDepots',
                                               'OK to Include SOIP'],
                            'allowed_vals' : {'OK to Include SOIP': ['YES', 'Yes', 'yes', 'Y', 'y',
                                                                     'NO', 'N', 'no', 'n']}
                            },
                  
        'RenterDistSort Preferred Depot':{'filename' : SOIP_OPT_ASSUMPTIONS_FILENAME,
                                          'start_row' : 0,
                                          'key_cols' : ['Ocode', 'Dcode'],
                                          'loc_attributes' : [['Ocode','O-Name'],
                                                              ['Dcode','D-Name']],
                                          'nonempty_cols' : ['Ocode', 'O-Name', 'Dcode', 'D-Name',
                                                             'Last 90 Days # Loads', 'Trans Comments (Y/N)'],
                                          'allowed_vals' : None
                                          },
                  
        'Depot Assignments':{'filename' : SOIP_DEPOT_ASSIGNMENTS_FILENAME,
                             'start_row' : 0,
                             'key_cols' : ['MoveType', 'Loc Code', 'Default Depot Code'],
                             'loc_attributes' : [['Loc Code', 'Location Name'],
                                                 ['Default Depot Code', 'Default Depot Name']],
                             'nonempty_cols' : ['MoveType', 'Loc Code', 'Location Name', 'Default Depot Code',
                                                'Default Depot Name'],
                             'allowed_vals' : None
                             },
        
        'SCAC Types':{'filename' : SCAC_CARRIER_TYPE_FILENAME,
                      'start_row' : 0,
                      'key_cols' : ['TMS_CarrierSCAC', 'TMS_CarrierName'],
                      'loc_attributes' : [['TMS_CarrierSCAC', 'TMS_CarrierName']],
                      'nonempty_cols' : ['TMS_CarrierSCAC'],
                      'allowed_vals' : {'Carrier_Type': ['CPU', 'Dedicated']}
                      },
        
        'Trans RFQ Rates':{'filename' : TRANS_RFQ_RATES_FILENAME,
                           'start_row' : 0,
                           'key_cols' : ['Lane Name'],
                           'loc_attributes' : None,
                           'nonempty_cols' : ['Lane Name', 'Final Rate Award'],
                           'allowed_vals' : None
                      }
        
        }
        
    # Create a dictionary to store the Excel data.
    data_dict = {}
    
    # Count the errors as we go.
    error_count = 0
    
    # Loop through each Excel dataset and do the following:
    #   1. Read the data.
    #   2. Convert the key columns to string datatypes.
    #   3. Validate that there are not multiple rows with the same primary key.
    #   4. Validate that the location attributes are one-to-one. 
    #      i.e. that a LocCode doesn't have multiple LocDescriptions. 
    #   5. Validate nonempty columns have no empty rows.
    #   6. Validate only allowed values in specified columns.
    
    print('\nReading and validating Excel data...')
    for sheetname, info in excel_info.items():
        filename  = info['filename']
        start_row = info['start_row']
        key_cols  = info['key_cols']
        loc_attributes = info['loc_attributes']
        nonempty_cols  = info['nonempty_cols']
        allowed_vals = info['allowed_vals']
        
        try:
            # 1. Read the data.
            df = pd.read_excel(os.path.join('..', 'data', filename), 
                                                 sheet_name = sheetname,
                                                 skiprows   = start_row)
            # 2. Convert key columns to strings.
            df[key_cols] = df[key_cols].astype(str)
            
            # 3. Validate primary keys are not duplicated.
            error_count += check_for_duplicate_keys(filename, sheetname, df, key_cols)
            
            # 4. Validate location attributes are one-to-one.
            error_count += check_for_one_to_one(filename, sheetname, df, loc_attributes)
            
            # 5. Validate nonempty columns have no empty rows.
            error_count += check_for_missing_values(filename, sheetname, df, nonempty_cols)
            
            # 6. Validate only allowed values in specified columns.
            error_count += check_for_only_allowed_values(filename, sheetname, df, allowed_vals)
            
            data_dict[sheetname] = df
            
        except Exception as e:
            error_count += 1
            print(e)
            pass
    
    if error_count == 0: print('\tDone. No Excel data errors detected!')
    return data_dict, error_count
    
# Testing
#excel_data_, error_count_ = pull_data_from_excel()
