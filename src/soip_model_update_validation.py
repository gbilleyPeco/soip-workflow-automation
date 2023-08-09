# =============================================================================
# The purpose of this script is to make it easier to compare differences between two Cosmic Frog models.
# This is meant to help us validate our ETL code associated with the monthly SOIP process.
# =============================================================================

############################################ USER INPUTS ###########################################
USER_NAME = 'graham.billey'
APP_KEY = 'op_NWQ3YjQ0NjktNTBjOC00M2JkLWE4NWEtNjM1NDBmODA5ODEw'
databases = ['PECO 2023-08 SOIP Opt copy 1',
             'PECO 2023-08 SOIP Opt copy 2']
# NOTE: Currently the code isn o

tables_we_want  = ['customerfulfillmentpolicies',
                   'customers',
                   'facilities',
                   'groups',
                   'inventoryconstraints',
                   'inventorypolicies',
                   'periods',   # NOTE: The model update code doesn't change the Periods table. This is for testing.
                   'productionconstraints',
                   'productionpolicies',
                   'replenishmentpolicies',
                   'transportationpolicies',
                   'warehousingpolicies',
                   ]


########################################## IMPORTS, SETUP ##########################################
# Imports
import sqlalchemy as sal
import pandas as pd
#import numpy as np
import warnings
import logging
import os
from datetime import date
from optilogic import pioneer

# Create output data directory.
OUTPUT_FOLDER = os.path.join('..', 'validation')
TODAY = pd.to_datetime(date.today()).strftime('%Y-%m-%d')
OUTPUT_LOCATION = os.path.join(OUTPUT_FOLDER, TODAY)
if not os.path.exists(OUTPUT_LOCATION):
    os.mkdir(OUTPUT_LOCATION)

# Names of output validation files.
DUP_INDEX_FILENAME = 'duplicate_primary_keys.xlsx'
COMPARE_FILENAME = 'dataframe_comparisons.xlsx'

# Logging
logging.basicConfig(filename=os.path.join(OUTPUT_FOLDER, TODAY, 'output.log'), level=logging.DEBUG)

############################################# PULL DATA ############################################
def pull_data_from_cosmic_frog(USER_NAME, APP_KEY, DB_NAME, tables_we_want):
    # Note: This syntax is compatible with SQLAlchemy 2.0.
    print('\nPulling data from Cosmic Frog...')
    
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")     # Ignore the Cosmic Frog API warning.
    
        # Code that makes connection to the Cosmic Frog database.
        api = pioneer.Api(auth_legacy = False, un=USER_NAME, appkey=APP_KEY)
        connection_str = api.sql_connection_info(DB_NAME)
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

# Create a dictionary to store the data.
cf_data = {}
           
# Pull data from Cosmic Frog.
for database in databases:
    cf_data[database] = pull_data_from_cosmic_frog(USER_NAME, APP_KEY, database, tables_we_want)

###################################### BEGIN DATA COMPARISON  ######################################

primary_keys = {'customerfulfillmentpolicies':['customername', 'productname', 'sourcename'],
                'customers':['customername'],
                'facilities':['facilityname'],
                'groups':['groupname', 'grouptype', 'membername'],
                'inventoryconstraints':['facilityname','facilitynamegroupbehavior','productname',
                                        'productnamegroupbehavior','periodname',
                                        'periodnamegroupbehavior','constrainttype',
                                        'constraintvalue','constraintvalueuom',
                                        'consideredinventory'],
                'inventorypolicies':['facilityname', 'productname'],
                'periods':['periodname'],   # NOTE: The model update code doesn't change the Periods table. This is for testing.
                'productionconstraints':['facilityname','facilitynamegroupbehavior','productname',
                                         'productnamegroupbehavior','periodname',
                                         'periodnamegroupbehavior','bomname','bomnamegroupbehavior',
                                         'processname','processnamegroupbehavior','constrainttype',
                                         'constraintvalue','constraintvalueuom'],
                'productionpolicies':['facilityname', 'productname', 'bomname', 'processname'],
                'replenishmentpolicies':['facilityname', 'productname', 'sourcename'],
                'transportationpolicies':['originname', 'destinationname', 'productname', 'modename'],
                'warehousingpolicies':['facilityname', 'productname'],
                   }

# Create a dictionary of tuples containing the dataframes we want to compare.
paired_tables = dict()
for table_name in tables_we_want:
    paired_tables[table_name] = (cf_data[databases[0]][table_name], cf_data[databases[1]][table_name])


def col_names(df1, df2):
# =============================================================================
#     This function looks for columns in df1 that are not in df2, and columns
#     in df2 that are not in df1.
#     
#     Returns three values:
#         The total number of columns that differ across both dataframes.
#         A list of the columns in df1 that are not in df2.
#         A list of the columns in df2 that are not in df1.
# =============================================================================
    c1_not_c2 = [c for c in df1.columns if c not in df2.columns]
    c2_not_c1 = [c for c in df2.columns if c not in df1.columns]
    diff_count = len(c1_not_c2) + len(c2_not_c1)
    return diff_count, c1_not_c2, c2_not_c1

def row_counts(df1, df2):
# =============================================================================
# This function returns the differences in row counts between df1 and df2. Although this check is 
# trivial and could be implemented in the main code without being defined externally, I 
# left it as a function in case we want to add additional logic to it later.
# =============================================================================
    diff_count = len(df1) - len(df2)
    return diff_count

def same_index(df1, df2, keys):
# =============================================================================
# In order to use DataFrame.compare(), the indexex of both dataframes have to be the same. 
# To implement DataFrame.compare(), we set the index to be the primary keys of each dataframe.
# The purpose of this function is to check if the indexes of both dataframes are the same, and to 
# output the differences (if any) as DataFrames for comparison.
# =============================================================================
    with warnings.catch_warnings():
        # NOTE: This is meant to suppress the following warning. 
        # FutureWarning: In a future version, the Index constructor will not infer numeric dtypes 
        # when passed object-dtype sequences (matching Series behavior)
        warnings.simplefilter(action='ignore', category=FutureWarning)
        merged = df1[keys].merge(df2[keys], how='outer', indicator=True)
        
    i1_not_i2 = merged[merged['_merge']=='left_only']
    i2_not_i1 = merged[merged['_merge']=='right_only']
    diff_count = len(i1_not_i2) + len(i2_not_i1)
    return diff_count, i1_not_i2, i2_not_i1

# Prep the dataframe for comparison
def prep_for_compare(df, keys):
# =============================================================================
# If two dataframes have the same index, there may still be additional formatting that needs to be 
# done in order for DataFrame.compare() to not flag false positives. 
# 
# From inspecting the output of DataFrame.compare(), there are false positives in numeric columns. 
# All columns are stored as strings in the Cosmic Frog Postgres database. Many columns in df1 are 
# reading in # as "integer-strings" (560), and many in df2 are "float-strings" (560.0) i.e. they 
# were stored as integers or floats originally prior to being uploaded to CF. 
# 
# For our purposes, we do not want to force the upstream ETL process to specify datatypes for all
# numeric columns, especially since the type isn't preserved in the Postgres database. Instead, 
# let's try to convert all columns to float, ignoring ones that can't be converted (as they would
# have rows with strings, like 'R_....' facility names.) Then round the data to X decimal places 
# (2?), and compare dataframes again.
# 
# When testing this, another problem was found. Sometimes, empty strings, i.e. '', are randomly 
# found in numeric columns, instead of NoneType objects. So before converting columns to floats, we 
# will # first replace all occurrences of empty strings witn NoneType objects, in all cells of both
# DataFrames. 
# =============================================================================
    
    # Replace all empty strings with None.
    df = df.replace({'':None})
    
    # Convert numeric columns to float and round to 2 decimal places.
    for c in df.columns:
        # Try to convert into a series of type float.
        try:
            df[c] = df[c].astype(float).round(2).fillna(0)
        except:
            pass
        
    # Set index based on primary keys, and sort on index.
    df = df.set_index(keys).sort_index().sort_index(axis=1)
    return df




def main():
    comparison_dict = {}
    
    for table_name, dataframe_tuple in paired_tables.items():
        print(f'Comparing {table_name} dataframes.')
        logging.info(f'Comparing {table_name} dataframes.')
        
        df1 = dataframe_tuple[0]
        df2 = dataframe_tuple[1]

        # Check for different column names.
        print('\t\tChecking col_names...')
        cn = col_names(df1, df2)
        if cn[0]:   # If there are different column names.
            logging.info('\tDifferent column names were found.')
            logging.info(f'\tdf1 contains : {cn[1]}')
            logging.info(f'\tdf2 contains : {cn[2]}')
            continue
        
        # Check for different row counts.
        print('\t\tChecking row_counts...')
        rc = row_counts(df1, df2)
        if rc:     # If there are different row counts
            logging.info('\tDifferent row counts were found.')
            logging.info(f'\tdf1 has {len(df1)} rows, df2 has {len(df2)} rows.')
            continue
        
        # Check for different indexes based on the primary key of each table.
        print('\t\tChecking same_index...')
        keys = primary_keys[table_name]
        si = same_index(df1, df2, keys)
        if si[0]:   # If there are different primary keys.     
            logging.info('\tDifferent values for primary keys were found.')
            # Save dataframes as Excel files. 
            with pd.ExcelWriter(os.path.join(OUTPUT_LOCATION, DUP_INDEX_FILENAME),
                                mode='a') as writer:
                si[1].to_excel(writer, sheet_name=f'{table_name}_0')
                si[2].to_excel(writer, sheet_name=f'{table_name}_1')
            continue
        
        # Now use DataFrame.compare()
        print('\tInitial checks passed.')
        print('\tPreparing dataframes for df.compare()...')
        df1_ = prep_for_compare(df1, keys)
        df2_ = prep_for_compare(df2, keys)
        print('\tComparing dataframes...')
        if df1_.equals(df2_): 
            print(f'{table_name} dataframes are the same.')
        else: 
            diff = df1_.compare(df2_)
            with pd.ExcelWriter(os.path.join(OUTPUT_LOCATION, DUP_INDEX_FILENAME),
                                mode='a') as writer:
                diff.to_excel(writer, sheet_name=f'{table_name}')
            comparison_dict[table_name] = diff
            
        print('\tDone.\n')
        
    return comparison_dict
        


#%%  Test main()

res = main()








#%% DataFrame.compare() examples.







#%% Misc Testing


# Why will the column 'returnqty' not convert to float? Some rows must contain a 
# string that can't be converted.
# Found it - one row contained the empty string ''.
    
# =============================================================================
# t2 = convert_numeric_cols(df1_).compare(convert_numeric_cols(df2_))
# =============================================================================

# =============================================================================
# r = 'R_CANE1E3Y9_70599'
# r_ =df1_.loc[r]
# =============================================================================


# =============================================================================
# rows = []
# for row in df1_.index:
#     val = df1_.loc[row, 'returnqty']
#     if val is not None:
#         try:
#             float(df1_.loc[row, 'returnqty'])
#         except:
#             #print(row)
#             rows.append(row)
# 
# 
# df1_.loc[rows[0], 'returnqty']
# 
# df_test = pd.DataFrame(
#     {
#         "col1": ["a", "a", "b", "b", "a"],
#         "col2": ['', 2.0, 3.0, np.nan, 5.0],
#         "col3": [1.0, 2.0, 3.0, 4.0, 5.0]
#     },
#     columns=["col1", "col2", "col3"],
# )
# 
# df_test_ = df_test.replace({'':None})
# =============================================================================



# =============================================================================
# DataFrame.compare() examples.
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
# res_1_2 = df1.compare(df2, result_names=('df1', 'df2'), align_axis=1, keep_equal=False)
# 
# 
# df3 = pd.DataFrame(
#     {
#         "col1": ["a", "a", "b", "b", "a"],
#         "col2": [2.0, 1.0, 3.0, np.nan, 5.0],
#         "col3": [2.0, 1.0, 3.0, 4.0, 5.0]
#     },
#     columns=["col1", "col2", "col3"],
# )
# 
# res_1_3 = df1.compare(df3, result_names=('df1', 'df2'), align_axis=1, keep_equal=False)
# 
# df1_s = df1.sort_values(list(df1.columns)).reset_index(drop=True)
# df3_s = df3.sort_values(list(df3.columns)).reset_index(drop=True)
# res_1_3_sorted = df1_s.compare(df3_s, result_names=('df1', 'df2'), align_axis=1, keep_equal=False)  # Works
# 
# 
# df1_s_i = df1.sort_values(list(df1.columns), ignore_index=True)
# df3_s_i = df3.sort_values(list(df3.columns), ignore_index=True)
# res_1i_3i_sorted = df1_s_i.compare(df3_s_i, result_names=('df1', 'df2'), align_axis=1, keep_equal=False)  # Works
# 
# 
# 
# p1 = cf_data['PECO 2023-08 SOIP Opt copy 1']['periods']
# p2 = cf_data['PECO 2023-08 SOIP Opt copy 2']['periods'] # Should be the same
# delta = p1.compare(p2) # Pass. Returns an empty DataFrame.
# =============================================================================






