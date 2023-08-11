# =============================================================================
# The purpose of this script is to make it easier to compare differences between two Cosmic Frog models.
# This is meant to help us validate our ETL code associated with the monthly SOIP process.
# =============================================================================

########################################## IMPORTS, SETUP ##########################################
# Imports
import sqlalchemy as sal
import pandas as pd
import warnings
import logging
import os
import sys
import time
from datetime import date
from optilogic import pioneer

t0 = time.time()

# Add project root to PATH to allow for relative imports. 
ROOT = os.path.abspath(os.path.join('..'))
if ROOT not in sys.path:
    sys.path.append(ROOT)

from user_inputs import USER_NAME, APP_KEY, INPUT_DB_NAME, OUTPUT_DB_NAME

databases = [INPUT_DB_NAME, OUTPUT_DB_NAME]
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

# Create output data directory.
OUTPUT_FOLDER = os.path.join('..', 'validation')
TODAY = pd.to_datetime(date.today()).strftime('%Y-%m-%d')
OUTPUT_LOCATION = os.path.join(OUTPUT_FOLDER, TODAY)
if not os.path.exists(OUTPUT_LOCATION):
    os.mkdir(OUTPUT_LOCATION)

# Names of output validation files.
DUP_INDEX_FILENAME = 'duplicate_primary_keys.xlsx'
#COMPARE_FILENAME = 'dataframe_comparisons.xlsx'
  
# Logging
logging.basicConfig(filename=os.path.join(OUTPUT_FOLDER, TODAY, 'output.log'), level=logging.INFO)

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
                                        'constraintvalueuom', #'constraintvalue',
                                        'consideredinventory'],
                'inventorypolicies':['facilityname', 'productname'],
                'periods':['periodname'],   # NOTE: The model update code doesn't change the Periods table. This is for testing.
                'productionconstraints':['facilityname','facilitynamegroupbehavior','productname',
                                         'productnamegroupbehavior','periodname',
                                         'periodnamegroupbehavior','bomname','bomnamegroupbehavior',
                                         'processname','processnamegroupbehavior','constrainttype',
                                         'constraintvalueuom', #'constraintvalue',
                                         ],
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
# In order to use DataFrame.compare(), the indexes of both dataframes have to be the same. 
# To implement DataFrame.compare(), we set the index to be the primary keys of each dataframe.
# The purpose of this function is to check if the indexes of both dataframes are the same, and to 
# output the differences (if any) as DataFrames for comparison.
#
# After testing, we found instances in inventoryconstraints and productionconstraints where
# index columns were numeric in nature, but sometimes stored with or without a decimal. (See the 
# notes for prep_for_compare()). We need to apply the same conversions to the 
# dataframes in this function, without setting the index columns. 
# =============================================================================
    df1 = prep_for_compare(df1, keys).reset_index()
    df2 = prep_for_compare(df2, keys).reset_index()
    
    with warnings.catch_warnings():
        # NOTE: This is meant to suppress the following warning. 
        # FutureWarning: In a future version, the Index constructor will not infer numeric dtypes 
        # when passed object-dtype sequences (matching Series behavior)
        warnings.simplefilter(action='ignore', category=FutureWarning)        
        merged = df1[keys].merge(df2[keys], how='outer', indicator=True)
        
    i1_not_i2 = merged[merged['_merge']=='left_only']
    i2_not_i1 = merged[merged['_merge']=='right_only']
    diff_index_count = len(i1_not_i2) + len(i2_not_i1)
    return diff_index_count, i1_not_i2, i2_not_i1

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
        
        ##### THIS SECTION HERE ONLY FOR TESTING #####
        if table_name not in ['replenishmentpolicies']:
            continue
        ##### THIS SECTION HERE ONLY FOR TESTING #####
        
        print(f'\nComparing {table_name} dataframes.')  
        print('\tPerforming initial checks.')
        
        logging.info('\n\n--------------------------------------------------------------------\n\n')
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
        else:
            logging.info('\tPASS.')
        
        # Check for different row counts.
        print('\t\tChecking row_counts...')
        rc = row_counts(df1, df2)
        if rc:     # If there are different row counts
            logging.info('\tDifferent row counts were found.')
            logging.info(f'\tdf1 has {len(df1)} rows, df2 has {len(df2)} rows.')
            continue
        else:
            logging.info('\tPASS.')
        
        # Check for different indexes based on the primary key of each table.
        print('\t\tChecking same_index...')
        keys = primary_keys[table_name]
        diff_index_count, df1_only, df2_only = same_index(df1, df2, keys)
        if diff_index_count:   # If there are different primary keys.  
        
            logging.info('\tDifferent values for primary keys were found.')
            # Save dataframes as Excel files.                
            excel_filename = os.path.join(OUTPUT_LOCATION, DUP_INDEX_FILENAME)
            if os.path.exists(excel_filename):
                with pd.ExcelWriter(excel_filename,mode='a', if_sheet_exists='replace') as writer:
                    df1_only.to_excel(writer, sheet_name=f'{table_name}_0')
                    df2_only.to_excel(writer, sheet_name=f'{table_name}_1')
            else:
                with pd.ExcelWriter(excel_filename,mode='w') as writer:
                    df1_only.to_excel(writer, sheet_name=f'{table_name}_0')
                    df2_only.to_excel(writer, sheet_name=f'{table_name}_1')
        
        else:
            logging.info('\tPASS.')
        
        # Prep DataFrames to later use DataFrame.compare()
        print('\tInitial checks passed.')
        print('\tPreparing dataframes for df.compare()...')
        tc1 = time.time()
        
        df1_ = prep_for_compare(df1, keys)
        df2_ = prep_for_compare(df2, keys)
        
        tc2 = time.time()
        mins = round((tc2-tc1)/60,1)
        logging.info(f'\tPrepping dataframes for compare took {mins} minutes.')
        print(f'\tPrepping dataframes for compare took {mins} minutes.')
        
        # Use DataFrame.compare()
        print('\tComparing dataframes...')
        if df1_.equals(df2_): 
            print(f'{table_name} dataframes are the same.')
            logging.info(f'\t{table_name} dataframes are the same.')
        else: 
            tc3 = time.time()
            # Drop rows in df1_ and df2_ that have primary keys unique to that dataframe. 
            # Keys must be equal to use df.compare().
            # The keys that are unique to each dataframe are saved in the 'duplicate_primary_keys.xlsx' file.
            df1_ = df1_.drop(df1_only.set_index(keys).index)
            df2_ = df2_.drop(df2_only.set_index(keys).index)
            diff = df1_.compare(df2_)
            
            tc4 = time.time()
            mins = round((tc4-tc3)/60,1)
            logging.info(f'\tComparing dataframes took {mins} minutes. {df1_.shape} rows, cols.')
            print(f'\tComparing dataframes took {mins} minutes. {df1_.shape} rows, cols.') 
            
            tc5 = time.time()
            print('\tExporting comparison to Excel...')
            excel_filename = os.path.join(OUTPUT_LOCATION, f'{table_name}.xlsx')
            with pd.ExcelWriter(excel_filename,mode='w') as writer:
                    diff.to_excel(writer)
            
            tc6 = time.time()
            mins = round((tc6-tc5)/60,1)
            logging.info(f'\tExporting comparison to Excel took {mins} minutes. {diff.shape} rows, cols.')
            print(f'\tExporting comparison to Excel took {mins} minutes. {diff.shape} rows, cols.') 
            
            comparison_dict[table_name] = diff
  
        print('\tDone.\n')
        
    return comparison_dict


#%%  Run comparison function.

res = main()
t1=time.time()
print(f'\nDone. Took {round((t1-t0)/60)} minutes to run.')

#%% Test D\dropping rows with unique primary keys on both dataframes, then compare.

# =============================================================================
# def main_test():
#     comparison_dict = {}
#     
#     for table_name, dataframe_tuple in paired_tables.items():
#         
#         ##### THIS SECTION HERE ONLY FOR TESTING #####
#         if table_name not in ['groups', 'replenishmentpolicies']:
#             continue
#         ##### THIS SECTION HERE ONLY FOR TESTING #####
#         
#         print(f'\nComparing {table_name} dataframes.')  
#         print('\tPerforming initial checks.')
#         
#         df1 = dataframe_tuple[0]
#         df2 = dataframe_tuple[1]
# 
#         # Check for different column names.
#         print('\t\tChecking col_names...')
#         cn = col_names(df1, df2)
#         if cn[0]:   # If there are different column names.
#             continue
#         
#         # Check for different row counts.
#         print('\t\tChecking row_counts...')
#         rc = row_counts(df1, df2)
#         if rc:     # If there are different row counts
#             continue
#         
#         # Check for different indexes based on the primary key of each table.
#         print('\t\tChecking same_index...')
#         keys = primary_keys[table_name]
#         diff_index_count, df1_only, df2_only = same_index(df1, df2, keys)
#         if diff_index_count:   # If there are different primary keys.  
#             # Save dataframes as Excel files.                
#             excel_filename = os.path.join(OUTPUT_LOCATION, DUP_INDEX_FILENAME)
#             if os.path.exists(excel_filename):
#                 with pd.ExcelWriter(excel_filename,mode='a', if_sheet_exists='replace') as writer:
#                     df1_only.to_excel(writer, sheet_name=f'{table_name}_0')
#                     df2_only.to_excel(writer, sheet_name=f'{table_name}_1')
#             else:
#                 with pd.ExcelWriter(excel_filename,mode='w') as writer:
#                     df1_only.to_excel(writer, sheet_name=f'{table_name}_0')
#                     df2_only.to_excel(writer, sheet_name=f'{table_name}_1')
#         
#         # Prep DataFrames to later use DataFrame.compare()
#         print('\tPreparing dataframes for df.compare()...')
#         df1_ = prep_for_compare(df1, keys)
#         df2_ = prep_for_compare(df2, keys)
# 
#         # Use DataFrame.compare()
#         print('\tComparing dataframes...')
#         if df1_.equals(df2_): 
#             print(f'{table_name} dataframes are the same.')
# 
#         else: 
#             # Drop rows in df1_ and df2_ that have primary keys unique to that dataframe. 
#             # Keys must be equal to use df.compare().
#             # The keys that are unique to each dataframe are saved in the 'duplicate_primary_keys.xlsx' file.
#             df1_ = df1_.drop(df1_only.set_index(keys).index)
#             df2_ = df2_.drop(df2_only.set_index(keys).index)
#             diff = df1_.compare(df2_)
# 
#             diff = df1_.compare(df2_)
#             
#             print('\tExporting comparison to Excel...')
#             excel_filename = os.path.join(OUTPUT_LOCATION, f'{table_name}.xlsx')
#             with pd.ExcelWriter(excel_filename,mode='w') as writer:
#                     diff.to_excel(writer)
#                         
#             comparison_dict[table_name] = diff
#   
#         print('\tDone.\n')
#         
#     return comparison_dict
# 
# res = main_test()
# =============================================================================




def same_index_test(df1, df2, keys):
# =============================================================================
# In order to use DataFrame.compare(), the indexes of both dataframes have to be the same. 
# To implement DataFrame.compare(), we set the index to be the primary keys of each dataframe.
# The purpose of this function is to check if the indexes of both dataframes are the same, and to 
# output the differences (if any) as DataFrames for comparison.
# 
# After testing, we found instances in inventoryconstraints and productionconstraints where
# index columns were numeric in nature, but sometimes stored with or without a decimal. (See the 
# notes for prep_for_compare()). We need to apply the same conversions to the 
# dataframes in this function. 
# 
# After testing, we found instances in replenishmentpolicies where rows with the same primary
# key were duplicated, with different values in some columns. To compare the dataframes, we 
# need to have identically-labelled dataframes, and these duplicated rows prevent the dataframes
# from having the same index, even after dropping rows where the keys are unique to one dataframe.
# 
# This function therefore needs to have the following outputs:
#     diff_index_count : the number of indices in df1 not in df2, plus the number in df2 not in df1.
#                        This is just used as a simple check.
#     df1_only         : The indices only in df1, and not in df2
#     df2_only         : The indices only in df2, and not in df1
#     df1_dups         : The subset of df1 containing all rows with duplicated indices
#     df2_dups         : The subset of df2 containing all rows with duplicated indices
#     df1_no_dups      : The subset of df1 without any of the rows in df1_dups
#     df2_no_dups      : The subset of df2 without any of the rows in df1_dups
# =============================================================================
    
    df1 = prep_for_compare(df1, keys).reset_index()
    df2 = prep_for_compare(df2, keys).reset_index()
    
    with warnings.catch_warnings():
        # NOTE: This is meant to suppress the following warning. 
        # FutureWarning: In a future version, the Index constructor will not infer numeric dtypes 
        # when passed object-dtype sequences (matching Series behavior)
        warnings.simplefilter(action='ignore', category=FutureWarning)        
        merged = df1[keys].merge(df2[keys], how='outer', indicator=True)
        
    df1_only = merged[merged['_merge']=='left_only']
    df2_only = merged[merged['_merge']=='right_only']
    diff_index_count = len(df1_only) + len(df2_only)
    
    return diff_index_count, df1_only, df2_only


#################################### Inside of Main()
table_name = 'replenishmentpolicies'
dataframe_tuple = paired_tables[table_name]
df1 = dataframe_tuple[0]
df2 = dataframe_tuple[1]

# Check for different indexes based on the primary key of each table.
keys = primary_keys[table_name]
#diff_index_count, df1_only, df2_only = same_index_test(df1, df2, keys)

########### same_index_test ###########
df1_a = prep_for_compare(df1, keys)
df2_a = prep_for_compare(df2, keys)


df1_dups    = df1_a[ df1_a.index.duplicated(keep=False)]
df1_no_dups = df1_a[~df1_a.index.duplicated(keep=False)]

df2_dups    = df2_a[ df2_a.index.duplicated(keep=False)]
df2_no_dups = df2_a[~df2_a.index.duplicated(keep=False)]

df1_a = df1_a.reset_index()


with warnings.catch_warnings():
    # NOTE: This is meant to suppress the following warning. 
    # FutureWarning: In a future version, the Index constructor will not infer numeric dtypes 
    # when passed object-dtype sequences (matching Series behavior)
    warnings.simplefilter(action='ignore', category=FutureWarning)        
    merged = df1_a[keys].merge(df2_a[keys], how='outer', indicator=True)
    
df1_only = merged[merged['_merge']=='left_only']
df2_only = merged[merged['_merge']=='right_only']
diff_index_count = len(df1_only) + len(df2_only)

########### same_index_test ###########

if diff_index_count:   # If there are different primary keys.  
    # Save dataframes as Excel files.                
    excel_filename = os.path.join(OUTPUT_LOCATION, DUP_INDEX_FILENAME)
    if os.path.exists(excel_filename):
        with pd.ExcelWriter(excel_filename,mode='a', if_sheet_exists='replace') as writer:
            df1_only.to_excel(writer, sheet_name=f'{table_name}_0')
            df2_only.to_excel(writer, sheet_name=f'{table_name}_1')
    else:
        with pd.ExcelWriter(excel_filename,mode='w') as writer:
            df1_only.to_excel(writer, sheet_name=f'{table_name}_0')
            df2_only.to_excel(writer, sheet_name=f'{table_name}_1')
    

# Prep DataFrames to later use DataFrame.compare()
print('\tPreparing dataframes for df.compare()...')
df1_ = prep_for_compare(df1, keys)
df2_ = prep_for_compare(df2, keys)


# Use DataFrame.compare()
print('\tComparing dataframes...')
if df1_.equals(df2_): 
    print(f'{table_name} dataframes are the same.')

else: 
    # Drop rows in df1_ and df2_ that have primary keys unique to that dataframe. 
    # Keys must be equal to use df.compare().
    # The keys that are unique to each dataframe are saved in the 'duplicate_primary_keys.xlsx' file.
    
    # Why did this not result in two identically-labelled dataframes?
    
    df1_test = df1_.drop(df1_only.set_index(keys).index)
    df2_test = df2_.drop(df2_only.set_index(keys).index)
    diff = df1_test.compare(df2_test)
    
    
    t1 = df1_test[df1_test.index.duplicated(keep=False)]
    t2 = df2_test[df2_test.index.duplicated(keep=False)]
    
    
    if diff.empty:
        print('\tNo differences between dataframes after removing unique keys.')
        logging.info('\tNo differences between dataframes after removing unique keys.')
        
    else:
        print('\tExporting comparison to Excel...')
        excel_filename = os.path.join(OUTPUT_LOCATION, f'{table_name}.xlsx')
        with pd.ExcelWriter(excel_filename,mode='w') as writer:
                diff.to_excel(writer)
                    

print('\tDone.\n')

diff.empty


