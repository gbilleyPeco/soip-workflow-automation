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

####################### BEGIN USER INPUTS #######################
USER_NAME = 'graham.billey'
APP_KEY = 'op_NWQ3YjQ0NjktNTBjOC00M2JkLWE4NWEtNjM1NDBmODA5ODEw'
DB_NAME = 'Testing - PECO 2023-08 SOIP Opt' # Cosmic Frog Model Name
######################## END USER INPUTS ########################

# Imports
import sqlalchemy as sal
import pandas as pd
import warnings
from optilogic import pioneer

#%% Pull Data from Cosmic Frog.

def pull_data_from_cosmic_frog(USER_NAME, APP_KEY, DB_NAME, tables_we_want):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")     # Ignore the Cosmic Frog API warning.
    
        # Code that makes connection to the Cosmic Frog database specified at the user input above
        api = pioneer.Api(auth_legacy = False, un=USER_NAME, appkey=APP_KEY)
        connection_str = api.sql_connection_info(DB_NAME)
        connection_string = 'postgresql://'+connection_str['raw']['user']+':'+connection_str['raw']['password']+'@'+connection_str['raw']['host']+':'+str(connection_str['raw']['port'])+'/'+connection_str['raw']['dbname']+'?sslmode=require'
        engine = sal.create_engine(connection_string)
        insp = sal.inspect(engine)
        conn = engine.connect()
    
    # List of all Cosmic Frog Model tables
    db_tables = insp.get_table_names()
    
    data_dict = {}
    
    for i in db_tables:
        with engine.connect() as conn:
            trans = conn.begin()
            if i in tables_we_want:
                print(f'Reading table: {i}')
                data = pd.read_sql_query(sal.text(f"SELECT * FROM {i}"), con=conn)
                if 'id' in data.columns:
                    del data['id']
                data_dict[i] = data
            trans.commit()
    del data
    
    return data_dict


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

data_dict = pull_data_from_cosmic_frog(USER_NAME, APP_KEY, DB_NAME, tables_we_want)

#%% Pull Data from Data Warehouse.

def pull_data_from_data_warehouse():
    return 1










