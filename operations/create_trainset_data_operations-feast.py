import itertools
import numpy as np
from snowflake.snowpark import Session
#import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
import pandas as pd
from datetime import datetime

from feast import FeatureStore

from feast.infra.offline_stores.snowflake_source import SavedDatasetSnowflakeStorage

def table_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    return exists

def create_entity_df(query): 
    
    dates = pd.date_range(start=query['start_date'], end=query['end_date'], freq=query['freq']).tolist()

    all_combinations = list(itertools.product(query['entities'], dates))
    entity_df = pd.DataFrame(all_combinations, columns=['UWI','TIMESTAMP'])
    return entity_df


def generate_gas_well_operations_training_dataset(session):
    session.use_schema('HARMONIZED')
    
    # Create select unique UWI's
    wells = session.table("WELLS_HISTORIAN").select(F.col("UWI")).distinct().collect()
    wells = [well['UWI'] for well in wells]
    # wells = ["100022002706W503"]
    
    start_date = "2023-01-01"
    end_date = "2023-03-21"
    freq = "1H"
        
    entity_df = create_entity_df({"entities": wells, "start_date": start_date, "end_date": end_date, "freq": freq})
        
    print(entity_df)

    store = FeatureStore(repo_path="\\\\ucalna01\\apps\\Tableau\\Server Datasets\\FeatureStore\\feast_snowflake\\feature_repo\\")
    
    # print(f"\n--- Materializing Incremental in Online Store")
    # store.materialize_incremental(end_date=datetime.now())
    
    historical_job  = store.get_historical_features(
        entity_df=entity_df,
        features=store.get_feature_service("fs_operations_well_labels"),
    ).to_df()

    print(historical_job.head())
    
    # Convert TIMESTAMP to datetime in  2023-01-07 00:39:55.000 -0800 format
    print(historical_job.sort_values(by=['TIMESTAMP']).head())
    
    session.use_schema('TRAINING_SETS')
    session.sql('USE ROLE CDE_ROLE').collect()
    # session.sql('DROP TABLE IF EXISTS GAS_WELL_OPERATIONS_FEAST')
    
    # # create a table in snowflake if it does not yet exist - ensure unique UWI and TIMESTAMP
    # print(table_exists(session, 'TRAINING_SETS', 'GAS_WELL_OPERATIONS_FEAST'))
    if not table_exists(session, 'TRAINING_SETS', 'GAS_WELL_OPERATIONS_FEAST'):
        session.sql("CREATE OR REPLACE TABLE TRAINING_SETS.GAS_WELL_OPERATIONS_FEAST ( \
            UWI VARCHAR(8192) COLLATE 'en_us', \
            TIMESTAMP TIMESTAMP_NTZ(9), \
            TEAM VARCHAR(16777216) COLLATE 'en_us', \
            AREA VARCHAR(16777216) COLLATE 'en_us', \
            GAS_FLOW FLOAT, \
            GAS_PRESS FLOAT, \
            GAS_TEMP FLOAT, \
            GAS_DP FLOAT, \
            PLUNGER FLOAT, \
            CHOKE_POSITION FLOAT, \
            ESDV_OPEN FLOAT, \
            ESDV_CLOSED FLOAT, \
            LABEL VARCHAR(16777216) COLLATE 'en_us', \
            CONSTRAINT uwi_timestamp PRIMARY KEY (UWI, TIMESTAMP) ENFORCED \
            )").collect()

    session.use_schema('TRAINING_SETS')
    
    # drop any duplicates based on UWI and TIMESTAMP
    historical_job = historical_job.drop_duplicates()
    
    # print any duplicates based on UWI and TIMESTAMP
    print(historical_job[historical_job.duplicated()])
    
    historical_job['TIMESTAMP'] = pd.to_datetime(historical_job['TIMESTAMP']).dt.strftime('%Y-%m-%d %H:%M:%S.%f %z')

    ## GETTING SOME DUPLICATES with this? No duplicates are present in the pandas dataframe? I think this has to do with the .write_pandas function
    session.write_pandas(historical_job, "GAS_WELL_OPERATIONS_FEAST", auto_create_table=False, overwrite=True)
    
     ## Select all rows but exclude duplicates based on UWI and TIMESTAMP, keeping the first row of a duplicate, then re-write to table
    session.sql('DROP TABLE IF EXISTS TRAINING_SETS.GAS_WELL_OPERATIONS_FEAST_NO_DUPS').collect()
    session.sql('CREATE OR REPLACE TABLE TRAINING_SETS.GAS_WELL_OPERATIONS_FEAST_NO_DUPS AS \
        SELECT * FROM TRAINING_SETS.GAS_WELL_OPERATIONS_FEAST QUALIFY ROW_NUMBER() OVER (PARTITION BY UWI, TIMESTAMP ORDER BY UWI, TIMESTAMP) = 1 ORDER BY UWI, TIMESTAMP').collect()
    session.sql('DROP TABLE IF EXISTS TRAINING_SETS.GAS_WELL_OPERATIONS_FEAST').collect()
    session.sql('CREATE OR REPLACE TABLE TRAINING_SETS.GAS_WELL_OPERATIONS_FEAST AS SELECT * FROM TRAINING_SETS.GAS_WELL_OPERATIONS_FEAST_NO_DUPS').collect()
    session.sql('DROP TABLE IF EXISTS TRAINING_SETS.GAS_WELL_OPERATIONS_FEAST_NO_DUPS').collect()
    
    # # manually drop the OPERATIONS_WELL_TRAINING_SET table
    # _ = session.sql('DROP TABLE IF EXISTS FEAST_OFFLINE.OPERATIONS_WELL_TRAINING_SET').collect()
    
    # dataset = store.create_saved_dataset(
    #     from_=historical_job ,
    #     name='operations_well_training_set',
    #     storage=SavedDatasetSnowflakeStorage(table_ref='OPERATIONS_WELL_TRAINING_SET'),
    #     # allow_overwrite=True, # doesn't work with Snowflake
    #     tags={'author': 'mnelson'}
    # )
    
    # historical_job.to_df()
    
    
    # # for each unique UWI, filter the table and output a csv file
    # for index, well in enumerate(wells):
    #     well = well[0]
    #     well_data = session.table("IGNITION_WELLS_V").filter(F.col("UWI") == well)
        
    #     team = well_data.select(F.col("TEAM")).distinct().collect()[0][0]
    #     area = well_data.select(F.col("AREA")).distinct().collect()[0][0]
    #     print(f"well: {well}, team: {team}, area: {area}")
        
    #     # to pandas df
    #     well_data = well_data.to_pandas()
        
    #     # pivot all columns except uwi_id and event_timestamp into a column named 'series'
    #     temp_df = well_data.melt(id_vars=['TEAM', 'AREA', 'UWI','TIMESTAMP'], var_name='series', value_name='value')
        
    #     temp_df.drop(columns=['UWI'], inplace=True)
        
    #     temp_df['label'] = None
        
    #     # set value column as float type, remove nan values
    #     temp_df['value'] = temp_df['value'].astype(float)
    #     temp_df = temp_df.dropna(subset=['value'])
        
    #     # Resample values to 6 hour chunks, partition by series and keep label as is and take mean of value
    #     # temp_df = temp_df.groupby(['series', pd.Grouper(key='TIMESTAMP', freq='6H')]).agg({'value': 'mean', 'label': 'first'}).reset_index()
        
    #     temp_df.rename(columns={'TIMESTAMP': 'timestamp'}, inplace=True)
        
    #     # some days do not exist in the dataset, so we need to fill in the missing days
    #     # first get all the dates
    #     dates = pd.date_range(start=temp_df['timestamp'].min(), end=temp_df['timestamp'].max(), freq='6H')
    #     # create a dataframe with all the dates
    #     temp_df2 = pd.DataFrame({'timestamp': dates, 'value': np.nan, 'series': 'GAS_FLOW'})
        
    #     # add a new row for each of the possible labels, with timestamps set as the max current timestamp + 1 day
    #     # we do this just to get the labels pre-added so the operators don't have to manually add them each time in Trainset
    #     possible_labels = ['normal_operation', 'test', 'potato']
    #     for label in possible_labels:
    #         temp_df2 = pd.concat([temp_df2, pd.DataFrame({'timestamp': [temp_df2['timestamp'].max() + pd.Timedelta(hours=2)], 'value': [np.nan], 'series': ['GAS_FLOW'], 'label': [label]})], axis=0)
            
        
    #     # concat the two dataframes
    #     temp_df = pd.concat([temp_df2, temp_df], axis=0)
    #     print(temp_df)
    #     temp_df.sort_values(['series', 'timestamp'], inplace=True)
    #     # fill in the missing values
    #     temp_df['value'] = temp_df['value'].fillna(method="ffill", limit=2).fillna(0)
        
    #     temp_df['timestamp'] = temp_df['timestamp'].dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        
    #     temp_df = temp_df[['series','timestamp','value', 'label']]
        
    #     # sort series so that gas_flow is first
    #     temp_df['series'] = pd.Categorical(temp_df['series'], ['GAS_FLOW', 'GAS_PRESS', 'GAS_TEMP', 'GAS_DP', 'ESDV_CLOSED', 'CHOKE_CLOSED'])
    #     temp_df.sort_values(['series', 'timestamp'], inplace=True)
        
        
    #     # to csv
    #     # mkdir if not exists
    #     import os
    #     os.makedirs(f"../data/labels/operations_raw/{team}/{area}", exist_ok=True)
        
    #     try:
    #         temp_df.to_csv(f"../data/labels/operations_raw/{team}/{area}/{well}.csv", index=False)
    #     except:
    #         print(f"Error writing file: {well}")
     
    #     if index == 10:
    #         break




# For local debugging
if __name__ == "__main__":
    # Add the utils package to our path and import the snowpark_utils function
    import os, sys
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()
    
    generate_gas_well_operations_training_dataset(session)

    session.close()
