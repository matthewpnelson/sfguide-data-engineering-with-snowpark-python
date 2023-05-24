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

# def create_entity_df(query): 
    
#     dates = pd.date_range(start=query['start_date'], end=query['end_date'], freq=query['freq']).tolist()

#     all_combinations = list(itertools.product(query['entities'], dates))
#     entity_df = pd.DataFrame(all_combinations, columns=['UWI','TIMESTAMP'])
#     return entity_df

# def create_well_labels_operations_dataset(session):
#     session.use_schema('HARMONIZED')

#     historian_data = session.table("IGNITION_WELLS_V") \
#                 .with_column("TIMESTAMP", F.call_builtin("TRUNC", F.col("TIMESTAMP"), F.lit("HOUR"))) \
#                 .select(F.col("UWI"), F.col("TIMESTAMP"), F.col("TEAM"), F.col("AREA"),F.col("GAS_FLOW"), F.col("GAS_PRESS")) \
#                 .group_by(F.col("UWI"), F.col("TIMESTAMP"), F.col("TEAM"), F.col("AREA")) \
#                 .agg(F.avg(F.col("GAS_FLOW")).alias("GAS_FLOW"), F.avg(F.col("GAS_PRESS")).alias("GAS_PRESS")) \
#                 .sort(F.col("TIMESTAMP"))
                             
#     labels = session.table("RAW_TRAINSET.WELL_LABELS_OPERATIONS") \
#                 .with_column("TIMESTAMP", F.call_builtin("TRUNC", F.col("TIMESTAMP"), F.lit("HOUR"))) \
#                 .select(F.col("UWI").collate('en_us').alias("UWI"), F.col("TIMESTAMP"), F.col("LABEL")) \
#                 .sort(F.col("TIMESTAMP")) \
#                 .group_by(F.col("UWI"), F.col("TIMESTAMP")) \
#                 .agg(F.max(F.col("LABEL")).alias("LABEL"))
                
#     # Join labels and historian data
#     joined_data = historian_data.join(labels, on=['UWI', 'TIMESTAMP'], how='left')
    

#     # convert to pandas and ffill labels
#     joined_data = joined_data.to_pandas()
#     joined_data['LABEL'].fillna(method='ffill', inplace=True, limit=12)
    
#     print(joined_data.loc[joined_data['LABEL'].notnull()])
    
#     session.write_pandas(joined_data, "OPERATIONS_WELL_TRAINING_SET", auto_create_table=True, overwrite=True )
    
    
def generate_csvs_for_trainset(session):
    
    session.use_schema('TRAINING_SETS')
        
    # Create select unique UWI's
    wells = session.table("GAS_WELL_OPERATIONS").select(F.col("UWI")).distinct().collect()
    
    # for each unique UWI, filter the table and output a csv file
    for index, well in enumerate(wells):
        well = well[0]
        well_data = session.table("GAS_WELL_OPERATIONS").filter(F.col("UWI") == well)
        
        team = well_data.select(F.col("TEAM")).distinct().collect()[0][0]
        area = well_data.select(F.col("AREA")).distinct().collect()[0][0]
        print(f"well: {well}, team: {team}, area: {area}")
        
        # to pandas df
        well_data = well_data.to_pandas()
        
        # pivot all columns except uwi_id and event_timestamp into a column named 'series'
        temp_df = well_data.melt(id_vars=['TEAM', 'AREA', 'UWI','TIMESTAMP', 'LABEL'], var_name='series', value_name='value')
        
        temp_df.drop(columns=['UWI'], inplace=True)
        
        
        
        # set value column as float type, remove nan values
        temp_df['value'] = temp_df['value'].astype(float)
        temp_df = temp_df.dropna(subset=['value'])
        
        # Resample values to 6 hour chunks, partition by series and keep label as is and take mean of value
        # temp_df = temp_df.groupby(['series', pd.Grouper(key='TIMESTAMP', freq='6H')]).agg({'value': 'mean', 'label': 'first'}).reset_index()
        
        temp_df.rename(columns={'TIMESTAMP': 'timestamp', 'LABEL': 'label'}, inplace=True)
        
        # some days do not exist in the dataset, so we need to fill in the missing days
        # first get all the dates
        dates = pd.date_range(start=temp_df['timestamp'].min(), end=temp_df['timestamp'].max(), freq='6H')
        # create a dataframe with all the dates
        temp_df2 = pd.DataFrame({'timestamp': dates, 'value': np.nan, 'series': 'GAS_FLOW'})
        
        # add a new row for each of the possible labels, with timestamps set as the max current timestamp + 1 day
        # we do this just to get the labels pre-added so the operators don't have to manually add them each time in Trainset
        possible_labels = ['normal_operation', 'test', 'potato']
        for label in possible_labels:
            temp_df2 = pd.concat([temp_df2, pd.DataFrame({'timestamp': [temp_df2['timestamp'].max() + pd.Timedelta(hours=2)], 'value': [np.nan], 'series': ['GAS_FLOW'], 'label': [label]})], axis=0)
            

        # concat the two dataframes
        temp_df = pd.concat([temp_df, temp_df2], axis=0)
        
        
        # where duplicate timestamps exist, keep the first one
        temp_df.drop_duplicates(subset=['timestamp', 'series'], keep='first', inplace=True)
        
        print(temp_df)
        temp_df.sort_values(['series', 'timestamp'], inplace=True)
        
        # fill in the missing values
        temp_df['value'] = temp_df['value'].fillna(method="ffill", limit=2).fillna(0)
        
        temp_df['timestamp'] = temp_df['timestamp'].dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        
        temp_df = temp_df[['series','timestamp','value', 'label']]
        
        # sort series so that gas_flow is first
        temp_df['series'] = pd.Categorical(temp_df['series'], ['GAS_FLOW', 'GAS_PRESS', 'GAS_TEMP', 'GAS_DP', 'ESDV_CLOSED', 'CHOKE_CLOSED'])
        temp_df.sort_values(['series', 'timestamp'], inplace=True)
        
        
        # to csv
        # mkdir if not exists
        import os
        os.makedirs(f"../data/labels/operations_raw/{team}/{area}", exist_ok=True)
        
        try:
            temp_df.to_csv(f"../data/labels/operations_raw/{team}/{area}/{well}.csv", index=False)
        except:
            print(f"Error writing file: {well}")
     
        if index == 10:
            break




# For local debugging
if __name__ == "__main__":
    # Add the utils package to our path and import the snowpark_utils function
    import os, sys
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()
    
    create_well_labels_operations_dataset(session)
    generate_csvs_for_trainset(session)
    session.close()
