
from datetime import datetime, timedelta, timezone
import pandas as pd
from snowflake.snowpark import Session
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
from snowflake.connector.pandas_tools import write_pandas
import numpy as np

def table_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    return exists


def resample_well_historian(session):
    
    update_or_overwrite = 'UPDATE' # 'UPDATE' or 'OVERWRITE'
    use_wh_scaling = False
    
    session.use_schema('HARMONIZED')
    
    if use_wh_scaling:
        print('Setting warehouse size to MEDIUM')
        _ = session.sql('ALTER WAREHOUSE CDE_WH SET WAREHOUSE_SIZE = MEDIUM WAIT_FOR_COMPLETION = TRUE').collect()

    if update_or_overwrite == 'OVERWRITE':

        historian_data = session.table("WELLS_HISTORIAN") \
                    .select(
                        F.col("UWI"),
                        F.col("TIMESTAMP").cast(T.TimestampType()).alias("TIMESTAMP"),
                        F.col("TEAM"),
                        F.col("MAJOR_AREA"),
                        F.col("AREA"),
                        F.col("GAS_FLOW"),
                        F.col("GAS_PRESS"),
                        F.col("GAS_TEMP"),
                        F.col("GAS_DP"),
                        F.col("PLUNGER"),
                        F.col("CHOKE_POSITION"),
                        F.col("ESDV_OPEN"),
                        F.col("ESDV_CLOSED"),
                        F.col("STATUS"),
                        F.col("ETO_DATE"),
                        F.col("NOTES")
                        # F.col("AVAILABILITY")
                    ) \
                    .sort(F.col("UWI"), F.col("TIMESTAMP"))
                    # .filter(((F.col("AREA") == F.lit("PANTHER")) )  & ( F.col("TIMESTAMP") > F.lit("2023-04-20 15:20:20")))
                    # .filter(F.col("TIMESTAMP") > F.lit("2023-02-20 15:20:20"))
                        # .filter(F.col("AREA") == F.lit("BURNT TIMB"))
    
    elif update_or_overwrite == 'UPDATE':
        
        # most recent date in resampled table
        most_recent_date = session.sql("SELECT MAX(TIMESTAMP) AS MAX_TIMESTAMP FROM WELLS_HISTORIAN_1H").collect()[0]['MAX_TIMESTAMP']
        
        historian_data = session.table("WELLS_HISTORIAN") \
                    .select(
                        F.col("UWI"),
                        F.col("TIMESTAMP").cast(T.TimestampType()).alias("TIMESTAMP"),
                        F.col("TEAM"),
                        F.col("MAJOR_AREA"),
                        F.col("AREA"),
                        F.col("GAS_FLOW"),
                        F.col("GAS_PRESS"),
                        F.col("GAS_TEMP"),
                        F.col("GAS_DP"),
                        F.col("PLUNGER"),
                        F.col("CHOKE_POSITION"),
                        F.col("ESDV_OPEN"),
                        F.col("ESDV_CLOSED"),
                        F.col("STATUS"),
                        F.col("ETO_DATE"),
                        F.col("NOTES")
                        # F.col("AVAILABILITY")
                    ) \
                    .sort(F.col("UWI"), F.col("TIMESTAMP")) \
                    .filter(( F.col("TIMESTAMP") > (most_recent_date - timedelta(days=90)) ))
                        # .filter(F.col("AREA") == F.lit("BURNT TIMB"))
    
    
    # create a new pandas dataframe with UWI and TIMESTAMP. 
    # fully populate the dataframe with all UWI and TIMESTAMP combinations in an hourly frequency, from the earliest to the latest TIMESTAMP
    # this will be used to join with the resampled data to fill in missing hours
     
    # get minimum and maximum TIMESTAMP
    min_timestamp = historian_data.select(F.min(F.col("TIMESTAMP"))).collect()[0][0]
    max_timestamp = historian_data.select(F.max(F.col("TIMESTAMP"))).collect()[0][0]
    
    # round up timestamp to the nearest hour
    min_timestamp = min_timestamp.replace(minute=0, second=0, microsecond=0)
    max_timestamp = max_timestamp.replace(minute=0, second=0, microsecond=0)
    
    print("min_timestamp: ", min_timestamp)
    print("max_timestamp: ", max_timestamp)
    
    # create a list of all UWI
    uwi_list = historian_data.select(F.col("UWI")).distinct().collect()
    
    # create a list of all timestamps
    timestamp_list = pd.date_range(start=min_timestamp, end=max_timestamp, freq='1H')
     
    # create a dataframe with all UWI and TIMESTAMP combinations
    uwi_timestamp_df = pd.DataFrame(columns=['UWI', 'TIMESTAMP'])
    for uwi in uwi_list:
        # concat together
        uwi_timestamp_df = pd.concat([uwi_timestamp_df, pd.DataFrame({'UWI': uwi['UWI'], 'TIMESTAMP': timestamp_list})])
         
    # print max and min timestamp for each uwi in uwi_timestamp_df
    # print(uwi_timestamp_df.groupby('UWI').agg({'TIMESTAMP': [min, max]}))
    
    # convert snowflake df to pandas and ffill labels within each UWI
    historian_data = historian_data.to_pandas()
    
    # # join with uwi_timestamp_df to fill in missing hours
    # historian_data =   uwi_timestamp_df.merge(historian_data, how='left', on=['UWI', 'TIMESTAMP'])
        
    # # print the number of rows for each UWI
    # print(historian_data.groupby('UWI').size())
        
    # # sort index ascending
    # historian_data.sort_values(by=['UWI', 'TIMESTAMP'], inplace=True)
    # # historian_data.sort_index(inplace=True)
            
    # resample to 1H and take mean of each column except categories 
    
    historian_data_resampled = historian_data[['UWI', 'TIMESTAMP', 'GAS_FLOW', 'GAS_PRESS', 'GAS_TEMP', 'GAS_DP', 'PLUNGER', 'CHOKE_POSITION', 'ESDV_OPEN', 'ESDV_CLOSED']].groupby(by=[ 'UWI', pd.Grouper(key='TIMESTAMP', freq='1H')]).mean(numeric_only=True).reset_index()
    
    # join with uwi_timestamp_df to fill in missing hours
    historian_data_resampled = uwi_timestamp_df.merge(historian_data_resampled, how='left', on=['UWI', 'TIMESTAMP'])
    
    # Interpolate missing hours within each UWI
    limit = 24
    # historian_data_resampled = historian_data_resampled.groupby('UWI').apply(lambda group: group.interpolate(method='index'))
    
    historian_data_resampled['GAS_FLOW'] = historian_data_resampled.groupby('UWI')['GAS_FLOW'].ffill(limit=limit).fillna(0)
    historian_data_resampled['GAS_PRESS'] = historian_data_resampled.groupby('UWI')['GAS_PRESS'].ffill().bfill()
    historian_data_resampled['GAS_TEMP'] = historian_data_resampled.groupby('UWI')['GAS_TEMP'].ffill().bfill()
    historian_data_resampled['GAS_DP'] = historian_data_resampled.groupby('UWI')['GAS_DP'].ffill(limit=limit).fillna(0)
    historian_data_resampled['PLUNGER'] = historian_data_resampled.groupby('UWI')['PLUNGER'].ffill().bfill()
    historian_data_resampled['CHOKE_POSITION'] = historian_data_resampled.groupby('UWI')['CHOKE_POSITION'].ffill().bfill()
    historian_data_resampled['ESDV_OPEN'] = historian_data_resampled.groupby('UWI')['ESDV_OPEN'].ffill().bfill()
    historian_data_resampled['ESDV_CLOSED'] = historian_data_resampled.groupby('UWI')['ESDV_CLOSED'].ffill().bfill()
    
    
    
    
    historian_data_resampled_categorical = historian_data[['UWI', 'TIMESTAMP', 'TEAM', 'MAJOR_AREA', 'AREA', 'STATUS', 'ETO_DATE', 'NOTES']].groupby(by=[ 'UWI', pd.Grouper(key='TIMESTAMP', freq='1H')]).last().reset_index()
    
    # # groupby UWI: if the count of nan's in the STATUS column is the same as the number of rows, fill with 'NEEDS TAG IN NOCO
    # historian_data_resampled_categorical['STATUS'] = historian_data_resampled_categorical.groupby('UWI')['STATUS'].apply( \
    #     lambda x: x.fillna('NEEDS TAG IN NOCO') if x.isnull().sum() == len(x) else x \
    # )
    
     # select most recent for each of the categorical columns from existing resampled data and fill na's for each UWI if necessary
     # this protects against showing no status for wells which have not had a new STATUS entry in the last 30 days. need to go back further in time
    # if update_or_overwrite == 'UPDATE':
    #     most_recent_historian_values = session.table("WELLS_HISTORIAN_MOST_RECENT") \
    #                             .select(F.col("UWI"), F.col("STATUS"), F.col("ETO_DATE"), F.col("NOTES"), F.col("AVAILABILITY").cast(T.IntegerType()).alias("AVAILABILITY")) \
    #                             .to_pandas()
    
    #     # count non nulls of STATUS for each UWI. If count is == 0 then fill with most_recent_historian_values[most_recent_historian_values['UWI'] == x.name]['STATUS'].values[0]
    #     historian_data_resampled_categorical['STATUS'] = historian_data_resampled_categorical.groupby('UWI')['STATUS'].apply( \
    #         lambda x: x.fillna(most_recent_historian_values[most_recent_historian_values['UWI'] == x.name]['STATUS'].values[0]) if x.isnull().sum() == len(x) else x \
    #     )
        
    
    # print rows where STATUS != null and UWI = 100020903010W500        
    # print(historian_data_resampled_categorical[(historian_data_resampled_categorical['AVAILABILITY'].notnull()) & (historian_data_resampled_categorical['UWI'] == '100033002706W500')])
    
    # join with uwi_timestamp_df to fill in missing hours
    historian_data_resampled_categorical = uwi_timestamp_df.merge(historian_data_resampled_categorical, how='left', on=['UWI', 'TIMESTAMP'])

    # print(historian_data_resampled_categorical.head())
    
    # join timestamps with historian_data_resampled and fill missing values with ffill within UWI 
    historian_data_resampled_categorical = historian_data_resampled[['UWI', 'TIMESTAMP']].merge(historian_data_resampled_categorical, on=['UWI', 'TIMESTAMP'], how='left')
    historian_data_resampled_categorical = historian_data_resampled_categorical.sort_values(by=['UWI', 'TIMESTAMP'])
    print(historian_data_resampled_categorical.head(10))
    
    # ffill but only within UWI
    historian_data_resampled_categorical['TEAM'] = historian_data_resampled_categorical.groupby('UWI')['TEAM'].ffill().bfill()
    historian_data_resampled_categorical['MAJOR_AREA'] = historian_data_resampled_categorical.groupby('UWI')['MAJOR_AREA'].ffill().bfill()
    historian_data_resampled_categorical['AREA'] = historian_data_resampled_categorical.groupby('UWI')['AREA'].ffill().bfill()
    historian_data_resampled_categorical['STATUS'] = historian_data_resampled_categorical.groupby('UWI')['STATUS'].ffill().fillna('NEEDS TAG IN NOCO')
    historian_data_resampled_categorical['ETO_DATE'] = historian_data_resampled_categorical.groupby('UWI')['ETO_DATE'].ffill()
    historian_data_resampled_categorical['NOTES'] = historian_data_resampled_categorical.groupby('UWI')['NOTES'].ffill()
    # historian_data_resampled_categorical['AVAILABILITY'] = historian_data_resampled_categorical.groupby('UWI')['AVAILABILITY'].ffill()
    
    # map statuses to availability
    availability_map = {'Available': 1, 'Auto Available': 1, 'Maintenance': 0, 'Auto Offline': 0, 'Operations': 0, 'Maintenance': 0, 'D&C': 0, 'OOS': 0, 'Projects': 0, 'D/S-Canlin': 0, 'D/S-3rd Party': 0, 'NEEDS TAG IN NOCO': 0}
    historian_data_resampled_categorical['AVAILABILITY'] =  historian_data_resampled_categorical['STATUS'].map(availability_map)
    
    # print bottom 10 rows for UWI 100110803010W500
    # print(historian_data_resampled_categorical[historian_data_resampled_categorical['UWI'] == '100110803010W500'].tail(10))
    # 

   
    
    # # Concatenate the two dataframes
    historian_data_resampled.set_index(['UWI', 'TIMESTAMP'], inplace=True)
    historian_data_resampled_categorical.set_index(['UWI', 'TIMESTAMP'], inplace=True)
    historian_data_resampled = pd.concat([historian_data_resampled_categorical, historian_data_resampled], axis=1)
    
    # Clean up
    historian_data_resampled.reset_index(inplace=True)
    historian_data_resampled.sort_values(by=['UWI', 'TIMESTAMP'], inplace=True)
    
    # if AVAILABILITY is null, assign to 1 if GAS_FLOW != 0 and is not null, else 0
    # historian_data_resampled['AVAILABILITY'] = historian_data_resampled['AVAILABILITY'].fillna(1 if (historian_data_resampled['GAS_FLOW'] != 0) & (historian_data_resampled['GAS_FLOW'].notnull()) else 0)
    
    # drop rows with nulls
    historian_data_resampled.dropna(subset=['UWI'], inplace=True)
    
    # # fill AVAILABILITY nulls with ffill then bfill then 0 
    # historian_data_resampled['AVAILABILITY'] = historian_data_resampled.groupby('UWI')['AVAILABILITY'].ffill().bfill().fillna(0)
    # print count of nulls in each column
    print(historian_data_resampled.isnull().sum())
    
    # Type Changes
    historian_data_resampled['TIMESTAMP'] = pd.to_datetime(historian_data_resampled['TIMESTAMP']).dt.strftime('%Y-%m-%d %H:%M:%S.%f %z')
    # historian_data_resampled['AVAILABILITY'] = historian_data_resampled['AVAILABILITY'].astype(int)
    
    # print the number of rows for each UWI, and the max and min timestamps
    # print( historian_data_resampled.groupby('UWI').size())
    # print( historian_data_resampled.groupby('UWI').agg({'TIMESTAMP': ['min', 'max']}))
    
    
    # Drop duplicates
    # historian_data_resampled = historian_data_resampled.drop_duplicates(subset=['UWI', 'TIMESTAMP'], keep='last')
    # print(historian_data_resampled.tail())
    
    # print 100052303109W500 to check
    # print(historian_data_resampled[historian_data_resampled['UWI'] == '100052303109W500'].head(20))
    
    if update_or_overwrite == 'OVERWRITE':
        session.use_schema('HARMONIZED')
        session.sql('DROP TABLE IF EXISTS HARMONIZED.WELLS_HISTORIAN_1H').collect()
        if not table_exists(session, 'HARMONIZED', 'WELLS_HISTORIAN_1H'):
            session.sql("CREATE OR REPLACE TABLE HARMONIZED.WELLS_HISTORIAN_1H ( \
                UWI VARCHAR(8192) COLLATE 'en_us', \
                TIMESTAMP TIMESTAMP_NTZ(9), \
                TEAM VARCHAR(16777216) COLLATE 'en_us', \
                MAJOR_AREA VARCHAR(16777216) COLLATE 'en_us', \
                AREA VARCHAR(16777216) COLLATE 'en_us', \
                GAS_FLOW FLOAT, \
                GAS_PRESS FLOAT, \
                GAS_TEMP FLOAT, \
                GAS_DP FLOAT, \
                PLUNGER FLOAT, \
                CHOKE_POSITION FLOAT, \
                ESDV_OPEN FLOAT, \
                ESDV_CLOSED FLOAT, \
                STATUS VARCHAR(16777216) COLLATE 'en_us', \
                ETO_DATE VARCHAR(16777216) COLLATE 'en_us', \
                NOTES VARCHAR(16777216) COLLATE 'en_us', \
                AVAILABILITY INT \
                )").collect()
        
        # GETTING SOME DUPLICATES with this? No duplicates are present in the historian_data_resampled dataframe? I think this has to do with the .write_pandas function
        session.write_pandas(historian_data_resampled, "WELLS_HISTORIAN_1H", auto_create_table=False, overwrite=True)
        
        # ## Select all rows but exclude duplicates based on UWI and TIMESTAMP, keeping the first row of a duplicate, then re-write to table
        # session.sql('DROP TABLE IF EXISTS HARMONIZED.WELLS_HISTORIAN_1H_NO_DUPS').collect()
        # session.sql('CREATE OR REPLACE TABLE HARMONIZED.WELLS_HISTORIAN_1H_NO_DUPS AS \
        #     SELECT * FROM HARMONIZED.WELLS_HISTORIAN_1H QUALIFY ROW_NUMBER() OVER (PARTITION BY UWI, TIMESTAMP ORDER BY UWI, TIMESTAMP) = 1 ORDER BY UWI, TIMESTAMP').collect()
        # session.sql('DROP TABLE IF EXISTS HARMONIZED.WELLS_HISTORIAN_1H').collect()
        # session.sql('CREATE OR REPLACE TABLE HARMONIZED.WELLS_HISTORIAN_1H AS SELECT * FROM HARMONIZED.WELLS_HISTORIAN_1H_NO_DUPS').collect()
        # session.sql('DROP TABLE IF EXISTS HARMONIZED.WELLS_HISTORIAN_1H_NO_DUPS').collect()
        
    elif update_or_overwrite == 'UPDATE':
        session.use_schema('HARMONIZED')
                            
        # write to Snowflake Table so we can use the MERGE function
        session.write_pandas(historian_data_resampled, "WELLS_HISTORIAN_TEMP_UPDATE", auto_create_table=True, overwrite=True, table_type='temp')
        
        update_table = session.table('HARMONIZED.WELLS_HISTORIAN_TEMP_UPDATE') \
                                .select(F.col('UWI'), F.col('TIMESTAMP'), F.col('TEAM'), F.col('MAJOR_AREA'), F.col('AREA'), F.col('GAS_FLOW'), F.col('GAS_PRESS'), F.col('GAS_TEMP'), F.col('GAS_DP'), F.col('PLUNGER'), F.col('CHOKE_POSITION'), F.col('ESDV_OPEN'), F.col('ESDV_CLOSED'), F.col('STATUS'),  F.col('ETO_DATE'),  F.col('NOTES'), F.col('AVAILABILITY'))
        sf_table = session.table('HARMONIZED.WELLS_HISTORIAN_1H')
        
        cols_to_update = {c: update_table[c] for c in update_table.schema.names}
        updates = {
            **cols_to_update,
            }
        
        sf_table.merge(update_table, (sf_table['TIMESTAMP'] == update_table['TIMESTAMP']) & (sf_table['UWI'] == update_table['UWI']), \
                            [F.when_matched().update(updates), F.when_not_matched().insert(updates)])

        session.sql('DROP TABLE IF EXISTS HARMONIZED.WELLS_HISTORIAN_TEMP_UPDATE').collect()
    
    if use_wh_scaling:
        print("Rescaling Warehouse to XSMALL")
        _ = session.sql('ALTER WAREHOUSE CDE_WH SET WAREHOUSE_SIZE = XSMALL').collect()

def main(session: Session) -> str:
    resample_well_historian(session)
    return f"Successfully resampled WELLS_HISTORIAN data to 1H Intervals."


# For local debugging
# Be aware you may need to type-convert arguments if you add input parameters
if __name__ == '__main__':
    # Add the utils package to our path and import the snowpark_utils function
    import os, sys
    current_dir = os.getcwd()
    parent_parent_dir = os.path.dirname(os.path.dirname(current_dir))
    sys.path.append(parent_parent_dir)
    
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()

    if len(sys.argv) > 1:
        print(main(session, *sys.argv[1:]))  # type: ignore
    else:
        print(main(session))  # type: ignore

    session.close()

#  MERGE INTO HARMONIZED.WELLS_HISTORIAN_1H USING 
#  ( SELECT "UWI" AS "r_cls2_UWI", 
#   "TIMESTAMP" AS "r_cls2_TIMESTAMP", 
#   "TEAM" AS "r_cls2_TEAM", 
#   "MAJOR_AREA" AS "r_cls2_MAJOR_AREA",
#   "AREA" AS "r_cls2_AREA", 
#   "GAS_FLOW" AS "r_cls2_GAS_FLOW",
#   "GAS_PRESS" AS "r_cls2_GAS_PRESS", 
#   "GAS_TEMP" AS "r_cls2_GAS_TEMP", 
#   "GAS_DP" AS "r_cls2_GAS_DP", 
#   "PLUNGER" AS "r_cls2_PLUNGER", 
#   "CHOKE_POSITION" AS "r_cls2_CHOKE_POSITION", 
#   "ESDV_OPEN" AS "r_cls2_ESDV_OPEN", 
#   "ESDV_CLOSED" AS "r_cls2_ESDV_CLOSED", 
#   "STATUS" AS "r_cls2_STATUS", 
#   "AVAILABILITY" AS "r_cls2_AVAILABILITY" 
#   FROM HARMONIZED.WELLS_HISTORIAN_TEMP_UPDATE
#   ) 
#  ON (("TIMESTAMP" = "r_cls2_TIMESTAMP") AND ("UWI" = "r_cls2_UWI")) 
#  WHEN  MATCHED  THEN  UPDATE  
#  SET "UWI" = "UWI", "TIMESTAMP" = "TIMESTAMP", "TEAM" = "TEAM", "MAJOR_AREA" = "MAJOR_AREA", "AREA" = "AREA", "GAS_FLOW" = "GAS_FLOW", "GAS_PRESS" = "GAS_PRESS", "GAS_TEMP" = "GAS_TEMP", "GAS_DP" = "GAS_DP", "PLUNGER" = "PLUNGER", "CHOKE_POSITION" = "CHOKE_POSITION", "ESDV_OPEN" = "ESDV_OPEN", "ESDV_CLOSED" = "ESDV_CLOSED", "STATUS" = "STATUS", "AVAILABILITY" = "AVAILABILITY" 
#  WHEN  NOT  MATCHED  THEN  INSERT ("UWI", "TIMESTAMP", "TEAM", "MAJOR_AREA", "AREA", "GAS_FLOW", "GAS_PRESS", "GAS_TEMP", "GAS_DP", "PLUNGER", "CHOKE_POSITION", "ESDV_OPEN", "ESDV_CLOSED", "STATUS", "AVAILABILITY") 
#  VALUES ("UWI", "TIMESTAMP", "TEAM", "MAJOR_AREA", "AREA", "GAS_FLOW", "GAS_PRESS", "GAS_TEMP", "GAS_DP", "PLUNGER", "CHOKE_POSITION", "ESDV_OPEN", "ESDV_CLOSED", "STATUS", "AVAILABILITY")