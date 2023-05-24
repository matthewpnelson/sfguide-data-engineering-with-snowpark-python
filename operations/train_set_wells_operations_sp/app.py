
from datetime import datetime, timedelta
import pandas as pd
from snowflake.snowpark import Session
#import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F

def table_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    return exists


def create_well_labels_operations_table(session):
    session.use_schema('HARMONIZED')

    historian_data = session.table("WELLS_HISTORIAN_1H") \
                .select(
                    F.col("UWI"),
                    F.col("TIMESTAMP"),
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
                    F.col("ESDV_CLOSED")
                ) \
                .sort(F.col("UWI"), F.col("TIMESTAMP"))
                             
    labels = session.table("RAW_TRAINSET.WELL_LABELS_OPERATIONS") \
                .with_column("TIMESTAMP", F.call_builtin("TRUNC", F.col("TIMESTAMP"), F.lit("HOUR"))) \
                .select(F.col("UWI").collate('en_us').alias("UWI"), F.col("TIMESTAMP"), F.col("LABEL")) \
                .sort(F.col("TIMESTAMP")) \
                .group_by(F.col("UWI"), F.col("TIMESTAMP")) \
                .agg(F.max(F.col("LABEL")).alias("LABEL"))
                
    # Join labels and historian data
    joined_data = historian_data.join(labels, on=['UWI', 'TIMESTAMP'], how='left')
    
    # convert to pandas and ffill labels within each UWI
    joined_data = joined_data.to_pandas()
    joined_data.sort_values(by=['UWI', 'TIMESTAMP'], inplace=True)

    # groupby UWI and ffill LABEL
    joined_data['LABEL'] = joined_data.groupby('UWI')['LABEL'].ffill(limit=24)
    
    joined_data['TIMESTAMP'] = pd.to_datetime(joined_data['TIMESTAMP']).dt.strftime('%Y-%m-%d %H:%M:%S.%f %z')
    print(joined_data.loc[joined_data['LABEL'].notnull()])

    session.use_schema('TRAINING_SETS')
    session.sql('DROP TABLE IF EXISTS TRAINING_SETS.GAS_WELL_OPERATIONS').collect()
    if not table_exists(session, 'TRAINING_SETS', 'GAS_WELL_OPERATIONS'):
        session.sql("CREATE OR REPLACE TABLE TRAINING_SETS.GAS_WELL_OPERATIONS ( \
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
            LABEL VARCHAR(16777216) COLLATE 'en_us' \
            )").collect()
    
    session.write_pandas(joined_data, "GAS_WELL_OPERATIONS", auto_create_table=False, overwrite=True )
    

def main(session: Session) -> str:
    create_well_labels_operations_table(session)

    return f"Successfully processed GAS_WELL_OPERATIONS training set table."


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
