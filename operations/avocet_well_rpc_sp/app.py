
from datetime import datetime, timedelta
import pandas as pd
from snowflake.snowpark import Session
#import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
from snowflake.snowpark import Window as W


def table_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    return exists

def update_table(session):
    
    session.use_schema('HARMONIZED')
    use_wh_scaling = False
    
    if use_wh_scaling:
        _ = session.sql('ALTER WAREHOUSE CDE_WH SET WAREHOUSE_SIZE = MEDIUM WAIT_FOR_COMPLETION = TRUE').collect()
    
    # select data where date > 365 days ago
    df = session.table("HARMONIZED.AVOCET_PROD_DETAIL") \
        .select(
            F.col("UWI"),
            F.col("PRODUCTION_DATE"),
            F.col("TEAM"),
            F.col("MAJOR_AREA"),
            F.col("AREA"),
            
            F.col("GROSS_BOE"),
            F.col("GROSS_M3E"),
            F.col("NGL_GROSS_M3"), 
            F.col("NGL_GROSS_BBL"),
            F.col("OIL_GROSS_M3"),
            F.col("OIL_GROSS_BBL"),
            F.col("COND_GROSS_M3"),
            F.col("COND_GROSS_BBL"),
            F.col("GAS_GROSS_E3M3"),
            F.col("GAS_GROSS_MCF"),
            F.col("GAS_GROSS_BOE"),
            
            # F.col("NET_BOE"),
            # F.col("NET_M3E"),
            # F.col("NGL_NET_M3"), 
            # F.col("NGL_NET_BBL"),
            # F.col("OIL_NET_M3"),
            # F.col("OIL_NET_BBL"),
            # F.col("COND_NET_M3"),
            # F.col("COND_NET_BBL"),
            # F.col("GAS_NET_E3M3"),
            # F.col("GAS_NET_MCF"),
            # F.col("GAS_NET_BOE"),
              
            F.col("SALES_M3E"),
            F.col("SALES_BOE"),
            F.col("NGL_SALES_M3"), 
            F.col("NGL_SALES_BBL"),
            F.col("OIL_SALES_M3"),
            F.col("OIL_SALES_BBL"),
            F.col("COND_SALES_M3"),
            F.col("COND_SALES_BBL"),
            F.col("GAS_SALES_E3M3"),
            F.col("GAS_SALES_MCF"),
            F.col("GAS_SALES_BOE"),
            
        ) \
        .filter((F.col("PRODUCTION_DATE") >= F.lit(datetime.now() - timedelta(days=730))) \
            # & (F.col('TEAM') == 'WILDCAT') \
            # & (F.col('TEAM') != F.lit("EAST")) # we will be getting rid of this filter eventually, will want to scale up to a larger warehouse to process all 7000 wells (939 excludes EAST)
                ) \
        .sort(F.col("UWI"), F.col("PRODUCTION_DATE")) \
            .to_pandas()



    # # calculate the 75th percentile of the 270 day window of GROSS_BOE, partition by UWI
    # window1 = W().partition_by("UWI").order_by("PRODUCTION_DATE").rows_between(-270, W.CURRENT_ROW)
    # df = df.withColumn(f'GROSS_BOE_270D_RPC',  
    #                     F.approx_percentile(F.col("GROSS_BOE"), 0.75).over(window1)
    #                    )
    # #### Sliding window frame unsupported for function APPROX_PERCENTILE - need to convert to pandas ###
    # print(df.limit(20).show())
    
    # in pandas, we can use rolling window to calculate the 75th percentile of the 270 day window of GROSS_BOE, partition by UWI
    # columns not in ['UWI', 'PRODUCTION_DATE', 'TEAM', 'MAJOR_AREA', 'AREA']
    
    val_columns =  [col for col in df.columns if col not in ['UWI', 'PRODUCTION_DATE', 'TEAM', 'MAJOR_AREA', 'AREA']]
    for col in val_columns:
        print(f'Calculating rolling percentiles for {col}')
        # time the operation
        start = datetime.now()
        df[f'{col}_270D_RPC'] = df.groupby('UWI')[col].rolling(270).quantile(0.65).reset_index(0, drop=True)
        df[f'{col}_90D_50P'] = df.groupby('UWI')[col].rolling(90).quantile(0.5).reset_index(0, drop=True)
        # print time elapsed in seconds
        print(f'Elapsed time:  {(datetime.now() - start).total_seconds()} seconds')
    
    # print(df.tail(20))
    df['PRODUCTION_DATE'] = pd.to_datetime(df['PRODUCTION_DATE']).dt.strftime('%Y-%m-%d %H:%M:%S.%f %z')
    
    print('Writing to Snowflake...')
    session.write_pandas(df, "UWI_RPC", auto_create_table=True, overwrite=True )
    
    if use_wh_scaling:
        _ = session.sql('ALTER WAREHOUSE CDE_WH SET WAREHOUSE_SIZE = XSMALL').collect()
    



def main(session: Session) -> str:
    # Create the WELLS_HISTORIAN table  if they don't exist
    # if not table_exists(session, schema='HARMONIZED', name='AVOCET_WELL_STATS'):
    #     create_table(session, schema='HARMONIZED', name='AVOCET_WELL_STATS')

    update_table(session)

    return f"Successfully processed UWI_RPC table."


# For local debugging
# Be aware you may need to type-convert arguments if you add input parameters
if __name__ == '__main__':
    # Add the utils package to our path and import the snowpark_utils function
    import os, sys
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)
    parent_parent_dir = os.path.dirname(os.path.dirname(current_dir))
    sys.path.append(parent_parent_dir)


    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()

    if len(sys.argv) > 1:
        print(main(session, *sys.argv[1:]))  # type: ignore
    else:
        print(main(session))  # type: ignore

    session.close()
