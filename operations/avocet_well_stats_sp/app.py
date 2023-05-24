
from datetime import datetime, timedelta
import pandas as pd
from snowflake.snowpark import Session
#import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F


def table_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    return exists

def update_table(session):
    
    session.use_schema('HARMONIZED')
    use_wh_scaling = False
    pass_through_east_values = True
    
    if use_wh_scaling:
        _ = session.sql('ALTER WAREHOUSE CDE_WH SET WAREHOUSE_SIZE = MEDIUM WAIT_FOR_COMPLETION = TRUE').collect()
    
    # select data where date > 365 days ago
    df = session.table("HARMONIZED.AVOCET_PROD_DETAIL") \
        .filter((F.col("PRODUCTION_DATE") >= F.lit(datetime.now() - timedelta(days=365))) \
            # & (F.col('TEAM') != F.lit("EAST")) # we will be getting rid of this filter eventually, will want to scale up to a larger warehouse to process all 7000 wells (939 excludes EAST)
                ) \
        .sort(F.col("UWI"), F.col("PRODUCTION_DATE")) \
        .to_pandas()
        
    print(df.head())
    
    current_stats_east_df = session.table("HARMONIZED.AVOCET_WELL_STATS").filter(F.col('TEAM') == F.lit("EAST")).to_pandas()
    
    # for every unique UWI, calculate the most recent 30 day average of GROSS_BOE  - add to df
    output_df = pd.DataFrame()
    
    # if it is january 1 or july 1, set pass_through_east_values to False
    if datetime.now().month in [1, 7] and datetime.now().day == 1:
        pass_through_east_values = False
    
    
    well_count = len(df["UWI"].unique())
    # if df['TEAM']== 'EAST' and pass_through_east_values = True
    # add the current values from current_stats_east_df to the output_df
    # then remove all EAST wells from df
    if pass_through_east_values:
        print('Passing previously calculated values for EAST wells through...')
        output_df = current_stats_east_df
        df = df[df['TEAM'] != 'EAST']
    
    
        
    
    
    for uwi in df['UWI'].unique():

        # carry through TEAM, MAJOR_AREA, AREA, CC_NUM
        uwi_row = {'UWI': uwi, 'TEAM': df[df['UWI'] == uwi]['TEAM'].iloc[0], 'MAJOR_AREA': df[df['UWI'] == uwi]['MAJOR_AREA'].iloc[0], 'AREA': df[df['UWI'] == uwi]['AREA'].iloc[0], 'CC_NUM': df[df['UWI'] == uwi]['CC_NUM'].iloc[0]}
            
        df_uwi_full = df[df['UWI'] == uwi].sort_values('PRODUCTION_DATE', ascending=False)
        
        # Most recent value of columns with GROSS, NET, or SALES in the name
        for col in [col for col in df.columns if any(x in col for x in ['GROSS', 'NET', 'SALES'])]:
            # calculate the average of the most recent x days of GROSS_BOE for each UWI
            most_recent = df_uwi_full[col].iloc[0]
            # add the average to the uwi_row
            uwi_row[col + '_MOST_RECENT'] = most_recent

        uwi_row['NET_GROSS_FACTOR'] = uwi_row['NET_BOE_MOST_RECENT'] / uwi_row['GROSS_BOE_MOST_RECENT'] if uwi_row['GROSS_BOE_MOST_RECENT'] != 0 else 0
        uwi_row['SALES_GROSS_FACTOR'] = uwi_row['SALES_BOE_MOST_RECENT'] / uwi_row['GROSS_BOE_MOST_RECENT'] if uwi_row['GROSS_BOE_MOST_RECENT'] != 0 else 0
        
        windows = [30, 60, 90, 180, 270, 365]
        
        for window in windows:
            # get the most recent 30 days of GROSS_BOE for each UWI
            df_uwi = df_uwi_full.head(window)
            
            ### GROSS COLUMN AVERAGES
            for col in [col for col in df.columns if 'GROSS' in col]:
                # calculate the average of the most recent x days of GROSS_BOE for each UWI
                avg = df_uwi[col].mean()
                
                # add the metric to the uwi_row
                uwi_row[col + f'_{window}D_AVG'] = avg
                
                # 50th Percentile 
                if window == 90 :
                    # calculate the 50th percentile of the most recent xdays of NETBACK for each UWI
                    percentile = df_uwi[col].quantile(0.50)
                    # add the 75th percentile to the uwi_row
                    uwi_row[col + f'_{window}D_50P'] = percentile
                
                # RPC Calculation
                if window == 270:
                    # calculate the 75th percentile of the most recent xdays of GROSS_BOE for each UWI
                    percentile = df_uwi[col].quantile(0.65)
                    # add the 75th percentile to the uwi_row
                    uwi_row[col + f'_{window}D_RPC'] = percentile
                    
                    

            ### OTHER COLUMN AVERAGES
            for col in [
                'HOURS_ON',
                'HOURS_DOWN',
                'HOURS_AVAILABLE'
                ]:
                                
                # calculate the average of the most recent x days of GROSS_BOE for each UWI
                avg = df_uwi[col].mean()
                # add the average to the uwi_row
                uwi_row[col + f'_{window}D_AVG'] = avg
                
                # 50th Percentile 
                if window == 90 :
                    # calculate the 50th percentile of the most recent xdays of NETBACK for each UWI
                    percentile = df_uwi[col].quantile(0.50)
                    # add the 75th percentile to the uwi_row
                    uwi_row[col + f'_{window}D_50P'] = percentile
                
                
            ### Financial and Metrics
            for col in [
                'OPERATING_COSTS_WELL',
                'REVENUE_ROYALTIES_WELL',
                'NET_OPERATING_INCOME_WELL',
                ]:
                
                uwi_row[col + f'_{window}D_AVG'] = df_uwi[col].mean() # values present only monthly

            sales_boe = uwi_row['GROSS_BOE' + f'_{window}D_AVG'] * uwi_row['SALES_GROSS_FACTOR']
            uwi_row['LIFTING_COST' + f'_{window}D_AVG'] = uwi_row['OPERATING_COSTS_WELL' + f'_{window}D_AVG'] / sales_boe if sales_boe != 0 else 0
            uwi_row['NETBACK' + f'_{window}D_AVG'] = uwi_row['NET_OPERATING_INCOME_WELL' + f'_{window}D_AVG'] / sales_boe if sales_boe != 0 else 0

            
            
            ### OTHER COLUMN SUMS
            for col in ['HOURS_DOWN']:
                # calculate the avg of each column 
                col_sum = df_uwi[col].sum()
                # add the average to the uwi_row
                uwi_row[col + f'_{window}D_SUM'] = col_sum
        
        

            
        # write the uwi_row to the output_df using concat
        output_df = pd.concat([output_df, pd.DataFrame(uwi_row, index=[0])])
            
        print(f'{output_df.shape[0]}/{well_count} processed')
        
    print(output_df.head())
    
    
    session.write_pandas(output_df, "AVOCET_WELL_STATS", auto_create_table=True, overwrite=True )
    
    if use_wh_scaling:
        _ = session.sql('ALTER WAREHOUSE CDE_WH SET WAREHOUSE_SIZE = XSMALL').collect()
    



def main(session: Session) -> str:
    # Create the WELLS_HISTORIAN table  if they don't exist
    # if not table_exists(session, schema='HARMONIZED', name='AVOCET_WELL_STATS'):
    #     create_table(session, schema='HARMONIZED', name='AVOCET_WELL_STATS')

    update_table(session)

    return f"Successfully processed AVOCET_WELL_STATS table."


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
