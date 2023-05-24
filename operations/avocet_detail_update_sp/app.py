
from datetime import datetime, timedelta
from snowflake.snowpark import Session
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F


def table_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    return exists

# def create_table(session):
#     _ = session.sql("CREATE TABLE HARMONIZED.AVOCET_PROD_DETAIL LIKE HARMONIZED.AVOCET_PROD_DETAIL_V").collect()
    
def create_table(session):
    SHARED_COLUMNS= [
                        T.StructField("UWI", T.StringType()),
                        T.StructField("PRODUCTION_DATE", T.DateType()),
                        T.StructField("AREA", T.StringType()),
                        T.StructField("MAJOR_AREA", T.StringType()),
                        T.StructField("TEAM", T.StringType()),
                        T.StructField("CC_NUM", T.StringType()),
                        T.StructField("MONTH_START", T.DateType()),
                        T.StructField("SITE_ID", T.StringType()),
                        T.StructField("HOURS_ON_PROD", T.DecimalType()),
                        T.StructField("OIL_GROSS_M3", T.DecimalType()),
                        T.StructField("GAS_GROSS_E3M3", T.DecimalType()),
                        T.StructField("COND_GROSS_M3", T.DecimalType()),
                        T.StructField("WATER_GROSS_M3", T.DecimalType()),
                        T.StructField("OIL_ALLOC_M3", T.DecimalType()),
                        T.StructField("GAS_ALLOC_E3M3", T.DecimalType()),
                        T.StructField("SALES_GAS_ALLOC_E3M3", T.DecimalType()),
                        T.StructField("COND_ALLOC_M3", T.DecimalType()),
                        T.StructField("WATER_ALLOC_M3", T.DecimalType()),
                        T.StructField("CASING_PRESSURE_KPA", T.DecimalType()),
                        T.StructField("TUBING_PRESSURE_KPA", T.DecimalType()),
                        T.StructField("WELL_HEAD_PRESSURE_KPA", T.DecimalType()),
                        T.StructField("HOURS_ON", T.DecimalType()),
                        T.StructField("HOURS_DOWN", T.DecimalType()),
                        T.StructField("WORK_INTEREST", T.DecimalType()),
                        T.StructField("GAS_SHRINK", T.DecimalType()),
                        T.StructField("NGL_YIELD_FACTOR", T.DecimalType()),
                        T.StructField("OIL_NET_M3", T.DecimalType()),
                        T.StructField("GAS_NET_E3M3", T.DecimalType()),
                        T.StructField("COND_NET_M3", T.DecimalType()),
                        T.StructField("WATER_NET_M3", T.DecimalType()),
                        T.StructField("OIL_SALES_M3", T.DecimalType()),
                        T.StructField("GAS_SALES_E3M3", T.DecimalType()),
                        T.StructField("COND_SALES_M3", T.DecimalType()),
                        T.StructField("WATER_SALES_M3", T.DecimalType()),
                        T.StructField("OIL_GROSS_BBL", T.DecimalType()),
                        T.StructField("OIL_NET_BBL", T.DecimalType()),
                        T.StructField("OIL_ALLOC_BBL", T.DecimalType()),
                        T.StructField("OIL_SALES_BBL", T.DecimalType()),
                        T.StructField("COND_GROSS_BBL", T.DecimalType()),
                        T.StructField("COND_NET_BBL", T.DecimalType()),
                        T.StructField("COND_ALLOC_BBL", T.DecimalType()),
                        T.StructField("COND_SALES_BBL", T.DecimalType()),
                        T.StructField("WATER_GROSS_BBL", T.DecimalType()),
                        T.StructField("WATER_NET_BBL", T.DecimalType()),
                        T.StructField("WATER_ALLOC_BBL", T.DecimalType()),
                        T.StructField("WATER_SALES_BBL", T.DecimalType()),
                        T.StructField("GAS_GROSS_MCF", T.DecimalType()),
                        T.StructField("GAS_NET_MCF", T.DecimalType()),
                        T.StructField("GAS_ALLOC_MCF", T.DecimalType()),
                        T.StructField("GAS_SALES_MCF", T.DecimalType()),
                        T.StructField("SALES_GAS_ALLOC_MCF", T.DecimalType()),
                        T.StructField("CASING_PRESSURE_PSI", T.DecimalType()),
                        T.StructField("TUBING_PRESSURE_PSI", T.DecimalType()),
                        T.StructField("WELL_HEAD_PRESSURE_PSI", T.DecimalType()),
                        T.StructField("GAS_GROSS_BOE", T.DecimalType()),
                        T.StructField("GAS_NET_BOE", T.DecimalType()),
                        T.StructField("GAS_ALLOC_BOE", T.DecimalType()),
                        T.StructField("GAS_SALES_BOE", T.DecimalType()),
                        T.StructField("SALES_GAS_ALLOC_BOE", T.DecimalType()),
                        T.StructField("NGL_GROSS_BBL", T.DecimalType()),
                        T.StructField("NGL_NET_BBL", T.DecimalType()),
                        T.StructField("NGL_SALES_BBL", T.DecimalType()),
                        T.StructField("NGL_GROSS_M3", T.DecimalType()),
                        T.StructField("NGL_NET_M3", T.DecimalType()),
                        T.StructField("NGL_SALES_M3", T.DecimalType()),
                        T.StructField("GROSS_BOE", T.DecimalType()),
                        T.StructField("NET_BOE", T.DecimalType()),
                        T.StructField("SALES_BOE", T.DecimalType()),
                        T.StructField("GROSS_M3E", T.DecimalType()),
                        T.StructField("NET_M3E", T.DecimalType()),
                        T.StructField("SALES_M3E", T.DecimalType()),
                        T.StructField("GROSS_BOE_MONTH_TOTAL", T.DecimalType()),
                        T.StructField("GROSS_BOE_MONTHLY_PERC", T.DecimalType()),
                        T.StructField("HOURS_AVAILABLE", T.DecimalType()),
                        T.StructField("DOWNTIME_NAME", T.StringType()),
                        T.StructField("PRODUCTION_DATE_SFM", T.DateType()),
                        T.StructField("NETBACK", T.DecimalType()),
                        T.StructField("LIFTING_COST", T.DecimalType()),
                        T.StructField("CC_WELL_COUNT", T.IntegerType()),
                        T.StructField("FIXED_WELL", T.DecimalType()),
                        T.StructField("VARIABLE_WELL", T.DecimalType()),
                        T.StructField("R&M_WELL", T.DecimalType()),
                        T.StructField("REVENUE_WELL", T.DecimalType()),
                        T.StructField("ROYALTIES_WELL", T.DecimalType()),
                        T.StructField("REVENUE_ROYALTIES_WELL", T.DecimalType()),
                        T.StructField("OPERATING_COSTS_WELL", T.DecimalType()),
                        T.StructField("NET_OPERATING_INCOME_WELL", T.DecimalType()), 
                        T.StructField("BOE_WELL", T.DecimalType()),
                    ]
    
    AVOCET_PROD_DETAIL_COLUMNS = [*SHARED_COLUMNS, T.StructField("META_UPDATED_AT", T.TimestampType())]
    AVOCET_PROD_DETAIL_SCHEMA = T.StructType(AVOCET_PROD_DETAIL_COLUMNS)

    dcm = session.create_dataframe([[None]*len(AVOCET_PROD_DETAIL_SCHEMA.names)], schema=AVOCET_PROD_DETAIL_SCHEMA) \
                        .na.drop() \
                        .write.mode('overwrite').save_as_table('HARMONIZED.AVOCET_PROD_DETAIL')
    dcm = session.table('HARMONIZED.AVOCET_PROD_DETAIL')

def update_table(session):
    
    
    update_or_overwrite = 'UPDATE' # 'UPDATE' or 'OVERWRITE'
    
    print(f"Starting AVOCET_PROD_DETAIL: {update_or_overwrite}")
    
    if update_or_overwrite == 'OVERWRITE':
        _ = session.sql('ALTER WAREHOUSE CDE_WH SET WAREHOUSE_SIZE = MEDIUM WAIT_FOR_COMPLETION = TRUE').collect()
    
    if update_or_overwrite == 'OVERWRITE':
        print("Resetting Stream")
        session.sql("CREATE OR REPLACE STREAM RAW_AVOCET.AVOCET_WELL_PROD_STREAM ON TABLE RAW_AVOCET.TDW_FV_DAILY_WELL_PROD SHOW_INITIAL_ROWS = TRUE").collect()
        print("{} records in stream".format(session.table('RAW_AVOCET.AVOCET_WELL_PROD_STREAM').count()))
    
    if update_or_overwrite == 'UPDATE':
        print("{} records in stream".format(session.table('RAW_AVOCET.AVOCET_WELL_PROD_STREAM').count()))
        avocet_prod_stream_dates = session.table('RAW_AVOCET.AVOCET_WELL_PROD_STREAM').select(F.col("PRODUCTION_DATE").alias("DATE")).distinct()
        avocet_prod_stream_dates.limit(5).show()
    
    corpwell = session.table('RAW_TABLEAU_PASSTHROUGH.CORPWELL').select(F.col("UWI_ID").collate('en_us').alias("UWI"), \
                                                                            F.col("TEAM"), \
                                                                            F.col('MAJOR_AREA'), \
                                                                            F.col("AREA"), \
                                                                            F.col("QBYTE_CC_NUM_CLEAN").alias("CC_NUM"), \
                                                                            )
    
    simple_financial_metrics = session.table("HARMONIZED.SIMPLE_FINANCIAL_METRICS").select(F.col("CC_NUM").collate('en_us').alias("CC_NUM"), \
                                                                                        F.col("DATE").alias("PRODUCTION_DATE"), \
                                                                                        F.col("NETBACK"), \
                                                                                        F.col("LIFTING_COST"), \
                                                                                        F.col("FIXED").alias("FIXED_CC"), \
                                                                                        F.col("VARIABLE").alias("VARIABLE_CC"), \
                                                                                        F.col("R&M").alias("R&M_CC"), \
                                                                                        F.col("REVENUE").alias("REVENUE_CC"), \
                                                                                        F.col("ROYALTIES").alias("ROYALTIES_CC"), \
                                                                                        F.col("REVENUE_ROYALTIES").alias("REVENUE_ROYALTIES_CC"), \
                                                                                        F.col("OPERATING_COSTS").alias("OPERATING_COSTS_CC"), \
                                                                                        F.col("NET_OPERATING_INCOME").alias("NET_OPERATING_INCOME_CC"), \
                                                                                        F.col("BOE").alias("BOE_CC") \
                                                                                        ) \
                                                                                    .with_column("MONTH_START", F.call_builtin("DATE_TRUNC", F.lit("MONTH"), F.col("PRODUCTION_DATE")))
                                                                                    
    
    fv_ops_flat_list = session.table("RAW_AVOCET.V_FV_OPS_FLAT_LIST").select(F.col("AREA_NAME"), \
                                                                F.col("FOLDER_NAME"), \
                                                                F.col("FIELD_NAME"), \
                                                                F.col("SITE_ID"))
    
    tdw_fv_site = session.table("RAW_AVOCET.TDW_FV_SITE").select(F.col("SITE_ID"), \
                                                                F.col("NAME"), \
                                                                F.col("LOCATION"), \
                                                                F.col("TYPE"))
    
    
    tdw_fv_daily_well_prod = session.table("RAW_AVOCET.AVOCET_WELL_PROD_STREAM") \
                                    .select(F.col("SITE_ID"), \
                                            F.col("PRODUCTION_DATE"), \
                                            F.col("HOURS_ON_PROD"), \
                                            F.col("OIL_GROSS").alias("OIL_GROSS_M3"), \
                                            F.col("GAS_GROSS").alias("GAS_GROSS_E3M3"), \
                                            F.col("COND_GROSS").alias("COND_GROSS_M3"), \
                                            F.col("WATER_GROSS").alias("WATER_GROSS_M3"), \
                                            F.col("OIL_ALLOC").alias("OIL_ALLOC_M3"), \
                                            F.col("GAS_ALLOC").alias("GAS_ALLOC_E3M3"), \
                                            F.col("SALES_GAS_ALLOC").alias("SALES_GAS_ALLOC_E3M3"), \
                                            F.col("COND_ALLOC").alias("COND_ALLOC_M3"), \
                                            F.col("WATER_ALLOC").alias("WATER_ALLOC_M3"), \
                                            F.col("WORK_INTEREST"), \
                                            F.col("GAS_SHRINK"), \
                                            F.col("NGL_YIELD_FACTOR"), \
                                            F.col("CASING_PRESSURE").alias("CASING_PRESSURE_KPA"), \
                                            F.col("TUBING_PRESSURE").alias("TUBING_PRESSURE_KPA"), \
                                            F.col("WELL_HEAD_PRESSURE").alias("WELL_HEAD_PRESSURE_KPA") \
                                            ,F.col("METADATA$ACTION") \
                                            ,F.col("METADATA$ISUPDATE") \
                                            ) \
                                    .where( \
                                        (F.col("COND_GROSS_M3") <= 10000) & \
                                        (F.col("PRODUCTION_DATE") > F.lit('2018-01-01')) & \
                                        ((F.col("METADATA$ACTION") == F.lit('INSERT')) |  (F.col("METADATA$ISUPDATE") == True)) \
                                            ) \
                                    .with_column("HOURS_ON", F.call_builtin("ZEROIFNULL", F.col("HOURS_ON_PROD"))) \
                                    .with_column("HOURS_DOWN", 24 - F.col("HOURS_ON_PROD")) \
                                    .with_column("WORK_INTEREST", F.call_builtin("ZEROIFNULL", F.col("WORK_INTEREST"))) \
                                    .with_column("GAS_SHRINK", F.call_builtin("ZEROIFNULL", F.col("GAS_SHRINK"))) \
                                    .with_column("NGL_YIELD_FACTOR", F.call_builtin("ZEROIFNULL", F.col("NGL_YIELD_FACTOR"))) \
                                    .with_column("OIL_NET_M3", F.call_builtin("ZEROIFNULL", F.col("OIL_GROSS_M3") * F.col("WORK_INTEREST") * 0.01)) \
                                    .with_column("GAS_NET_E3M3", F.call_builtin("ZEROIFNULL", F.col("GAS_GROSS_E3M3") * F.col("WORK_INTEREST") * 0.01)) \
                                    .with_column("COND_NET_M3", F.call_builtin("ZEROIFNULL", F.col("COND_GROSS_M3") * F.col("WORK_INTEREST") * 0.01)) \
                                    .with_column("WATER_NET_M3", F.call_builtin("ZEROIFNULL", F.col("WATER_GROSS_M3") * F.col("WORK_INTEREST") * 0.01)) \
                                    .with_column("OIL_SALES_M3", F.call_udf("HARMONIZED.GROSS_TO_SALES_LIQUIDS_UDF", F.col("OIL_ALLOC_M3"), F.col("OIL_GROSS_M3"), F.col("WORK_INTEREST"))) \
                                    .with_column("GAS_SALES_E3M3", F.call_udf("HARMONIZED.GROSS_TO_SALES_GAS_UDF", F.col("SALES_GAS_ALLOC_E3M3"), F.col("GAS_GROSS_E3M3"), F.col("WORK_INTEREST"), F.col("GAS_SHRINK"))) \
                                    .with_column("COND_SALES_M3", F.call_udf("HARMONIZED.GROSS_TO_SALES_LIQUIDS_UDF", F.col("COND_ALLOC_M3"), F.col("COND_GROSS_M3"), F.col("WORK_INTEREST"))) \
                                    .with_column("WATER_SALES_M3", F.call_udf("HARMONIZED.GROSS_TO_SALES_LIQUIDS_UDF", F.col("WATER_ALLOC_M3"), F.col("WATER_GROSS_M3"), F.col("WORK_INTEREST"))) \
                                    .with_column("OIL_GROSS_BBL", F.call_udf("HARMONIZED.M3_TO_BBL_UDF", F.col("OIL_GROSS_M3"))) \
                                    .with_column("OIL_NET_BBL", F.call_udf("HARMONIZED.M3_TO_BBL_UDF", F.col("OIL_NET_M3"))) \
                                    .with_column("OIL_ALLOC_BBL", F.call_udf("HARMONIZED.M3_TO_BBL_UDF", F.col("OIL_ALLOC_M3"))) \
                                    .with_column("OIL_SALES_BBL", F.call_udf("HARMONIZED.M3_TO_BBL_UDF", F.col("OIL_SALES_M3"))) \
                                    .with_column("COND_GROSS_BBL", F.call_udf("HARMONIZED.M3_TO_BBL_UDF", F.col("COND_GROSS_M3"))) \
                                    .with_column("COND_NET_BBL", F.call_udf("HARMONIZED.M3_TO_BBL_UDF", F.col("COND_NET_M3"))) \
                                    .with_column("COND_ALLOC_BBL", F.call_udf("HARMONIZED.M3_TO_BBL_UDF", F.col("COND_ALLOC_M3"))) \
                                    .with_column("COND_SALES_BBL", F.call_udf("HARMONIZED.M3_TO_BBL_UDF", F.col("COND_SALES_M3"))) \
                                    .with_column("WATER_GROSS_BBL", F.call_udf("HARMONIZED.M3_TO_BBL_UDF", F.col("WATER_GROSS_M3"))) \
                                    .with_column("WATER_NET_BBL", F.call_udf("HARMONIZED.M3_TO_BBL_UDF", F.col("WATER_NET_M3"))) \
                                    .with_column("WATER_ALLOC_BBL", F.call_udf("HARMONIZED.M3_TO_BBL_UDF", F.col("WATER_ALLOC_M3"))) \
                                    .with_column("WATER_SALES_BBL", F.call_udf("HARMONIZED.M3_TO_BBL_UDF", F.col("WATER_SALES_M3"))) \
                                    .with_column("GAS_GROSS_MCF", F.call_udf("HARMONIZED.E3M3_TO_MCF_UDF", F.col("GAS_GROSS_E3M3"))) \
                                    .with_column("GAS_NET_MCF", F.call_udf("HARMONIZED.E3M3_TO_MCF_UDF", F.col("GAS_NET_E3M3"))) \
                                    .with_column("GAS_ALLOC_MCF", F.call_udf("HARMONIZED.E3M3_TO_MCF_UDF", F.col("GAS_ALLOC_E3M3"))) \
                                    .with_column("GAS_SALES_MCF", F.call_udf("HARMONIZED.E3M3_TO_MCF_UDF", F.col("GAS_SALES_E3M3"))) \
                                    .with_column("SALES_GAS_ALLOC_MCF", F.call_udf("HARMONIZED.E3M3_TO_MCF_UDF", F.col("SALES_GAS_ALLOC_E3M3"))) \
                                    .with_column("CASING_PRESSURE_PSI", F.call_udf("HARMONIZED.KPA_TO_PSI_UDF", F.col("CASING_PRESSURE_KPA"))) \
                                    .with_column("TUBING_PRESSURE_PSI", F.call_udf("HARMONIZED.KPA_TO_PSI_UDF", F.col("TUBING_PRESSURE_KPA"))) \
                                    .with_column("WELL_HEAD_PRESSURE_PSI", F.call_udf("HARMONIZED.KPA_TO_PSI_UDF", F.col("WELL_HEAD_PRESSURE_KPA"))) \
                                    .with_column("GAS_GROSS_BOE", F.call_udf("HARMONIZED.E3M3_TO_BOE_UDF", F.col("GAS_GROSS_E3M3"))) \
                                    .with_column("GAS_NET_BOE", F.call_udf("HARMONIZED.E3M3_TO_BOE_UDF", F.col("GAS_NET_E3M3"))) \
                                    .with_column("GAS_ALLOC_BOE", F.call_udf("HARMONIZED.E3M3_TO_BOE_UDF", F.col("GAS_ALLOC_E3M3"))) \
                                    .with_column("GAS_SALES_BOE", F.call_udf("HARMONIZED.E3M3_TO_BOE_UDF", F.col("GAS_SALES_E3M3"))) \
                                    .with_column("SALES_GAS_ALLOC_BOE", F.call_udf("HARMONIZED.E3M3_TO_BOE_UDF", F.col("SALES_GAS_ALLOC_E3M3"))) \
                                    .with_column("NGL_GROSS_BBL", F.call_udf("HARMONIZED.GAS_NET_MCF_TO_GROSS_NGL_BBL_UDF", F.col("GAS_NET_MCF"), F.col("NGL_YIELD_FACTOR"), F.col("WORK_INTEREST"))) \
                                    .with_column("NGL_NET_BBL", F.col("NGL_GROSS_BBL") * F.col("WORK_INTEREST") * 0.01) \
                                    .with_column("NGL_SALES_BBL", F.col("NGL_GROSS_BBL") * F.col("WORK_INTEREST") * 0.01) \
                                    .with_column("NGL_GROSS_M3", F.call_udf("HARMONIZED.BBL_TO_M3_UDF", F.col("NGL_GROSS_BBL"))) \
                                    .with_column("NGL_NET_M3", F.call_udf("HARMONIZED.BBL_TO_M3_UDF", F.col("NGL_NET_BBL"))) \
                                    .with_column("NGL_SALES_M3", F.call_udf("HARMONIZED.BBL_TO_M3_UDF", F.col("NGL_SALES_BBL"))) \
                                    .with_column("GROSS_BOE", F.col("GAS_GROSS_BOE") + F.col("NGL_GROSS_BBL") + F.col("COND_GROSS_BBL") + F.col("OIL_GROSS_BBL")) \
                                    .with_column("NET_BOE", F.col("GAS_NET_BOE") + F.col("NGL_NET_BBL") + F.col("COND_NET_BBL") + F.col("OIL_NET_BBL")) \
                                    .with_column("SALES_BOE", F.col("GAS_SALES_BOE") + F.col("NGL_SALES_BBL") + F.col("COND_SALES_BBL") + F.col("OIL_SALES_BBL")) \
                                    .with_column("GROSS_M3E", F.call_udf("HARMONIZED.BBL_TO_M3_UDF", F.col("GROSS_BOE"))) \
                                    .with_column("NET_M3E", F.call_udf("HARMONIZED.BBL_TO_M3_UDF", F.col("NET_BOE"))) \
                                    .with_column("SALES_M3E", F.call_udf("HARMONIZED.BBL_TO_M3_UDF", F.col("SALES_BOE"))) \
                                    .with_column("MONTH_START", F.call_builtin("DATE_TRUNC", F.lit("MONTH"), F.col("PRODUCTION_DATE"))) \
                                    .drop("METADATA$ACTION") \
                                    .drop("METADATA$ISUPDATE")

    print(tdw_fv_daily_well_prod.limit(5).show())   
    
    avocet_accounting_volumes = session.table("HARMONIZED.AVOCET_ACCOUNTING_VOLUMES")
    
    tdw_fv_daily_well_prod = tdw_fv_daily_well_prod.join(avocet_accounting_volumes, on=["SITE_ID", "PRODUCTION_DATE"], how="left", rsuffix="_av")
                                     
                                    
    monthly_production_totals = tdw_fv_daily_well_prod.groupBy("SITE_ID", "MONTH_START").agg(F.sum("GROSS_BOE").alias("GROSS_BOE_MONTH_TOTAL"))                                       
                                     
    tdw_fv_daily_well_prod = tdw_fv_daily_well_prod.join(monthly_production_totals, on=["SITE_ID", "MONTH_START"], how="left", rsuffix="_mt") \
                                    .with_column("GROSS_BOE_MONTHLY_PERC", F.call_builtin("IFF", F.col("GROSS_BOE_MONTH_TOTAL") == 0, 0, F.col("GROSS_BOE") / F.col("GROSS_BOE_MONTH_TOTAL")))                                                                   
    
    tdw_fv_daily_lost_prod = session.table("RAW_AVOCET.TDW_FV_DAILY_LOST_PROD").select(F.col("SITE_ID"),
                                                                        F.col("HOURS_AVAILABLE"),
                                                                        F.col("PRODUCTION_DATE"),
                                                                        F.col("DOWNTIME_NAME"))
    
    
    
    joined = fv_ops_flat_list.join(tdw_fv_site, on="SITE_ID", how="inner").select(F.col("SITE_ID"), \
                                                                            F.col("LOCATION").alias("UWI"))
                                                                            # F.col("NAME").alias("WELL_NAME"), \
                                                                            # F.col("TYPE").alias("WELL_TYPE"), \
                                                                            # F.col("FOLDER_NAME").alias("FIELD_NAME"), \
                                                                            # F.col("AREA_NAME").alias("BUSINESS_UNIT_NAME"))
    
    
    joined = joined.join(corpwell, on="UWI", how="left", rsuffix="_cw")
    
    joined = joined.join(tdw_fv_daily_well_prod, on="SITE_ID", how="inner")
    
    joined = joined.join(tdw_fv_daily_lost_prod, on=["SITE_ID", "PRODUCTION_DATE"], how="left", rsuffix="_lp")

    # print(simple_financial_metrics.limit(5).show())
    
    joined = joined.join(simple_financial_metrics, on=["CC_NUM", 'MONTH_START'], how="left", rsuffix="_sfm")
        
    # print(joined.limit(5).show())
    
    # count the number of UWI's for each CC_NUM and PRODUCTION_DATE
    cc_well_counts = joined.groupBy("CC_NUM", "PRODUCTION_DATE").count()
         
    # add wellcount column to combined_data as CC_WELL_COUNT
    joined = joined.join(cc_well_counts, on=["CC_NUM", "PRODUCTION_DATE"], how="left")
    
    # add the well count to the combined_data table
    joined = joined.with_column("CC_WELL_COUNT", F.call_builtin("ZEROIFNULL", F.col("count")))
    
    # drop the count column
    joined = joined.drop("count")
    
    # print(joined.limit(5).show())
    
    
    joined = joined \
        .with_column("FIXED_WELL", F.call_builtin("IFF", F.col("CC_WELL_COUNT") == 0, 0, F.col("FIXED_CC") * F.col("GROSS_BOE_MONTHLY_PERC") /  F.col("CC_WELL_COUNT"))) \
        .with_column("VARIABLE_WELL", F.call_builtin("IFF", F.col("CC_WELL_COUNT") == 0, 0, F.col("VARIABLE_CC") * F.col("GROSS_BOE_MONTHLY_PERC") / F.col("CC_WELL_COUNT"))) \
        .with_column("R&M_WELL", F.call_builtin("IFF", F.col("CC_WELL_COUNT") == 0, 0, F.col("R&M_CC") * F.col("GROSS_BOE_MONTHLY_PERC") / F.col("CC_WELL_COUNT"))) \
        .with_column("REVENUE_WELL", F.call_builtin("IFF", F.col("CC_WELL_COUNT") == 0, 0, F.col("REVENUE_CC") * F.col("GROSS_BOE_MONTHLY_PERC") / F.col("CC_WELL_COUNT"))) \
        .with_column("ROYALTIES_WELL", F.call_builtin("IFF", F.col("CC_WELL_COUNT") == 0, 0, F.col("ROYALTIES_CC") * F.col("GROSS_BOE_MONTHLY_PERC") / F.col("CC_WELL_COUNT"))) \
        .with_column("REVENUE_ROYALTIES_WELL", F.call_builtin("IFF", F.col("CC_WELL_COUNT") == 0, 0, F.col("REVENUE_ROYALTIES_CC") * F.col("GROSS_BOE_MONTHLY_PERC") / F.col("CC_WELL_COUNT"))) \
        .with_column("OPERATING_COSTS_WELL", F.call_builtin("IFF", F.col("CC_WELL_COUNT") == 0, 0, F.col("OPERATING_COSTS_CC") * F.col("GROSS_BOE_MONTHLY_PERC") / F.col("CC_WELL_COUNT"))) \
        .with_column("NET_OPERATING_INCOME_WELL", F.call_builtin("IFF", F.col("CC_WELL_COUNT") == 0, 0, F.col("NET_OPERATING_INCOME_CC") * F.col("GROSS_BOE_MONTHLY_PERC") / F.col("CC_WELL_COUNT"))) \
        .with_column("BOE_WELL", F.call_builtin("IFF", F.col("CC_WELL_COUNT") == 0, 0, F.col("BOE_CC") * F.col("GROSS_BOE_MONTHLY_PERC") / F.col("CC_WELL_COUNT"))) \
        .drop("FIXED_CC", "VARIABLE_CC", "R&M_CC", "REVENUE_CC", "ROYALTIES_CC", "REVENUE_ROYALTIES_CC", "OPERATING_COSTS_CC", "NET_OPERATING_INCOME_CC", "BOE_CC")


    if update_or_overwrite == 'UPDATE':
        cols_to_update = {c: joined[c] for c in joined.schema.names}
        metadata_col_to_update = {"META_UPDATED_AT": F.current_timestamp()}
        updates = {
            **cols_to_update,
            **metadata_col_to_update
            }

        sf_table = session.table('HARMONIZED.AVOCET_PROD_DETAIL')
        sf_table.merge(joined, (sf_table['PRODUCTION_DATE'] == joined['PRODUCTION_DATE']) & (sf_table['UWI'] == joined['UWI']), \
                            [F.when_matched().update(updates), F.when_not_matched().insert(updates)])

    if update_or_overwrite == 'OVERWRITE':
        #add a blank column for the metadata
        joined = joined.with_column("META_UPDATED_AT", F.lit(None).cast("timestamp"))
    # overwrite the table 
        joined.write.mode("overwrite").saveAsTable("HARMONIZED.AVOCET_PROD_DETAIL")
    
    if update_or_overwrite == 'OVERWRITE':
        _ = session.sql('ALTER WAREHOUSE CDE_WH SET WAREHOUSE_SIZE = XSMALL').collect()

def main(session: Session) -> str:
        
    # Create the AVOCET_PROD_DETAIL table  if it doesn't exist
    if not table_exists(session, schema='HARMONIZED', name='AVOCET_PROD_DETAIL'):
        create_table(session)

    update_table(session)


    return f"Successfully processed AVOCET_PROD_DETAIL table."


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
