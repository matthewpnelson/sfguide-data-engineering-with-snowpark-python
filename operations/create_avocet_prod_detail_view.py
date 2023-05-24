from snowflake.snowpark import Session
#import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F


def create_pos_view(session):
    # session.use_schema('RAW_AVOCET')
    
    fv_ops_flat_list = session.table("RAW_AVOCET.V_FV_OPS_FLAT_LIST").select(F.col("AREA_NAME"), \
                                                                F.col("FOLDER_NAME"), \
                                                                F.col("FIELD_NAME"), \
                                                                F.col("SITE_ID"))
    
    tdw_fv_site = session.table("RAW_AVOCET.TDW_FV_SITE").select(F.col("SITE_ID"), \
                                                    F.col("NAME"), \
                                                    F.col("LOCATION"), \
                                                    F.col("TYPE"))
    
    tdw_fv_daily_well_prod = session.table("RAW_AVOCET.TDW_FV_DAILY_WELL_PROD") \
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
                                            F.col("WELL_HEAD_PRESSURE").alias("WELL_HEAD_PRESSURE_KPA")) \
                                    .where(F.col("COND_GROSS_M3") <= 10000) \
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
                                    .with_column("SALES_BOE", F.col("GAS_SALES_BOE") + F.col("NGL_SALES_BBL") + F.col("COND_SALES_BBL") + F.col("OIL_SALES_BBL"))
                                    
                                    
                                            
                                                                            
    
    tdw_fv_daily_lost_prod = session.table("RAW_AVOCET.TDW_FV_DAILY_LOST_PROD").select(F.col("SITE_ID"),
                                                                        F.col("HOURS_AVAILABLE"),
                                                                        F.col("PRODUCTION_DATE"),
                                                                        F.col("DOWNTIME_NAME"))
    
    
    
    j1 = fv_ops_flat_list.join(tdw_fv_site, on="SITE_ID", how="inner").select(F.col("SITE_ID"), \
                                                                            F.col("NAME").alias("WELL_NAME"), \
                                                                            F.col("TYPE").alias("WELL_TYPE"), \
                                                                            F.col("LOCATION").alias("UWI"), \
                                                                            F.col("FOLDER_NAME").alias("FIELD_NAME"), \
                                                                            F.col("AREA_NAME").alias("BUSINESS_UNIT_NAME"))
    
    
    j2 = j1.join(tdw_fv_daily_well_prod, on="SITE_ID", how="inner")
    
    j3 = j2.join(tdw_fv_daily_lost_prod, on=["SITE_ID", "PRODUCTION_DATE"], how="left", rsuffix="_lp")
    
    j3.create_or_replace_view('HARMONIZED.AVOCET_PROD_DETAIL_V')

    


# def create_pos_view_stream(session):
#     session.use_schema('HARMONIZED')
#     _ = session.sql('CREATE OR REPLACE STREAM AVOCET_PROD_DETAIL_V_STREAM \
#                         ON VIEW AVOCET_PROD_DETAIL_V \
#                         SHOW_INITIAL_ROWS = TRUE').collect()

def test_pos_view(session):
    session.use_schema('HARMONIZED')
    tv = session.table('AVOCET_PROD_DETAIL_V')
    tv.limit(5).show()


# For local debugging
if __name__ == "__main__":
    # Add the utils package to our path and import the snowpark_utils function
    import os, sys
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()
    
    create_pos_view(session)
    # create_pos_view_stream(session)
    # test_pos_view(session)

    session.close()
