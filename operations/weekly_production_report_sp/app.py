
from datetime import datetime, timedelta
import pandas as pd
from snowflake.snowpark import Session, Window
#import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F


def table_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    return exists

def process(session):
    
    session.use_schema('HARMONIZED')
    use_wh_scaling = False
    
    if use_wh_scaling:
        _ = session.sql('ALTER WAREHOUSE CDE_WH SET WAREHOUSE_SIZE = MEDIUM WAIT_FOR_COMPLETION = TRUE').collect()
    
    
    ### AVOCET DATA ####
    
    # select data where date > 365 days ago
    avocet = session.table("HARMONIZED.AVOCET_PROD_DETAIL") \
        .select(
            F.col("UWI").collate('en_us-ci').alias("UWI"),
            F.col("PRODUCTION_DATE"),
            F.col("TEAM"),
            F.col("MAJOR_AREA"),
            F.col("AREA"),
            F.col("ACCT_BOE").alias("SALES_BOE"),
            F.col("ACCT_M3E").alias("SALES_M3E"),
            F.col("ACCT_GAS_E3M3").alias("GAS_SALES_E3M3"),
            F.col("ACCT_GAS_MCF").alias("GAS_SALES_MCF"),
            F.col("ACCT_GAS_BOE").alias("GAS_SALES_BOE"),
            F.col("ACCT_OIL_M3").alias("OIL_SALES_M3"),
            F.col("ACCT_COND_M3").alias("COND_SALES_M3"),
            F.col("ACCT_OIL_BBL").alias("OIL_SALES_BBL"),
            F.col("ACCT_COND_BBL").alias("COND_SALES_BBL"),
            F.col("ACCT_NGL_M3").alias("NGL_SALES_M3"),
            F.col("ACCT_NGL_BBL").alias("NGL_SALES_BBL"),
            # F.col("SALES_BOE"),
            # F.col("SALES_M3E"),
            # F.col("GAS_SALES_E3M3"),
            # F.col("GAS_SALES_MCF"),
            # F.col("GAS_SALES_BOE"),
            # F.col("OIL_SALES_M3"),
            # F.col("COND_SALES_M3"),
            # F.col("OIL_SALES_BBL"),
            # F.col("COND_SALES_BBL"),
            # F.col("NGL_SALES_M3"),
            # F.col("NGL_SALES_BBL"),
            F.col("BOE_WELL").alias("QBYTE_BOE"),
            F.col("HOURS_DOWN").alias("DOWNTIME_HOURS"),
            F.col("DOWNTIME_NAME")
        ) \
        .filter((F.col("PRODUCTION_DATE") >= F.lit(datetime.now() - timedelta(days=365))) \
            # & (F.col('TEAM') != F.lit("EAST")) # we will be getting rid of this filter eventually, will want to scale up to a larger warehouse to process all 7000 wells (939 excludes EAST)
                ) \
        .sort(F.col("UWI"), F.col("PRODUCTION_DATE")) \
        .with_column('WEEK_START', F.date_trunc('week', F.col('PRODUCTION_DATE'))) \
        .with_column('MONTH_START', F.date_trunc('month', F.col('PRODUCTION_DATE'))) \
        .with_column('YEAR_START', F.date_trunc('year', F.col('PRODUCTION_DATE'))) \
        .with_column('PRIOR_WEEK_START', F.date_trunc('week', F.date_sub(F.date_trunc('week', F.col('PRODUCTION_DATE')), 2))) \
        .with_column('PRIOR_MONTH_START', F.date_trunc('month', F.date_sub(F.date_trunc('month', F.col('PRODUCTION_DATE')), 2))) \
        .with_column('PRIOR_YEAR_START', F.date_trunc('year', F.date_sub(F.date_trunc('year', F.col('PRODUCTION_DATE')), 2))) \
        .with_column("QBYTE_M3E", F.call_udf("HARMONIZED.BBL_TO_M3_UDF", F.col("QBYTE_BOE")))
        
        
    
    well_rpc = session.table("HARMONIZED.UWI_RPC") \
         .select(
            F.col("UWI").collate('en_us-ci').alias("UWI"),
            F.col("PRODUCTION_DATE"),
            F.col("GROSS_BOE_270D_RPC"),
            F.col("GROSS_M3E_270D_RPC"),
            F.col("NGL_GROSS_M3_270D_RPC").alias("GROSS_NGL_M3_270D_RPC"),
            F.col("NGL_GROSS_BBL_270D_RPC").alias("GROSS_NGL_BBL_270D_RPC"),
            F.col("OIL_GROSS_M3_270D_RPC").alias("GROSS_OIL_M3_270D_RPC"),
            F.col("OIL_GROSS_BBL_270D_RPC").alias("GROSS_OIL_BBL_270D_RPC"),
            F.col("COND_GROSS_M3_270D_RPC").alias("GROSS_COND_M3_270D_RPC"),
            F.col("COND_GROSS_BBL_270D_RPC").alias("GROSS_COND_BBL_270D_RPC"),
            F.col("GAS_GROSS_E3M3_270D_RPC").alias("GROSS_GAS_E3M3_270D_RPC"),
            F.col("GAS_GROSS_MCF_270D_RPC").alias("GROSS_GAS_MCF_270D_RPC"),
            F.col("GAS_GROSS_BOE_270D_RPC").alias("GROSS_GAS_BOE_270D_RPC"),
            F.col("SALES_BOE_270D_RPC"),
            F.col("SALES_M3E_270D_RPC"),
            F.col("NGL_SALES_M3_270D_RPC").alias("SALES_NGL_M3_270D_RPC"),
            F.col("NGL_SALES_BBL_270D_RPC").alias("SALES_NGL_BBL_270D_RPC"),
            F.col("OIL_SALES_M3_270D_RPC").alias("SALES_OIL_M3_270D_RPC"),
            F.col("OIL_SALES_BBL_270D_RPC").alias("SALES_OIL_BBL_270D_RPC"),
            F.col("COND_SALES_M3_270D_RPC").alias("SALES_COND_M3_270D_RPC"),
            F.col("COND_SALES_BBL_270D_RPC").alias("SALES_COND_BBL_270D_RPC"),
            F.col("GAS_SALES_E3M3_270D_RPC").alias("SALES_GAS_E3M3_270D_RPC"),
            F.col("GAS_SALES_MCF_270D_RPC").alias("SALES_GAS_MCF_270D_RPC"),
            F.col("GAS_SALES_BOE_270D_RPC").alias("SALES_GAS_BOE_270D_RPC"),
            F.col("SALES_BOE_90D_50P")
             )
        
    # Get RPC values for each UWI
    avocet_uwi_daily = avocet.join(well_rpc, (avocet.UWI == well_rpc.UWI) & (avocet.PRODUCTION_DATE == well_rpc.PRODUCTION_DATE), how='left', rsuffix='_well_stats') \
        .with_column("SALES_BOE_VS_RPC", F.col("SALES_BOE") - F.col("SALES_BOE_270D_RPC")) \
        .with_column("SALES_M3E_VS_RPC", F.col("SALES_M3E") - F.col("SALES_M3E_270D_RPC")) \
        .with_column("SALES_NGL_M3_VS_RPC", F.col("NGL_SALES_M3") - F.col("SALES_NGL_M3_270D_RPC")) \
        .with_column("SALES_NGL_BBL_VS_RPC", F.col("NGL_SALES_BBL") - F.col("SALES_NGL_BBL_270D_RPC")) \
        .with_column("SALES_OIL_M3_VS_RPC", F.col("OIL_SALES_M3") - F.col("SALES_OIL_M3_270D_RPC")) \
        .with_column("SALES_OIL_BBL_VS_RPC", F.col("OIL_SALES_BBL") - F.col("SALES_OIL_BBL_270D_RPC")) \
        .with_column("SALES_COND_M3_VS_RPC", F.col("COND_SALES_M3") - F.col("SALES_COND_M3_270D_RPC")) \
        .with_column("SALES_COND_BBL_VS_RPC", F.col("COND_SALES_BBL") - F.col("SALES_COND_BBL_270D_RPC")) \
        .with_column("SALES_GAS_E3M3_VS_RPC", F.col("GAS_SALES_E3M3") - F.col("SALES_GAS_E3M3_270D_RPC")) \
        .with_column("SALES_GAS_MCF_VS_RPC", F.col("GAS_SALES_MCF") - F.col("SALES_GAS_MCF_270D_RPC")) \
        .with_column("SALES_GAS_BOE_VS_RPC", F.col("GAS_SALES_BOE") - F.col("SALES_GAS_BOE_270D_RPC")) \
        .with_column("DOWNTIME_BOE", F.col("SALES_BOE_90D_50P") * F.col("DOWNTIME_HOURS") / 24)
    
    
    ### BUDGET ###
    budget = session.table("RAW_VALNAV.BUDGET_2023").select(
        F.col("UNIQUE_ID").collate('en_us-ci').alias("UWI"),
        F.col("Net BOE(BOE/d)").alias("BUDGET_BOE"),
        F.col("STEP_DATE"),
        F.col("H Team (MGMT)").alias("TEAM"),
        F.col("H Major Area").alias("MAJOR_AREA"),
        F.col("H Area").alias("AREA"),
    )  \
    .filter(F.col("STEP_DATE") <= F.lit(datetime.now())) \
    .with_column('MONTH_START', F.date_trunc('month', F.col('STEP_DATE'))) \
    .with_column('YEAR_START', F.date_trunc('year', F.col('STEP_DATE'))) \
    .with_column("BUDGET_M3E", F.call_udf("HARMONIZED.BBL_TO_M3_UDF", F.col("BUDGET_BOE"))) \
    .drop(F.col("STEP_DATE"))
    
    
    ## MAPPINGS - mostly used to fix hierarchy errors in the Budget database ##
    # generate a mapping from the avocet AREA to MAJOR_AREA. Drop this into the MAP_AREA_TO_MAJOR_AREA_UDF function and redeploy if changes are made to the hiererchy
    area_major_area_map = avocet_uwi_daily.select(F.col("AREA"), F.col("MAJOR_AREA")).distinct().toPandas().set_index("AREA").to_dict()["MAJOR_AREA"]
    print(area_major_area_map)
    
    # generate a mapping from the avocet AREA to TEAM. Drop this into the MAP_AREA_TO_TEAM_UDF function and redeploy if changes are made to the hiererchy
    area_team_map = avocet_uwi_daily.select(F.col("AREA"), F.col("TEAM")).distinct().toPandas().set_index("AREA").to_dict()["TEAM"]
    print(area_team_map)
    
    # clean up budget hierarchies which do not sync with Avocet/Qbyte
    budget = budget \
        .with_column("AREA", F.when(F.col("AREA") == "PRA", "GLACIER").otherwise(F.col("AREA"))) \
        .with_column("MAJOR_AREA", F.when((F.col("MAJOR_AREA") == "TBD") | (F.col("MAJOR_AREA") == "BASHAW") | (F.col("MAJOR_AREA") == "PRA"), F.call_udf("HARMONIZED.MAP_AREA_TO_MAJAREA_UDF", F.col("AREA"))).otherwise(F.col("MAJOR_AREA"))) \
        .with_column("TEAM", F.when(F.col("TEAM") == "TBD", F.call_udf("HARMONIZED.MAP_AREA_TO_TEAM_UDF", F.col("AREA"))).otherwise(F.col("TEAM")))
        
    # team comments
    team_comments = session.table("MANUAL_INPUTS.WPR_TEAM_COMMENTS_WOW").select(
        F.col("TEAM"),
        F.col("WEEK_START"),
        F.col("COMMENT").alias("TEAM_COMMENT")
    )
    
    # major area comments
    major_area_comments = session.table("MANUAL_INPUTS.WPR_MAJORAREA_COMMENTS_WOW").select(
        F.col("TEAM"),
        F.col("MAJOR_AREA"),
        F.col("WEEK_START"),
        F.col("COMMENT").alias("MAJOR_AREA_COMMENT")
    )
    
    
    ## CONVERSION TO WEEKLY ##
    
    # group by UWI and WEEK_START, generate averages for each column
    avocet_uwi_weekly = avocet_uwi_daily.groupBy(F.col("UWI"), F.col("WEEK_START")) \
        .agg(
            F.avg(F.col("SALES_BOE")).alias("SALES_BOE_WEEKLY_AVG"),
            F.avg(F.col("SALES_M3E")).alias("SALES_M3E_WEEKLY_AVG"),
            F.avg(F.col("QBYTE_BOE")).alias("QBYTE_BOE_WEEKLY_AVG"),
            F.avg(F.col("QBYTE_M3E")).alias("QBYTE_M3E_WEEKLY_AVG"),
            F.avg(F.col("GAS_SALES_E3M3")).alias("GAS_SALES_E3M3_WEEKLY_AVG"),
            F.avg(F.col("GAS_SALES_MCF")).alias("GAS_SALES_MCF_WEEKLY_AVG"),
            F.avg(F.col("GAS_SALES_BOE")).alias("GAS_SALES_BOE_WEEKLY_AVG"),
            F.avg(F.col("OIL_SALES_M3")).alias("OIL_SALES_M3_WEEKLY_AVG"),
            F.avg(F.col("COND_SALES_M3")).alias("COND_SALES_M3_WEEKLY_AVG"),
            F.avg(F.col("OIL_SALES_BBL")).alias("OIL_SALES_BBL_WEEKLY_AVG"),
            F.avg(F.col("COND_SALES_BBL")).alias("COND_SALES_BBL_WEEKLY_AVG"),
            F.avg(F.col("NGL_SALES_M3")).alias("NGL_SALES_M3_WEEKLY_AVG"),
            F.avg(F.col("NGL_SALES_BBL")).alias("NGL_SALES_BBL_WEEKLY_AVG"),
            F.avg(F.col("DOWNTIME_BOE")).alias("DOWNTIME_BOE_WEEKLY_AVG"),
            F.avg(F.col("SALES_BOE_270D_RPC")).alias("SALES_BOE_270D_RPC_WEEKLY_AVG"),
            F.avg(F.col("SALES_M3E_270D_RPC")).alias("SALES_M3E_270D_RPC_WEEKLY_AVG"),
            F.avg(F.col("SALES_NGL_M3_270D_RPC")).alias("SALES_NGL_M3_270D_RPC_WEEKLY_AVG"),
            F.avg(F.col("SALES_NGL_BBL_270D_RPC")).alias("SALES_NGL_BBL_270D_RPC_WEEKLY_AVG"),
            F.avg(F.col("SALES_OIL_M3_270D_RPC")).alias("SALES_OIL_M3_270D_RPC_WEEKLY_AVG"),
            F.avg(F.col("SALES_OIL_BBL_270D_RPC")).alias("SALES_OIL_BBL_270D_RPC_WEEKLY_AVG"),
            F.avg(F.col("SALES_COND_M3_270D_RPC")).alias("SALES_COND_M3_270D_RPC_WEEKLY_AVG"),
            F.avg(F.col("SALES_COND_BBL_270D_RPC")).alias("SALES_COND_BBL_270D_RPC_WEEKLY_AVG"),
            F.avg(F.col("SALES_GAS_E3M3_270D_RPC")).alias("SALES_GAS_E3M3_270D_RPC_WEEKLY_AVG"),
            F.avg(F.col("SALES_GAS_MCF_270D_RPC")).alias("SALES_GAS_MCF_270D_RPC_WEEKLY_AVG"),
            F.avg(F.col("SALES_GAS_BOE_270D_RPC")).alias("SALES_GAS_BOE_270D_RPC_WEEKLY_AVG"),
            F.max(F.col("PRIOR_WEEK_START")).alias("PRIOR_WEEK_START"),
            F.max(F.col("MONTH_START")).alias("MONTH_START"),
            F.max(F.col("AREA")).alias("AREA"),
            F.max(F.col("MAJOR_AREA")).alias("MAJOR_AREA"),
            F.max(F.col("TEAM")).alias("TEAM"),
        ) \
        .with_column("SALES_BOE_VS_RPC_WEEKLY_AVG", F.col("SALES_BOE_WEEKLY_AVG") - F.col("SALES_BOE_270D_RPC_WEEKLY_AVG")) \
        .with_column("SALES_M3E_VS_RPC_WEEKLY_AVG", F.col("SALES_M3E_WEEKLY_AVG") - F.col("SALES_M3E_270D_RPC_WEEKLY_AVG")) \
        .with_column("SALES_NGL_M3_VS_RPC_WEEKLY_AVG", F.col("NGL_SALES_M3_WEEKLY_AVG") - F.col("SALES_NGL_M3_270D_RPC_WEEKLY_AVG")) \
        .with_column("SALES_NGL_BBL_VS_RPC_WEEKLY_AVG", F.col("NGL_SALES_BBL_WEEKLY_AVG") - F.col("SALES_NGL_BBL_270D_RPC_WEEKLY_AVG")) \
        .with_column("SALES_OIL_M3_VS_RPC_WEEKLY_AVG", F.col("OIL_SALES_M3_WEEKLY_AVG") -  F.col("SALES_OIL_M3_270D_RPC_WEEKLY_AVG")) \
        .with_column("SALES_OIL_BBL_VS_RPC_WEEKLY_AVG", F.col("OIL_SALES_BBL_WEEKLY_AVG") - F.col("SALES_OIL_BBL_270D_RPC_WEEKLY_AVG")) \
        .with_column("SALES_COND_M3_VS_RPC_WEEKLY_AVG", F.col("COND_SALES_M3_WEEKLY_AVG") -  F.col("SALES_COND_M3_270D_RPC_WEEKLY_AVG")) \
        .with_column("SALES_COND_BBL_VS_RPC_WEEKLY_AVG", F.col("COND_SALES_BBL_WEEKLY_AVG") - F.col("SALES_COND_BBL_270D_RPC_WEEKLY_AVG")) \
        .with_column("SALES_GAS_E3M3_VS_RPC_WEEKLY_AVG", F.col("GAS_SALES_E3M3_WEEKLY_AVG") -  F.col("SALES_GAS_E3M3_270D_RPC_WEEKLY_AVG")) \
        .with_column("SALES_GAS_MCF_VS_RPC_WEEKLY_AVG", F.col("GAS_SALES_MCF_WEEKLY_AVG") -  F.col("SALES_GAS_MCF_270D_RPC_WEEKLY_AVG")) \
        .with_column("SALES_GAS_BOE_VS_RPC_WEEKLY_AVG", F.col("GAS_SALES_BOE_WEEKLY_AVG") -  F.col("SALES_GAS_BOE_270D_RPC_WEEKLY_AVG"))
        # .with_column("SALES_BOE_RPC_PRODUCTION_EFFICIENCY", F.call_builtin("IFF", F.col("SALES_BOE_270D_RPC_WEEKLY_AVG") == 0, 1, F.col("SALES_BOE_WEEKLY_AVG") / F.col("SALES_BOE_270D_RPC_WEEKLY_AVG")))
            
    avocet_uwi_weekly = avocet_uwi_weekly.join(
        team_comments,
        (avocet_uwi_weekly.TEAM == team_comments.TEAM) & (avocet_uwi_weekly.WEEK_START == team_comments.WEEK_START),
        "left",
         rsuffix="_TEAM_COMMENTS"
    ).join(
        major_area_comments,
        (avocet_uwi_weekly.MAJOR_AREA == major_area_comments.MAJOR_AREA) & (avocet_uwi_weekly.WEEK_START == major_area_comments.WEEK_START),
        "left",
            rsuffix="_MAJOR_AREA_COMMENTS"
    )
        
    avocet_uwi_weekly_prior = avocet_uwi_daily.groupBy(F.col("UWI"), F.col("WEEK_START")) \
        .agg(
            F.avg(F.col("SALES_BOE")).alias("SALES_BOE_WEEKLY_AVG"),
            F.avg(F.col("SALES_M3E")).alias("SALES_M3E_WEEKLY_AVG"),
        ).select(
            F.col("UWI"),
            F.col("WEEK_START"),
            F.col("SALES_BOE_WEEKLY_AVG"),
            F.col("SALES_M3E_WEEKLY_AVG")
            )
        
        # join uwi_weekly_avgs with uwi_weekly_avgs_prior on UWI=UWI and WEEK_START=PRIOR_WEEK_START to get the previous week's averages
    avocet_uwi_weekly_with_priors = avocet_uwi_weekly.join(avocet_uwi_weekly_prior, 
                                          (avocet_uwi_weekly.UWI == avocet_uwi_weekly_prior.UWI) & (avocet_uwi_weekly.PRIOR_WEEK_START == avocet_uwi_weekly_prior.WEEK_START), 
                                            how='left', 
                                            rsuffix='_PRIOR_WEEK'
                                          ).drop(F.col("UWI_PRIOR_WEEK"),F.col("WEEK_START_PRIOR_WEEK")) \
                                        .with_column("SALES_BOE_VS_PRIOR_WEEK_AVG",  F.col("SALES_BOE_WEEKLY_AVG") - F.col("SALES_BOE_WEEKLY_AVG_PRIOR_WEEK")) \
                                        .with_column("SALES_M3E_VS_PRIOR_WEEK_AVG",  F.col("SALES_M3E_WEEKLY_AVG") - F.col("SALES_M3E_WEEKLY_AVG_PRIOR_WEEK"))

    budget_weekly = budget.groupBy(F.col("UWI"), F.col("MONTH_START")) \
        .agg(
            F.avg(F.col("BUDGET_BOE")).alias("BUDGET_BOE_MONTHLY_AVG"),
            F.avg(F.col("BUDGET_M3E")).alias("BUDGET_M3E_MONTHLY_AVG"),
            F.max(F.col("AREA")).alias("AREA"),
            F.max(F.col("MAJOR_AREA")).alias("MAJOR_AREA"),
            F.max(F.col("TEAM")).alias("TEAM"),
        )
        
    print(budget_weekly.limit(5).show())

    avocet_uwi_weekly_with_priors = avocet_uwi_weekly_with_priors.join(budget_weekly,
                                                                   ['UWI', 'MONTH_START'],
                                                how='full', 
                                                rsuffix='_BUDGET'
                                            ) \
                                                .with_column("AREA", F.call_builtin("IFF", F.col("AREA").isNull(), F.col("AREA_BUDGET"), F.col("AREA"))) \
                                                .with_column("MAJOR_AREA", F.call_builtin("IFF", F.col("MAJOR_AREA").isNull(), F.col("MAJOR_AREA_BUDGET"), F.col("MAJOR_AREA"))) \
                                                .with_column("TEAM", F.call_builtin("IFF", F.col("TEAM").isNull(), F.col("TEAM_BUDGET"), F.col("TEAM"))) \
                                                .filter(~F.col("AREA").isin(['SAVANNA', 'BASHAW', 'WOOD RIVER']))

                                                        
                                                 
                                                
    
    print(avocet_uwi_weekly_with_priors.limit(5).show())
    
         
    ## CONVERSION TO MONTHLY ##
    
    # group by UWI and MONTH_START, generate averages for each column
    avocet_uwi_monthly = avocet_uwi_daily.groupBy(F.col("UWI"), F.col("MONTH_START")) \
        .agg(
            F.avg(F.col("SALES_BOE")).alias("SALES_BOE_MONTHLY_AVG"),
            F.avg(F.col("SALES_M3E")).alias("SALES_M3E_MONTHLY_AVG"),
            F.avg(F.col("QBYTE_BOE")).alias("QBYTE_BOE_MONTHLY_AVG"),
            F.avg(F.col("QBYTE_M3E")).alias("QBYTE_M3E_MONTHLY_AVG"),
            F.avg(F.col("GAS_SALES_E3M3")).alias("GAS_SALES_E3M3_MONTHLY_AVG"),
            F.avg(F.col("GAS_SALES_MCF")).alias("GAS_SALES_MCF_MONTHLY_AVG"),
            F.avg(F.col("GAS_SALES_BOE")).alias("GAS_SALES_BOE_MONTHLY_AVG"),
            F.avg(F.col("OIL_SALES_M3")).alias("OIL_SALES_M3_MONTHLY_AVG"),
            F.avg(F.col("COND_SALES_M3")).alias("COND_SALES_M3_MONTHLY_AVG"),
            F.avg(F.col("OIL_SALES_BBL")).alias("OIL_SALES_BBL_MONTHLY_AVG"),
            F.avg(F.col("COND_SALES_BBL")).alias("COND_SALES_BBL_MONTHLY_AVG"),
            F.avg(F.col("NGL_SALES_M3")).alias("NGL_SALES_M3_MONTHLY_AVG"),
            F.avg(F.col("NGL_SALES_BBL")).alias("NGL_SALES_BBL_MONTHLY_AVG"),
            F.avg(F.col("DOWNTIME_BOE")).alias("DOWNTIME_BOE_MONTHLY_AVG"),
            F.avg(F.col("SALES_BOE_270D_RPC")).alias("SALES_BOE_270D_RPC_MONTHLY_AVG"),
            F.avg(F.col("SALES_M3E_270D_RPC")).alias("SALES_M3E_270D_RPC_MONTHLY_AVG"),
            F.avg(F.col("SALES_NGL_M3_270D_RPC")).alias("SALES_NGL_M3_270D_RPC_MONTHLY_AVG"),
            F.avg(F.col("SALES_NGL_BBL_270D_RPC")).alias("SALES_NGL_BBL_270D_RPC_MONTHLY_AVG"),
            F.avg(F.col("SALES_OIL_M3_270D_RPC")).alias("SALES_OIL_M3_270D_RPC_MONTHLY_AVG"),
            F.avg(F.col("SALES_OIL_BBL_270D_RPC")).alias("SALES_OIL_BBL_270D_RPC_MONTHLY_AVG"),
            F.avg(F.col("SALES_COND_M3_270D_RPC")).alias("SALES_COND_M3_270D_RPC_MONTHLY_AVG"),
            F.avg(F.col("SALES_COND_BBL_270D_RPC")).alias("SALES_COND_BBL_270D_RPC_MONTHLY_AVG"),
            F.avg(F.col("SALES_GAS_E3M3_270D_RPC")).alias("SALES_GAS_E3M3_270D_RPC_MONTHLY_AVG"),
            F.avg(F.col("SALES_GAS_MCF_270D_RPC")).alias("SALES_GAS_MCF_270D_RPC_MONTHLY_AVG"),
            F.avg(F.col("SALES_GAS_BOE_270D_RPC")).alias("SALES_GAS_BOE_270D_RPC_MONTHLY_AVG"),
            F.max(F.col("PRIOR_MONTH_START")).alias("PRIOR_MONTH_START"),
            F.max(F.col("AREA")).alias("AREA"),
            F.max(F.col("MAJOR_AREA")).alias("MAJOR_AREA"),
            F.max(F.col("TEAM")).alias("TEAM"),
        ) \
        .with_column("SALES_BOE_VS_RPC_MONTHLY_AVG", F.col("SALES_BOE_MONTHLY_AVG") - F.col("SALES_BOE_270D_RPC_MONTHLY_AVG")) \
        .with_column("SALES_M3E_VS_RPC_MONTHLY_AVG", F.col("SALES_M3E_MONTHLY_AVG") - F.col("SALES_M3E_270D_RPC_MONTHLY_AVG")) \
        .with_column("SALES_NGL_M3_VS_RPC_MONTHLY_AVG", F.col("NGL_SALES_M3_MONTHLY_AVG") - F.col("SALES_NGL_M3_270D_RPC_MONTHLY_AVG")) \
        .with_column("SALES_NGL_BBL_VS_RPC_MONTHLY_AVG", F.col("NGL_SALES_BBL_MONTHLY_AVG") - F.col("SALES_NGL_BBL_270D_RPC_MONTHLY_AVG")) \
        .with_column("SALES_OIL_M3_VS_RPC_MONTHLY_AVG", F.col("OIL_SALES_M3_MONTHLY_AVG") -  F.col("SALES_OIL_M3_270D_RPC_MONTHLY_AVG")) \
        .with_column("SALES_OIL_BBL_VS_RPC_MONTHLY_AVG", F.col("OIL_SALES_BBL_MONTHLY_AVG") - F.col("SALES_OIL_BBL_270D_RPC_MONTHLY_AVG")) \
        .with_column("SALES_COND_M3_VS_RPC_MONTHLY_AVG", F.col("COND_SALES_M3_MONTHLY_AVG") -  F.col("SALES_COND_M3_270D_RPC_MONTHLY_AVG")) \
        .with_column("SALES_COND_BBL_VS_RPC_MONTHLY_AVG", F.col("COND_SALES_BBL_MONTHLY_AVG") - F.col("SALES_COND_BBL_270D_RPC_MONTHLY_AVG")) \
        .with_column("SALES_GAS_E3M3_VS_RPC_MONTHLY_AVG", F.col("GAS_SALES_E3M3_MONTHLY_AVG") -  F.col("SALES_GAS_E3M3_270D_RPC_MONTHLY_AVG")) \
        .with_column("SALES_GAS_MCF_VS_RPC_MONTHLY_AVG", F.col("GAS_SALES_MCF_MONTHLY_AVG") -  F.col("SALES_GAS_MCF_270D_RPC_MONTHLY_AVG")) \
        .with_column("SALES_GAS_BOE_VS_RPC_MONTHLY_AVG", F.col("GAS_SALES_BOE_MONTHLY_AVG") -  F.col("SALES_GAS_BOE_270D_RPC_MONTHLY_AVG"))
        # .with_column("SALES_BOE_RPC_PRODUCTION_EFFICIENCY", F.call_builtin("IFF", F.col("SALES_BOE_270D_RPC_MONTHLY_AVG") == 0, 1, F.col("SALES_BOE_MONTHLY_AVG") / F.col("SALES_BOE_270D_RPC_MONTHLY_AVG")))
            
    
        
    avocet_uwi_monthly_prior = avocet_uwi_daily.groupBy(F.col("UWI"), F.col("MONTH_START")) \
        .agg(
            F.avg(F.col("SALES_BOE")).alias("SALES_BOE_MONTHLY_AVG"),
            F.avg(F.col("SALES_M3E")).alias("SALES_M3E_MONTHLY_AVG"),
        ).select(
            F.col("UWI"),
            F.col("MONTH_START"),
            F.col("SALES_BOE_MONTHLY_AVG"),
            F.col("SALES_M3E_MONTHLY_AVG")
            )
        
        # join uwi_monthly_avgs with uwi_monthly_avgs_prior on UWI=UWI and MONTH_START=PRIOR_MONTH_START to get the previous week's averages
    avocet_uwi_monthly_with_priors = avocet_uwi_monthly.join(avocet_uwi_monthly_prior, 
                                          (avocet_uwi_monthly.UWI == avocet_uwi_monthly_prior.UWI) & (avocet_uwi_monthly.PRIOR_MONTH_START == avocet_uwi_monthly_prior.MONTH_START), 
                                            how='left', 
                                            rsuffix='_PRIOR_MONTH'
                                          ).drop(F.col("UWI_PRIOR_MONTH"),F.col("MONTH_START_PRIOR_MONTH")) \
                                        .with_column("SALES_BOE_VS_PRIOR_MONTH_AVG",  F.col("SALES_BOE_MONTHLY_AVG") - F.col("SALES_BOE_MONTHLY_AVG_PRIOR_MONTH")) \
                                        .with_column("SALES_M3E_VS_PRIOR_MONTH_AVG",  F.col("SALES_M3E_MONTHLY_AVG") - F.col("SALES_M3E_MONTHLY_AVG_PRIOR_MONTH"))
    
    budget_month = budget.groupBy(F.col("UWI"), F.col("MONTH_START")) \
        .agg(
            F.avg(F.col("BUDGET_BOE")).alias("BUDGET_BOE_MONTHLY_AVG"),
            F.avg(F.col("BUDGET_M3E")).alias("BUDGET_M3E_MONTHLY_AVG"),
            F.max(F.col("AREA")).alias("AREA"),
            F.max(F.col("MAJOR_AREA")).alias("MAJOR_AREA"),
            F.max(F.col("TEAM")).alias("TEAM"),
        )
        
    print(budget_month.limit(5).show())

    avocet_uwi_monthly_with_priors = avocet_uwi_monthly_with_priors.join(budget_month,
                                                                   ['UWI', 'MONTH_START'],
                                                how='full', 
                                                rsuffix='_BUDGET'
                                            ) \
                                                .with_column("SALES_BOE_VS_BUDGET_MONTHLY_AVG",  F.call_builtin("ZEROIFNULL", F.col("SALES_BOE_MONTHLY_AVG")) - F.call_builtin("ZEROIFNULL", F.col("BUDGET_BOE_MONTHLY_AVG"))) \
                                                .with_column("SALES_M3E_VS_BUDGET_MONTHLY_AVG",  F.call_builtin("ZEROIFNULL", F.col("SALES_M3E_MONTHLY_AVG")) - F.call_builtin("ZEROIFNULL", F.col("BUDGET_M3E_MONTHLY_AVG"))) \
                                                .with_column("AREA", F.call_builtin("IFF", F.col("AREA").isNull(), F.col("AREA_BUDGET"), F.col("AREA"))) \
                                                .with_column("MAJOR_AREA", F.call_builtin("IFF", F.col("MAJOR_AREA").isNull(), F.col("MAJOR_AREA_BUDGET"), F.col("MAJOR_AREA"))) \
                                                .with_column("TEAM", F.call_builtin("IFF", F.col("TEAM").isNull(), F.col("TEAM_BUDGET"), F.col("TEAM"))) \
                                                    .dropna(subset=["SALES_BOE_MONTHLY_AVG", "BUDGET_BOE_MONTHLY_AVG"], how='all') \
                                                    .filter(~F.col("AREA").isin(['SAVANNA', 'BASHAW', 'WOOD RIVER']))
                                                
    
    print(avocet_uwi_monthly_with_priors.limit(5).show())
    
    
    
    ## CONVERSION TO ANNUAL ##
    
    # group by UWI and MONTH_START, generate averages for each column
    avocet_uwi_year = avocet_uwi_daily.groupBy(F.col("UWI"), F.col("YEAR_START")) \
        .agg(
            F.avg(F.col("SALES_BOE")).alias("SALES_BOE_YEAR_AVG"),
            F.avg(F.col("SALES_M3E")).alias("SALES_M3E_YEAR_AVG"),
            F.avg(F.col("QBYTE_BOE")).alias("QBYTE_BOE_YEAR_AVG"),
            F.avg(F.col("QBYTE_M3E")).alias("QBYTE_M3E_YEAR_AVG"),
            F.avg(F.col("GAS_SALES_E3M3")).alias("GAS_SALES_E3M3_YEAR_AVG"),
            F.avg(F.col("GAS_SALES_MCF")).alias("GAS_SALES_MCF_YEAR_AVG"),
            F.avg(F.col("GAS_SALES_BOE")).alias("GAS_SALES_BOE_YEAR_AVG"),
            F.avg(F.col("OIL_SALES_M3")).alias("OIL_SALES_M3_YEAR_AVG"),
            F.avg(F.col("COND_SALES_M3")).alias("COND_SALES_M3_YEAR_AVG"),
            F.avg(F.col("OIL_SALES_BBL")).alias("OIL_SALES_BBL_YEAR_AVG"),
            F.avg(F.col("COND_SALES_BBL")).alias("COND_SALES_BBL_YEAR_AVG"),
            F.avg(F.col("NGL_SALES_M3")).alias("NGL_SALES_M3_YEAR_AVG"),
            F.avg(F.col("NGL_SALES_BBL")).alias("NGL_SALES_BBL_YEAR_AVG"),
            F.avg(F.col("DOWNTIME_BOE")).alias("DOWNTIME_BOE_YEAR_AVG"),
            F.avg(F.col("SALES_BOE_270D_RPC")).alias("SALES_BOE_270D_RPC_YEAR_AVG"),
            F.avg(F.col("SALES_M3E_270D_RPC")).alias("SALES_M3E_270D_RPC_YEAR_AVG"),
            F.avg(F.col("SALES_NGL_M3_270D_RPC")).alias("SALES_NGL_M3_270D_RPC_YEAR_AVG"),
            F.avg(F.col("SALES_NGL_BBL_270D_RPC")).alias("SALES_NGL_BBL_270D_RPC_YEAR_AVG"),
            F.avg(F.col("SALES_OIL_M3_270D_RPC")).alias("SALES_OIL_M3_270D_RPC_YEAR_AVG"),
            F.avg(F.col("SALES_OIL_BBL_270D_RPC")).alias("SALES_OIL_BBL_270D_RPC_YEAR_AVG"),
            F.avg(F.col("SALES_COND_M3_270D_RPC")).alias("SALES_COND_M3_270D_RPC_YEAR_AVG"),
            F.avg(F.col("SALES_COND_BBL_270D_RPC")).alias("SALES_COND_BBL_270D_RPC_YEAR_AVG"),
            F.avg(F.col("SALES_GAS_E3M3_270D_RPC")).alias("SALES_GAS_E3M3_270D_RPC_YEAR_AVG"),
            F.avg(F.col("SALES_GAS_MCF_270D_RPC")).alias("SALES_GAS_MCF_270D_RPC_YEAR_AVG"),
            F.avg(F.col("SALES_GAS_BOE_270D_RPC")).alias("SALES_GAS_BOE_270D_RPC_YEAR_AVG"),
            F.max(F.col("PRIOR_YEAR_START")).alias("PRIOR_YEAR_START"),
            F.max(F.col("AREA")).alias("AREA"),
            F.max(F.col("MAJOR_AREA")).alias("MAJOR_AREA"),
            F.max(F.col("TEAM")).alias("TEAM"),
        ) \
        .with_column("SALES_BOE_VS_RPC_YEAR_AVG", F.col("SALES_BOE_YEAR_AVG") - F.col("SALES_BOE_270D_RPC_YEAR_AVG")) \
        .with_column("SALES_M3E_VS_RPC_YEAR_AVG", F.col("SALES_M3E_YEAR_AVG") - F.col("SALES_M3E_270D_RPC_YEAR_AVG")) \
        .with_column("SALES_NGL_M3_VS_RPC_YEAR_AVG", F.col("NGL_SALES_M3_YEAR_AVG") - F.col("SALES_NGL_M3_270D_RPC_YEAR_AVG")) \
        .with_column("SALES_NGL_BBL_VS_RPC_YEAR_AVG", F.col("NGL_SALES_BBL_YEAR_AVG") - F.col("SALES_NGL_BBL_270D_RPC_YEAR_AVG")) \
        .with_column("SALES_OIL_M3_VS_RPC_YEAR_AVG", F.col("OIL_SALES_M3_YEAR_AVG") -  F.col("SALES_OIL_M3_270D_RPC_YEAR_AVG")) \
        .with_column("SALES_OIL_BBL_VS_RPC_YEAR_AVG", F.col("OIL_SALES_BBL_YEAR_AVG") - F.col("SALES_OIL_BBL_270D_RPC_YEAR_AVG")) \
        .with_column("SALES_COND_M3_VS_RPC_YEAR_AVG", F.col("COND_SALES_M3_YEAR_AVG") -  F.col("SALES_COND_M3_270D_RPC_YEAR_AVG")) \
        .with_column("SALES_COND_BBL_VS_RPC_YEAR_AVG", F.col("COND_SALES_BBL_YEAR_AVG") - F.col("SALES_COND_BBL_270D_RPC_YEAR_AVG")) \
        .with_column("SALES_GAS_E3M3_VS_RPC_YEAR_AVG", F.col("GAS_SALES_E3M3_YEAR_AVG") -  F.col("SALES_GAS_E3M3_270D_RPC_YEAR_AVG")) \
        .with_column("SALES_GAS_MCF_VS_RPC_YEAR_AVG", F.col("GAS_SALES_MCF_YEAR_AVG") -  F.col("SALES_GAS_MCF_270D_RPC_YEAR_AVG")) \
        .with_column("SALES_GAS_BOE_VS_RPC_YEAR_AVG", F.col("GAS_SALES_BOE_YEAR_AVG") -  F.col("SALES_GAS_BOE_270D_RPC_YEAR_AVG"))
        # .with_column("SALES_BOE_RPC_PRODUCTION_EFFICIENCY", F.call_builtin("IFF", F.col("SALES_BOE_270D_RPC_YEAR_AVG") == 0, 1, F.col("SALES_BOE_YEAR_AVG") / F.col("SALES_BOE_270D_RPC_YEAR_AVG")))
            
        
    avocet_uwi_year_prior = avocet_uwi_daily.groupBy(F.col("UWI"), F.col("YEAR_START")) \
        .agg(
            F.avg(F.col("SALES_BOE")).alias("SALES_BOE_YEAR_AVG"),
            F.avg(F.col("SALES_M3E")).alias("SALES_M3E_YEAR_AVG"),
        ).select(
            F.col("UWI"),
            F.col("YEAR_START"),
            F.col("SALES_BOE_YEAR_AVG"),
            F.col("SALES_M3E_YEAR_AVG")
            )
        
        # join uwi_year_avgs with uwi_year_avgs_prior on UWI=UWI and YEAR_START=PRIOR_YEAR_START to get the previous week's averages
    avocet_uwi_year_with_priors = avocet_uwi_year.join(avocet_uwi_year_prior, 
                                          (avocet_uwi_year.UWI == avocet_uwi_year_prior.UWI) & (avocet_uwi_year.PRIOR_YEAR_START == avocet_uwi_year_prior.YEAR_START), 
                                            how='left', 
                                            rsuffix='_PRIOR_YEAR'
                                          ).drop(F.col("UWI_PRIOR_YEAR"),F.col("YEAR_START_PRIOR_YEAR")) \
                                        .with_column("SALES_BOE_VS_PRIOR_YEAR_AVG",  F.col("SALES_BOE_YEAR_AVG") - F.col("SALES_BOE_YEAR_AVG_PRIOR_YEAR")) \
                                        .with_column("SALES_M3E_VS_PRIOR_YEAR_AVG",  F.col("SALES_M3E_YEAR_AVG") - F.col("SALES_M3E_YEAR_AVG_PRIOR_YEAR")) \
                                        .with_column("YOY_DECLINE_PERCENT", F.call_builtin("IFF", F.col("SALES_BOE_YEAR_AVG_PRIOR_YEAR") == 0, 0, (F.col("SALES_BOE_YEAR_AVG") - F.col("SALES_BOE_YEAR_AVG_PRIOR_YEAR")) / F.col("SALES_BOE_YEAR_AVG_PRIOR_YEAR")))

    budget_year = budget.groupBy(F.col("UWI"), F.col("YEAR_START")) \
        .agg(
            F.avg(F.col("BUDGET_BOE")).alias("BUDGET_BOE_YEAR_AVG"),
            F.avg(F.col("BUDGET_M3E")).alias("BUDGET_M3E_YEAR_AVG"),
            F.max(F.col("AREA")).alias("AREA"),
            F.max(F.col("MAJOR_AREA")).alias("MAJOR_AREA"),
            F.max(F.col("TEAM")).alias("TEAM"),
        )
        
    print(budget_year.limit(5).show())

    avocet_uwi_year_with_priors = avocet_uwi_year_with_priors.join(budget_year,
                                                                   ['UWI', 'YEAR_START'],
                                                how='full', 
                                                rsuffix='_BUDGET'
                                            ) \
                                                .with_column("SALES_BOE_VS_BUDGET_YEAR_AVG",  F.call_builtin("ZEROIFNULL", F.col("SALES_BOE_YEAR_AVG")) - F.call_builtin("ZEROIFNULL", F.col("BUDGET_BOE_YEAR_AVG"))) \
                                                .with_column("SALES_M3E_VS_BUDGET_YEAR_AVG",  F.call_builtin("ZEROIFNULL", F.col("SALES_M3E_YEAR_AVG")) - F.call_builtin("ZEROIFNULL", F.col("BUDGET_M3E_YEAR_AVG"))) \
                                                .with_column("AREA", F.call_builtin("IFF", F.col("AREA").isNull(), F.col("AREA_BUDGET"), F.col("AREA"))) \
                                                .with_column("MAJOR_AREA", F.call_builtin("IFF", F.col("MAJOR_AREA").isNull(), F.col("MAJOR_AREA_BUDGET"), F.col("MAJOR_AREA"))) \
                                                .with_column("TEAM", F.call_builtin("IFF", F.col("TEAM").isNull(), F.col("TEAM_BUDGET"), F.col("TEAM"))) \
                                                    .dropna(subset=["SALES_BOE_YEAR_AVG", "BUDGET_BOE_YEAR_AVG"], how='all') \
                                                    .filter(~F.col("AREA").isin(['SAVANNA', 'BASHAW', 'WOOD RIVER']))
    # remove rows where sales and budget are both non 0 and non null 
    # avocet_uwi_year_with_priors = avocet_uwi_year_with_priors.filter((F.col("SALES_BOE_YEAR_AVG") != 0) | (F.col("BUDGET_BOE_YEAR_AVG") != 0))
     
    
    
    print(avocet_uwi_year_with_priors.limit(5).show())
        
    avocet_uwi_weekly_with_priors.write.mode("overwrite").saveAsTable("ANALYTICS.WEEKLY_PRODUCTION_REPORT")
    avocet_uwi_monthly_with_priors.write.mode("overwrite").saveAsTable("ANALYTICS.MONTHLY_PRODUCTION_REPORT")
    avocet_uwi_year_with_priors.write.mode("overwrite").saveAsTable("ANALYTICS.ANNUAL_PRODUCTION_REPORT")
    # session.write_pandas(output_df, "AVOCET_WELL_STATS", auto_create_table=True, overwrite=True)
    
        
    ## WEEKLY DOWNTIME
    avocet_uwi_weekly_downtime = avocet_uwi_daily.groupBy(F.col("UWI"), F.col("WEEK_START"), F.col("DOWNTIME_NAME")) \
        .agg(

            F.avg(F.col("DOWNTIME_BOE")).alias("DOWNTIME_BOE_WEEKLY_AVG"),
            F.avg(F.col("DOWNTIME_HOURS")).alias("DOWNTIME_HOURS_WEEKLY_AVG"),
            F.max(F.col("AREA")).alias("AREA"),
            F.max(F.col("MAJOR_AREA")).alias("MAJOR_AREA"),
            F.max(F.col("TEAM")).alias("TEAM"),
        )    
    
    avocet_uwi_weekly_downtime.write.mode("overwrite").saveAsTable("ANALYTICS.WEEKLY_DOWNTIME_REPORT")
    # if use_wh_scaling:
    #     _ = session.sql('ALTER WAREHOUSE CDE_WH SET WAREHOUSE_SIZE = XSMALL').collect()
    



def main(session: Session) -> str:
    # Create the WELLS_HISTORIAN table  if they don't exist
    # if not table_exists(session, schema='HARMONIZED', name='AVOCET_WELL_STATS'):
    #     create_table(session, schema='HARMONIZED', name='AVOCET_WELL_STATS')

    process(session)

    return f"Successfully processed WEEKLY_PRODUCTION_REPORT table."


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
