
from datetime import datetime, timedelta
from snowflake.snowpark import Session
#import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
from snowflake.snowpark.functions import udtf
from collections.abc import Iterable
from typing import Tuple
from snowflake.snowpark.functions import col, lit, sum as sum_, max as max_

def table_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    return exists
def create_table(session):
    _ = session.sql("CREATE TABLE HARMONIZED.SIMPLE_FINANCIAL_METRICS LIKE HARMONIZED.SIMPLE_FINANCIAL_METRICS_V").collect()



        # yield (a + b, )
# >>> df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
# >>> df.with_column("total", sum_udtf(df.a, df.b)).sort(df.a).show()

def update_table(session):
    
    # @udtf(output_schema=["string"])
    # class category_case_udf:
    #     def process(self, a: str) -> Iterable[Tuple[str]]:
    #         if a in ["EHS", "FIXED ACCRUALS", "LABOUR", "LEASE RENTALS & ROAD USE", "OVERHEAD", "PROPERTY TAXES", "REGULATORY & INSURANCE", "UTILITIES"]:
    #             yield ("FIXED", )
    #         else:
    #             yield ("VARIABLE", )
    
    # select all lease ops data, include only ORG_ID = [150, 151, 152, 153, 156, 157, 165, 499, 699]
    lease_ops = session.table("HARMONIZED.LEASE_OPS_G15") \
        .filter(F.col("ORG_ID").isin([150, 151, 152, 153, 156, 157, 165, 499, 699])) \
        .with_column("AMT", -F.col("AMT")) \
        .with_column("LEASE_OPS_CATEGORY", \
            F.call_builtin("IFF", F.col("ACCT_GROUP_DESC").isin(["EHS", "FIXED ACCRUALS", "LABOUR", "LEASE RENTALS & ROAD USE", "OVERHEAD", "PROPERTY TAXES", "REGULATORY & INSURANCE", "UTILITIES"]), "FIXED", \
            F.call_builtin("IFF", F.col("ACCT_GROUP_DESC").isin(["INSTRUMENTATION & ELECTRICAL", "PROD COST AFE PROJECTS", "REPAIRS & MAINTENANCE", "ROTATING EQUIPMENT MAINT"]), "R&M", \
            F.call_builtin("IFF", F.col("ACCT_GROUP_DESC").isin(["GAS REVENUE", "GEOLOGICAL & GEOPHYSICAL", "HEDGING REVENUE (GAS)", "HEDGING REVENUE (OIL)", "LIQUIDS REVENUE", "OIL AND PENTANE REVENUE", "PROCESSING INCOME", "ROYALTY INCOME - GAS", "ROYALTY INCOME  - LIQUIDS", "ROYALTY INCOME - OIL & PENTANE", "ROYALTY INCOME - SULPHUR", "SALES - MARKETING", "SULPHUR REVENUE", "TRANSPORTATION - GAS", "TRANSPORTATION - LIQUIDS", "TRANSPORTATION - OIL & PENTANE", "TRANSPORTATION - SULPHUR"]), "REVENUE", \
            F.call_builtin("IFF", F.col("ACCT_GROUP_DESC").isin(["ROYALTIES - GAS", "ROYALTIES - LIQUIDS", "ROYALTIES - OIL & PENTANE", "ROYALTIES - SULPHUR"]), "ROYALTIES", \
            F.call_builtin("IFF", F.col("ACCT_GROUP_DESC").isin(["CHEMICALS", "CONSULTING", "MISCELLANEOUS", "PROCESSING FEES", "RENTALS", "ROAD & LEASE MAINT", "TRUCKING", "VARIABLE ACCRUALS"]), "VARIABLE", \
            "OTHER")))))) \
        .select(F.col("LEASE_OPS_CATEGORY"), F.col("CC_NUM"), F.col("CC_NAME"), F.col("AMT"), F.col("DATE"), F.col("BOE"), F.col("TEAM"), F.col("AREA"))
            
    # print(lease_ops.limit(5).show())
    
    lease_ops = lease_ops \
        .groupBy("CC_NUM", "LEASE_OPS_CATEGORY", "DATE") \
        .agg(F.max("CC_NAME").alias("CC_NAME"), F.sum("AMT").alias("AMT"), F.sum("BOE").alias("BOE"), F.max("TEAM").alias("TEAM"), F.max("AREA").alias("AREA")) 
        
    # print(lease_ops.limit(5).show())
    
    lease_ops = lease_ops \
        .pivot("LEASE_OPS_CATEGORY", ["VARIABLE", "ROYALTIES", "REVENUE", "FIXED", "R&M", "OTHER"]).sum("AMT") \
        .select(F.col("CC_NUM"), \
                F.col("DATE"), \
                F.col("CC_NAME"), \
                F.col("TEAM"), \
                F.col("AREA"), \
                F.col("'VARIABLE'").alias("VARIABLE"), \
                F.col("'ROYALTIES'").alias("ROYALTIES"), \
                F.col("'REVENUE'").alias("REVENUE"), \
                F.col("'FIXED'").alias("FIXED"), \
                F.col("'R&M'").alias("R&M"), \
                F.col("'OTHER'").alias("OTHER"), \
                F.col("BOE") \
                ) \
        .with_column("VARIABLE", F.call_builtin("ZEROIFNULL", F.col("VARIABLE"))) \
        .with_column("ROYALTIES", F.call_builtin("ZEROIFNULL", F.col("ROYALTIES"))) \
        .with_column("REVENUE", F.call_builtin("ZEROIFNULL", F.col("REVENUE"))) \
        .with_column("FIXED", F.call_builtin("ZEROIFNULL", F.col("FIXED"))) \
        .with_column("R&M", F.call_builtin("ZEROIFNULL", F.col("R&M"))) \
        .with_column("OTHER", F.call_builtin("ZEROIFNULL", F.col("OTHER"))) \
        .with_column("BOE", F.call_builtin("ZEROIFNULL", F.col("BOE")))
    
    # print(lease_ops.limit(5).show()) 
    
    lease_ops = lease_ops \
        .groupBy("CC_NUM", "CC_NAME", "TEAM", "AREA", "DATE") \
            .agg(sum_("VARIABLE").alias("VARIABLE"), sum_("ROYALTIES").alias("ROYALTIES"), sum_("REVENUE").alias("REVENUE"), sum_("FIXED").alias("FIXED"), sum_("R&M").alias("R&M"), sum_("OTHER").alias("OTHER"), sum_("BOE").alias("BOE")) \
        .with_column("OPERATING_COSTS", F.col("FIXED") + F.col("VARIABLE") + F.col("R&M")) \
        .with_column("REVENUE_ROYALTIES", F.col("REVENUE") + F.col("ROYALTIES")) \
        .with_column("NET_OPERATING_INCOME", F.col("OPERATING_COSTS") + F.col("REVENUE_ROYALTIES")) \
        .with_column("LIFTING_COST", -F.call_builtin("IFF", F.col("BOE") == 0, 0, F.col("OPERATING_COSTS") / F.col("BOE"))) \
        .with_column("NETBACK", F.call_builtin("IFF", F.col("BOE") == 0, 0, F.col("NET_OPERATING_INCOME") / F.col("BOE")))


    # print(lease_ops.limit(5).show())
    
    # Create table if it does not exist, create a view first to infer schema. TODO: add schema manually
    if not table_exists(session, schema='HARMONIZED', name='SIMPLE_FINANCIAL_METRICS'):
        lease_ops.create_or_replace_view('HARMONIZED.SIMPLE_FINANCIAL_METRICS_V')
        create_table(session)
    
    # overwrite the table 
    lease_ops.write.mode("overwrite").saveAsTable("HARMONIZED.SIMPLE_FINANCIAL_METRICS")
    


def main(session: Session) -> str:
    update_table(session)
    
    return f"Successfully processed SIMPLE_FINANCIAL_METRICS table."


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
