from datetime import datetime, timedelta
from snowflake.snowpark import Session
#import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F


def create_ignition_wells_view(session):
    # session.use_schema('RAW_AVOCET')
    
    # get list of all tables in RAW_IGNITION
    
    tables = session.sql("select t.table_name from information_schema.tables t where t.table_schema = 'RAW_IGNITION' and t.table_type = 'BASE TABLE' order by t.table_name;").collect()    
    # include only those tables that have a name that starts with 'sqlt_data_1_' and includes the string designating either this month or last month
    # this month string example = '2021_10'
    # last month string example = '2021_09'
    this_month_string =  datetime.now().strftime('%Y_%m')
    last_month_string = (datetime.now() - timedelta(days=30)).strftime('%Y_%m')
    
    tables = [table[0] for table in tables if table[0].startswith('SQLT_DATA_1_') and (this_month_string in table[0] or last_month_string in table[0])]
    # tables = [table[0] for table in tables if table[0].startswith('SQLT_DATA_1_')  and ('2023' in table[0] or '2022' in table[0])]
    print(tables)
    
    noco_wells = session.table("RAW_NOCODB.WELLS").select(F.col("UWI"), \
                                                        F.col("QBYTE_2017MGMT-AREA").alias("AREA"), \
                                                        F.col("QBYTE_2017MGMT-TEAM").alias("TEAM"), \
                                                        F.upper(F.col("GAS_FLOW")).alias("GAS_FLOW"), \
                                                        F.upper(F.col("GAS_PRESS")).alias("GAS_PRESS"), \
                                                        F.upper(F.col("GAS_TEMP")).alias("GAS_TEMP"), \
                                                        F.upper(F.col("GAS_DP")).alias("GAS_DP"), \
                                                        # F.upper(F.col("GAS_VOL")).alias("GAS_VOL"), \
                                                        F.upper(F.col("PLUNGER")).alias("PLUNGER"), \
                                                        F.upper(F.col("ESDV_OPEN")).alias("ESDV_OPEN"), \
                                                        F.upper(F.col("CHOKE_POSITION")).alias("CHOKE_POSITION"), \
                                                        F.upper(F.col("ESDV_CLOSED")).alias("ESDV_CLOSED")) \
                                                    .filter(
                                                        (F.col("GAS_FLOW").isNotNull()) | 
                                                        (F.col("GAS_PRESS").isNotNull()) | 
                                                        (F.col("GAS_TEMP").isNotNull()) | 
                                                        (F.col("GAS_DP").isNotNull()) | 
                                                        # (F.col("GAS_VOL").isNotNull()) | 
                                                        (F.col("PLUNGER").isNotNull()) | 
                                                        (F.col("ESDV_OPEN").isNotNull()) | 
                                                        (F.col("CHOKE_POSITION").isNotNull()) | 
                                                        (F.col("ESDV_CLOSED").isNotNull())
                                                    )
                                                    # .where((F.col("UWI") == '100073102910W502') & (F.col("GAS_FLOW") is not None))
                                                    
                                                    
    # unpivot the GAS_FLOW and GAS_PRESS columns into a single column
    noco_wells = noco_wells.unpivot("TAG_OPC", "TAGNAME", ["GAS_FLOW", 
                                         "GAS_PRESS", 
                                         "GAS_TEMP", 
                                         "GAS_DP", 
                                        #  "GAS_VOL", 
                                         "PLUNGER", 
                                         "ESDV_OPEN",  
                                         "CHOKE_POSITION", 
                                         "ESDV_CLOSED"
                                         ])
                                
  
                                                    
    # create a list of unique tagids from the noco_wells table
    tag_opcs =  noco_wells.select(F.col("TAG_OPC")).distinct().collect()
    # flatten the list of tagids
    tag_opcs = [tag_opc[0] for tag_opc in tag_opcs]
                                                                
    # select FullPath as uppercase and TagName from the WILDCAT_IGN_TAG_ATTRIBUTES table
    wch_tags = session.table("RAW_IGNITION.WILDCAT_IGN_TAG_ATTRIBUTES") \
                        .select(F.upper(F.col('"FullPath"')).collate('en_us').alias("TAGPATH"), \
                                F.upper(F.concat(F.col('"FieldDeviceName"').collate('en_us'), F.lit('.'), F.col('"FieldTagName"').collate('en_us'))).alias("TAG_OPC"), \
                                F.col('"TagName"').collate('en_us').alias("TAGNAME")) \
                        .where(F.col("TAG_OPC").isin(tag_opcs))
    # print(wch_tags.limit(5).show())               
                                                    
    # create unique list of 'TAGPATH' values from the WILDCAT_IGN_TAG_ATTRIBUTES table
    tagpaths = wch_tags.select(F.col("TAGPATH")).distinct().collect()
    # print(tagpaths)
    
    # filter out retired tags (column 'retired' will be null for active tags)
    sqlth_te = session.table("RAW_IGNITION.SQLTH_TE") \
                        .select(F.upper(F.col('"tagpath"')).collate('en_us').alias("TAGPATH"), F.col('"id"').alias("TAGID")) \
                        .where(F.col('"retired"').isNull())                        
    # print(sqlth_te.limit(5).show())
            
    # select only the tags that are in the list of unique tagids
    wanted_tags = sqlth_te.select("TAGID").where(F.col("TAGPATH").isin(tagpaths)).collect()
    # flatten the list of tagids
    wanted_tags = [wanted_tag[0] for wanted_tag in wanted_tags]
    # print(wanted_tags)
    
    # loop through multiple tables and select the data for the tags in wanted_tags
    for index, table in enumerate(tables):
        ignition_data = session \
            .table(f"RAW_IGNITION.{table}") \
            .select(F.col('"tagid"').alias("TAGID"), \
                    F.col('"floatvalue"').alias("VALUE"), \
                    F.col('"t_stamp"').cast("STRING").alias("TIMESTAMP") ) \
            .with_column("TIMESTAMP", F.call_builtin("TO_TIMESTAMP_LTZ", F.col("TIMESTAMP")))
    
        # pull data for the tags in wanted_tags from ignition_data
        ignition_data = ignition_data.where(F.col("TAGID").isin(wanted_tags))
        
        #  union the data from each table
        if index == 0:
            combined_data = ignition_data
        else:
            combined_data = combined_data.union(ignition_data)
         
    # print(ignition_data.limit(5).show())
    
    combined_data = combined_data.join(sqlth_te, on="TAGID", how='left', rsuffix='_te')
    # print(combined_data.limit(5).show())
    
    combined_data = combined_data.join(wch_tags, on="TAGPATH", how='left', rsuffix='_data')
    # print(combined_data.limit(5).show())
    
    combined_data = combined_data.join(noco_wells, on="TAG_OPC", how='left', rsuffix='_wch')
    # print(combined_data.limit(5).show())

    # pivot the data so that each tag has its own column
    combined_data = combined_data.pivot("TAGNAME", 
                                        ["GAS_FLOW", 
                                         "GAS_PRESS", 
                                         "GAS_TEMP", 
                                         "GAS_DP", 
                                        #  "GAS_VOL", 
                                         "PLUNGER", 
                                         "ESDV_OPEN",  
                                         "CHOKE_POSITION", 
                                         "ESDV_CLOSED"
                                         ]).sum("VALUE") \
        .select(
            F.col("TIMESTAMP"), 
            F.col("UWI"), 
            F.col("AREA"), 
            F.col("TEAM"), 
            F.col("'GAS_FLOW'").alias("GAS_FLOW"), 
            F.col("'GAS_PRESS'").alias("GAS_PRESS"), 
            F.col("'GAS_TEMP'").alias("GAS_TEMP"), 
            F.col("'GAS_DP'").alias("GAS_DP"), 
            # F.col("'GAS_VOL'").alias("GAS_VOL"), 
            F.col("'PLUNGER'").alias("PLUNGER"), 
            F.col("'ESDV_OPEN'").alias("ESDV_OPEN"),
            F.col("'CHOKE_POSITION'").alias("CHOKE_POSITION"),
            F.col("'ESDV_CLOSED'").alias("ESDV_CLOSED")
        )
    # print(ignition_data.limit(5).show())
    
    
    # group by tagid and timestamp and avg the values
    combined_data = combined_data \
        .groupBy("TEAM", "AREA", "UWI", "TIMESTAMP").avg(
            "GAS_FLOW",
            "GAS_PRESS",
            "GAS_TEMP",
            "GAS_DP",
            # "GAS_VOL",
            "PLUNGER",
            "ESDV_OPEN",
            "CHOKE_POSITION",
            "ESDV_CLOSED"
        ) \
        .select(
            F.col("TEAM"), 
            F.col("AREA"), 
            F.col("UWI"), 
            F.col("TIMESTAMP"), 
            F.col('"AVG(GAS_FLOW)"').alias("GAS_FLOW"), 
            F.col('"AVG(GAS_PRESS)"').alias("GAS_PRESS"), 
            F.col('"AVG(GAS_TEMP)"').alias("GAS_TEMP"), 
            F.col('"AVG(GAS_DP)"').alias("GAS_DP"), 
            # F.col('"AVG(GAS_VOL)"').alias("GAS_VOL"), 
            F.col('"AVG(PLUNGER)"').alias("PLUNGER"), 
            F.col('"AVG(ESDV_OPEN)"').alias("ESDV_OPEN"),
            F.col('"AVG(CHOKE_POSITION)"').alias("CHOKE_POSITION"),
            F.col('"AVG(ESDV_CLOSED)"').alias("ESDV_CLOSED")
        )
    # print(ignition_data.limit(5).show())
    
    combined_data.create_or_replace_view('HARMONIZED.IGNITION_WELLS_V')

def create_ignition_wells_view_with_interpolation(session):
    # session.use_schema('RAW_AVOCET')
    
    # get list of all tables in RAW_IGNITION
    
    tables = session.sql("select t.table_name from information_schema.tables t where t.table_schema = 'RAW_IGNITION' and t.table_type = 'BASE TABLE' order by t.table_name;").collect()    
    # include only those tables that have a name that starts with 'sqlt_data_1_' and includes the string designating either this month or last month
    # this month string example = '2021_10'
    # last month string example = '2021_09'
    this_month_string =  datetime.now().strftime('%Y_%m')
    last_month_string = (datetime.now() - timedelta(days=30)).strftime('%Y_%m')
    
    tables = [table[0] for table in tables if table[0].startswith('SQLT_DATA_1_') and (this_month_string in table[0] or last_month_string in table[0])]
    # tables = [table[0] for table in tables if table[0].startswith('SQLT_DATA_1_')  and ('2023' in table[0] or '2022' in table[0])]
    print(tables)
    
    noco_wells = session.table("RAW_NOCODB.WELLS").select(F.col("UWI"), \
                                                        F.col("QBYTE_2017MGMT-AREA").alias("AREA"), \
                                                        F.col("QBYTE_2017MGMT-TEAM").alias("TEAM"), \
                                                        F.upper(F.col("GAS_FLOW")).alias("GAS_FLOW"), \
                                                        F.upper(F.col("GAS_PRESS")).alias("GAS_PRESS"), \
                                                        F.upper(F.col("GAS_TEMP")).alias("GAS_TEMP"), \
                                                        F.upper(F.col("GAS_DP")).alias("GAS_DP"), \
                                                        # F.upper(F.col("GAS_VOL")).alias("GAS_VOL"), \
                                                        F.upper(F.col("PLUNGER")).alias("PLUNGER"), \
                                                        F.upper(F.col("ESDV_OPEN")).alias("ESDV_OPEN"), \
                                                        F.upper(F.col("CHOKE_POSITION")).alias("CHOKE_POSITION"), \
                                                        F.upper(F.col("ESDV_CLOSED")).alias("ESDV_CLOSED")) \
                                                    .filter(
                                                        (F.col("GAS_FLOW").isNotNull()) | 
                                                        (F.col("GAS_PRESS").isNotNull()) | 
                                                        (F.col("GAS_TEMP").isNotNull()) | 
                                                        (F.col("GAS_DP").isNotNull()) | 
                                                        # (F.col("GAS_VOL").isNotNull()) | 
                                                        (F.col("PLUNGER").isNotNull()) | 
                                                        (F.col("ESDV_OPEN").isNotNull()) | 
                                                        (F.col("CHOKE_POSITION").isNotNull()) | 
                                                        (F.col("ESDV_CLOSED").isNotNull())
                                                    )
                                                    # .where((F.col("UWI") == '100073102910W502') & (F.col("GAS_FLOW") is not None))
                                                    
                                                    
    # unpivot the GAS_FLOW and GAS_PRESS columns into a single column
    noco_wells = noco_wells.unpivot("TAG_OPC", "TAGNAME", ["GAS_FLOW", 
                                         "GAS_PRESS", 
                                         "GAS_TEMP", 
                                         "GAS_DP", 
                                        #  "GAS_VOL", 
                                         "PLUNGER", 
                                         "ESDV_OPEN",  
                                         "CHOKE_POSITION", 
                                         "ESDV_CLOSED"
                                         ])
                                
  
                                                    
    # create a list of unique tagids from the noco_wells table
    tag_opcs =  noco_wells.select(F.col("TAG_OPC")).distinct().collect()
    # flatten the list of tagids
    tag_opcs = [tag_opc[0] for tag_opc in tag_opcs]
                                                                
    # select FullPath as uppercase and TagName from the WILDCAT_IGN_TAG_ATTRIBUTES table
    wch_tags = session.table("RAW_IGNITION.WILDCAT_IGN_TAG_ATTRIBUTES") \
                        .select(F.upper(F.col('"FullPath"')).collate('en_us').alias("TAGPATH"), \
                                F.upper(F.concat(F.col('"FieldDeviceName"').collate('en_us'), F.lit('.'), F.col('"FieldTagName"').collate('en_us'))).alias("TAG_OPC"), \
                                F.col('"TagName"').collate('en_us').alias("TAGNAME")) \
                        .where(F.col("TAG_OPC").isin(tag_opcs))
    # print(wch_tags.limit(5).show())               
                                                    
    # create unique list of 'TAGPATH' values from the WILDCAT_IGN_TAG_ATTRIBUTES table
    tagpaths = wch_tags.select(F.col("TAGPATH")).distinct().collect()
    # print(tagpaths)
    
    # filter out retired tags (column 'retired' will be null for active tags)
    sqlth_te = session.table("RAW_IGNITION.SQLTH_TE") \
                        .select(F.upper(F.col('"tagpath"')).collate('en_us').alias("TAGPATH"), F.col('"id"').alias("TAGID")) \
                        .where(F.col('"retired"').isNull())                        
    # print(sqlth_te.limit(5).show())
            
    # select only the tags that are in the list of unique tagids
    wanted_tags = sqlth_te.select("TAGID").where(F.col("TAGPATH").isin(tagpaths)).collect()
    # flatten the list of tagids
    wanted_tags = [wanted_tag[0] for wanted_tag in wanted_tags]
    # print(wanted_tags)
    
    # loop through multiple tables and select the data for the tags in wanted_tags
    for index, table in enumerate(tables):
        ignition_data = session \
            .table(f"RAW_IGNITION.{table}") \
            .select(F.col('"tagid"').alias("TAGID"), \
                    F.col('"floatvalue"').alias("VALUE"), \
                    F.col('"t_stamp"').cast("STRING").alias("TIMESTAMP") ) \
            .with_column("TIMESTAMP", F.call_builtin("TO_TIMESTAMP_LTZ", F.col("TIMESTAMP")))
    
        # pull data for the tags in wanted_tags from ignition_data
        ignition_data = ignition_data.where(F.col("TAGID").isin(wanted_tags))
        
        #  union the data from each table
        if index == 0:
            combined_data = ignition_data
        else:
            combined_data = combined_data.union(ignition_data)
         
    # print(ignition_data.limit(5).show())
    
    combined_data = combined_data.join(sqlth_te, on="TAGID", how='left', rsuffix='_te')
    # print(combined_data.limit(5).show())
    
    combined_data = combined_data.join(wch_tags, on="TAGPATH", how='left', rsuffix='_data')
    # print(combined_data.limit(5).show())
    
    combined_data = combined_data.join(noco_wells, on="TAG_OPC", how='left', rsuffix='_wch')
    # print(combined_data.limit(5).show())


    # before pivoting, interpolate using functions provided by Chris Waters https://github.com/jchriswaters/Snowflake_SQL_Interpolation/blob/main/SnowflakeInterpolation
    
    

    # pivot the data so that each tag has its own column
    combined_data = combined_data.pivot("TAGNAME", 
                                        ["GAS_FLOW", 
                                         "GAS_PRESS", 
                                         "GAS_TEMP", 
                                         "GAS_DP", 
                                        #  "GAS_VOL", 
                                         "PLUNGER", 
                                         "ESDV_OPEN",  
                                         "CHOKE_POSITION", 
                                         "ESDV_CLOSED"
                                         ]).sum("VALUE") \
        .select(
            F.col("TIMESTAMP"), 
            F.col("UWI"), 
            F.col("AREA"), 
            F.col("TEAM"), 
            F.col("'GAS_FLOW'").alias("GAS_FLOW"), 
            F.col("'GAS_PRESS'").alias("GAS_PRESS"), 
            F.col("'GAS_TEMP'").alias("GAS_TEMP"), 
            F.col("'GAS_DP'").alias("GAS_DP"), 
            # F.col("'GAS_VOL'").alias("GAS_VOL"), 
            F.col("'PLUNGER'").alias("PLUNGER"), 
            F.col("'ESDV_OPEN'").alias("ESDV_OPEN"),
            F.col("'CHOKE_POSITION'").alias("CHOKE_POSITION"),
            F.col("'ESDV_CLOSED'").alias("ESDV_CLOSED")
        )
    # print(ignition_data.limit(5).show())
    
    
    # group by tagid and timestamp and avg the values
    combined_data = combined_data \
        .groupBy("TEAM", "AREA", "UWI", "TIMESTAMP").avg(
            "GAS_FLOW",
            "GAS_PRESS",
            "GAS_TEMP",
            "GAS_DP",
            # "GAS_VOL",
            "PLUNGER",
            "ESDV_OPEN",
            "CHOKE_POSITION",
            "ESDV_CLOSED"
        ) \
        .select(
            F.col("TEAM"), 
            F.col("AREA"), 
            F.col("UWI"), 
            F.col("TIMESTAMP"), 
            F.col('"AVG(GAS_FLOW)"').alias("GAS_FLOW"), 
            F.col('"AVG(GAS_PRESS)"').alias("GAS_PRESS"), 
            F.col('"AVG(GAS_TEMP)"').alias("GAS_TEMP"), 
            F.col('"AVG(GAS_DP)"').alias("GAS_DP"), 
            # F.col('"AVG(GAS_VOL)"').alias("GAS_VOL"), 
            F.col('"AVG(PLUNGER)"').alias("PLUNGER"), 
            F.col('"AVG(ESDV_OPEN)"').alias("ESDV_OPEN"),
            F.col('"AVG(CHOKE_POSITION)"').alias("CHOKE_POSITION"),
            F.col('"AVG(ESDV_CLOSED)"').alias("ESDV_CLOSED")
        )
    # print(ignition_data.limit(5).show())
    
    combined_data.create_or_replace_view('HARMONIZED.IGNITION_WELLS_V')



def test_ignition_view(session):
    session.use_schema('HARMONIZED')
    tv = session.table('IGNITION_WELLS_V')
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
    
    create_ignition_wells_view(session)
    test_ignition_view(session)

    session.close()
