
from datetime import datetime, timedelta
from snowflake.snowpark import Session
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F


def table_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    return exists
def create_wells_historian_table(session):
    SHARED_COLUMNS= [
                        T.StructField("UWI", T.StringType()),
                        T.StructField("TIMESTAMP", T.DateType()),
                        T.StructField("AREA", T.StringType()),
                        T.StructField("MAJOR_AREA", T.StringType()),
                        T.StructField("TEAM", T.StringType()),
                        T.StructField("GAS_FLOW", T.DecimalType()),
                        T.StructField("GAS_PRESS", T.DecimalType()),
                        T.StructField("GAS_TEMP", T.DecimalType()),
                        T.StructField("GAS_DP", T.DecimalType()),
                        T.StructField("PLUNGER", T.DecimalType()),
                        T.StructField("ESDV_OPEN", T.DecimalType()),
                        T.StructField("CHOKE_POSITION", T.DecimalType()),
                        T.StructField("ESDV_CLOSED", T.DecimalType()),
                        T.StructField("STATUS", T.StringType()),
                        T.StructField("AVAILABILITY", T.IntegerType()),
                        T.StructField("ETO_DATE", T.StringType()),
                        T.StructField("NOTES", T.StringType()),
                    ]
    
    # AVOCET_PROD_DETAIL_COLUMNS = [*SHARED_COLUMNS, T.StructField("META_UPDATED_AT", T.TimestampType())]
    HISTORIAN_DETAIL_SCHEMA = T.StructType(SHARED_COLUMNS)

    dcm = session.create_dataframe([[None]*len(HISTORIAN_DETAIL_SCHEMA.names)], schema=HISTORIAN_DETAIL_SCHEMA) \
                        .na.drop() \
                        .write.mode('overwrite').save_as_table('HARMONIZED.WELLS_HISTORIAN')
    dcm = session.table('HARMONIZED.WELLS_HISTORIAN')
    # _ = session.sql("CREATE TABLE HARMONIZED.WELLS_HISTORIAN LIKE HARMONIZED.IGNITION_WELLS_V").collect()


def update_historian_stream(session):
    
    # get list of all tables in RAW_IGNITION
    tables = session.sql("select t.table_name from information_schema.tables t where t.table_schema = 'RAW_IGNITION' and t.table_type = 'BASE TABLE' order by t.table_name;").collect()    
    
    # include the most recent table that has a name that starts with 'sqlt_data_1_' and has the max YEAR and max MONTH as defined by the table name, example 2023_12
    tables = [table[0] for table in tables if table[0].startswith('SQLT_DATA_1_')]
    tables = [table for table in tables if table.split('_')[-2] == max([table.split('_')[-2] for table in tables])]
    tables = [table for table in tables if table.split('_')[-1] == max([table.split('_')[-1] for table in tables])]
    print(tables[0])
    
    _ = session.sql(f'''
                        ALTER TABLE RAW_IGNITION.{tables[0]} SET CHANGE_TRACKING = TRUE;
                    ''').collect()
    
    # Pull the current source for the RAW_IGNITION.HISTORIAN_STREAM stream
    current_stream = session.sql(f'''
                        SHOW STREAMS LIKE 'HISTORIAN_STREAM';
                    ''').collect()

    
    # if tables[0] is in the string current_stream[0].base_tables then we don't need to update the stream
    # ensure [0] index exists in both
    if len(current_stream) == 0:
        print(f"RAW_IGNITION.HISTORIAN_STREAM does not exist")
        _ = session.sql(f'''
                            CREATE OR REPLACE STREAM RAW_IGNITION.HISTORIAN_STREAM 
                            ON TABLE RAW_IGNITION.{tables[0]} 
                            SHOW_INITIAL_ROWS = TRUE;
                        ''').collect()
        
        print(f"RAW_IGNITION.HISTORIAN_STREAM created to watch table {tables[0]}")
        current_stream = session.sql(f'''
                        SHOW STREAMS LIKE 'HISTORIAN_STREAM';
                    ''').collect()
         
    if tables[0] in current_stream[0].base_tables:
        print(f"Current Stream Base Tables: {current_stream[0].base_tables}")
        print(f"RAW_IGNITION.HISTORIAN_STREAM already watching table {tables[0]}")
        
    else:
    
        _ = session.sql(f'''
                            CREATE OR REPLACE STREAM RAW_IGNITION.HISTORIAN_STREAM 
                            ON TABLE RAW_IGNITION.{tables[0]} 
                            SHOW_INITIAL_ROWS = TRUE;
                        ''').collect()
        
        print(f"RAW_IGNITION.HISTORIAN_STREAM updated to watch table {tables[0]}")

def update_wells_historian_table(session):
    use_wh_scaling = False
    update_or_overwrite = 'UPDATE' # 'UPDATE' or 'OVERWRITE' -- Execute OVERWRITE locally to pull in historical data earlier than the current month
    
    
    # if update_or_overwrite == 'OVERWRITE':
    #     use_wh_scaling = True
        
    if use_wh_scaling:
        print("Setting warehouse size to LARGE")
        _ = session.sql('ALTER WAREHOUSE CDE_WH SET WAREHOUSE_SIZE = LARGE WAIT_FOR_COMPLETION = TRUE').collect()
   
    if update_or_overwrite == 'UPDATE':
        print("{} records in stream".format(session.table('RAW_IGNITION.HISTORIAN_STREAM').count()))
        stream_dates = session.table('RAW_IGNITION.HISTORIAN_STREAM').select(F.col("DATETIME")).distinct()
        stream_dates.limit(5).show()
   
    # get list of all tables in RAW_IGNITION
    tables = session.sql("select t.table_name from information_schema.tables t where t.table_schema = 'RAW_IGNITION' and t.table_type = 'BASE TABLE' order by t.table_name;").collect()    
    # include only those tables that have a name that starts with 'sqlt_data_1_' and includes 2023 or 2022
    # tables = [table[0] for table in tables if table[0].startswith('SQLT_DATA_1_') and '2023' in table[0]]
    tables = [table[0] for table in tables if table[0].startswith('SQLT_DATA_1_')  and ('2023' in table[0] or '2022' in table[0])]
    print(tables)
    
    corpwell = session.table('CANLIN_SCHEMA."ds_corpwell_uwi_canlin"').select(F.col("UWI_ID").collate('en_us').alias("UWI"), \
                                                                            F.col("TEAM"), \
                                                                            F.col('MAJOR_AREA'), \
                                                                            F.col("AREA"), \
                                                                            F.col("QBYTE_CC_NUM_CLEAN").alias("CC_NUM"), \
                                                                            )
  
    
    noco_wells = session.table("RAW_NOCODB.WELLS").select(F.col("UWI"), \
                                                        F.upper(F.col("GAS_FLOW")).alias("GAS_FLOW"), \
                                                        F.upper(F.col("GAS_PRESS")).alias("GAS_PRESS"), \
                                                        F.upper(F.col("GAS_TEMP")).alias("GAS_TEMP"), \
                                                        F.upper(F.col("GAS_DP")).alias("GAS_DP"), \
                                                        # F.upper(F.col("GAS_VOL")).alias("GAS_VOL"), \
                                                        F.upper(F.col("PLUNGER")).alias("PLUNGER"), \
                                                        F.upper(F.col("ESDV_OPEN")).alias("ESDV_OPEN"), \
                                                        F.upper(F.col("CHOKE_POSITION")).alias("CHOKE_POSITION"), \
                                                        F.upper(F.col("STATUS")).alias("STATUS"), \
                                                        F.upper(F.col("ETO_DATE")).alias("ETO_DATE"), \
                                                        F.upper(F.col("NOTES")).alias("NOTES"), \
                                                        F.upper(F.col("AVAILABILITY")).alias("AVAILABILITY"), \
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
                                                        (F.col("STATUS").isNotNull()) | 
                                                        (F.col("ETO_DATE").isNotNull()) | 
                                                        (F.col("NOTES").isNotNull()) | 
                                                        (F.col("AVAILABILITY").isNotNull()) | 
                                                        (F.col("ESDV_CLOSED").isNotNull())
                                                    )
                                                    # .where((F.col("UWI") == '100141503010W500') & (F.col("GAS_FLOW") is not None))
                                         
    noco_wells = noco_wells.join(corpwell, on="UWI", how="left")                                                    
    print(noco_wells.limit(5).show())                                  
    # unpivot the GAS_FLOW and GAS_PRESS columns into a single column
    noco_wells = noco_wells.unpivot("TAG_OPC", "TAGNAME", ["GAS_FLOW", 
                                         "GAS_PRESS", 
                                         "GAS_TEMP", 
                                         "GAS_DP", 
                                        #  "GAS_VOL", 
                                         "PLUNGER", 
                                         "ESDV_OPEN",  
                                         "CHOKE_POSITION", 
                                         "STATUS", 
                                         "ETO_DATE", 
                                         "NOTES", 
                                         "AVAILABILITY", 
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
    # actually, we don't want to filter out retired tags because we want to keep the data for the tags that are retired! Use this for any future queries which need only the current tag...
    if update_or_overwrite == 'UPDATE':
        sqlth_te = session.table("RAW_IGNITION.SQLTH_TE") \
                        .select(F.upper(F.col('"tagpath"')).collate('en_us').alias("TAGPATH"), F.col('"id"').alias("TAGID")) \
                        .where(F.col('"retired"').isNull()) \
                        .distinct()
    else:
        sqlth_te = session.table("RAW_IGNITION.SQLTH_TE") \
                        .select(F.upper(F.col('"tagpath"')).collate('en_us').alias("TAGPATH"), F.col('"id"').alias("TAGID")).distinct()
                        # .where(F.col('"retired"').isNull())                        
    # print(sqlth_te.limit(5).show())
            
    # select only the tags that are in the list of unique tagids
    wanted_tags = sqlth_te.select("TAGID").where(F.col("TAGPATH").isin(tagpaths)).collect()
    # print(wanted_tags)
    # flatten the list of tagids
    wanted_tags = [wanted_tag[0] for wanted_tag in wanted_tags]
    # print(wanted_tags)
    
    # loop through multiple tables and select the data for the tags in wanted_tags
    
    if update_or_overwrite == 'UPDATE':
        combined_data = session \
            .table(f"RAW_IGNITION.HISTORIAN_STREAM") \
            .select(F.col('"tagid"').alias("TAGID"), \
                    F.col('"floatvalue"').alias("VALUE"), \
                    F.col('"t_stamp"').cast("STRING").alias("TIMESTAMP") ) \
            .with_column("TIMESTAMP", F.call_builtin("TO_TIMESTAMP_LTZ", F.col("TIMESTAMP")))
    
    if update_or_overwrite == 'OVERWRITE':
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
            
    # print min date and max date
    print(combined_data.agg(F.min("TIMESTAMP"), F.max("TIMESTAMP")).show())
    # print(ignition_data.limit(5).show())
    
    combined_data = combined_data.join(sqlth_te, on="TAGID", how='left', rsuffix='_te')
    # print(combined_data.limit(5).show())
    
    combined_data = combined_data.join(wch_tags, on="TAGPATH", how='left', rsuffix='_data')
    # print(combined_data.limit(5).show())
    
    combined_data = combined_data.join(noco_wells, on="TAG_OPC", how='left', rsuffix='_wch')
    # print(combined_data.limit(5).show())

    # # output the long table 
    # # print(combined_data.limit(5).show())
    # combined_data.write.mode("overwrite").saveAsTable("HARMONIZED.WELLS_HISTORIAN_LONG")
    

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
                                         ]).max("VALUE") \
        .select(
            F.col("TIMESTAMP"), 
            F.col("UWI"), 
            F.col("AREA"), 
            F.col("MAJOR_AREA"),
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
        .groupBy("TEAM", "MAJOR_AREA", "AREA", "UWI", "TIMESTAMP").avg(
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
            F.col("MAJOR_AREA"),
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
        ) \
        .with_column("STATUS", F.lit(None)) \
        .with_column("ETO_DATE", F.lit(None)) \
        .with_column("NOTES", F.lit(None)) \
        .with_column("AVAILABILITY", F.lit(None).cast("INT")) 
    # print(combined_data.limit(5).show())
    
    
    # soft tags
    
    soft_tags = session.table("RAW_IGNITION.SQLT_DATA_TABLEAU").select( \
                                                                F.col('"intvalue"').cast("STRING").alias("INTVALUE"), \
                                                                F.col('"stringvalue"').alias("STRINGVALUE"), \
                                                                F.col('"datevalue"').cast("STRING").alias("DATEVALUE"), \
                                                                F.upper(F.col('"tagpath"').collate('en_us')).alias("TAG_OPC"), \
                                                                F.col('"t_stamp"').cast("STRING").alias("TIMESTAMP") ) \
                                                        .with_column("TIMESTAMP", F.call_builtin("TO_TIMESTAMP_LTZ", F.col("TIMESTAMP"))) \
                                                        .with_column("STRINGVALUE", F.call_builtin("IFF", F.col("DATEVALUE").isNotNull(), F.col("DATEVALUE"), F.col("STRINGVALUE"))) \
                                                        .with_column("value", F.call_builtin("IFF", F.col("INTVALUE").isNotNull(), F.col("INTVALUE"), F.col("STRINGVALUE"))) \
                                                        .drop("INTVALUE", "STRINGVALUE", "DATEVALUE") \
    
    # join on noco_wells.tag_opc = soft_tags.tagname
    soft_tags = soft_tags.join(noco_wells, on="TAG_OPC", how='left', rsuffix='_soft')
    
    # pivot TAGNAME and VALUE
    soft_tags = soft_tags.pivot("TAGNAME", ["STATUS", "AVAILABILITY", "ETO_DATE", "NOTES"]).max("VALUE") \
    .select(
            F.col("TIMESTAMP"), 
            F.col("UWI"), 
            F.col("AREA"), 
            F.col("MAJOR_AREA"),
            F.col("TEAM"), 
            F.col("'STATUS'").cast("STRING").alias("STATUS"), 
            F.col("'ETO_DATE'").cast("STRING").alias("ETO_DATE"), 
            F.col("'NOTES'").cast("STRING").alias("NOTES"), 
            F.col("'AVAILABILITY'").cast("INT").alias("AVAILABILITY")
        )
    
    soft_tags = soft_tags \
        .groupBy("TEAM", "MAJOR_AREA", "AREA", "UWI", "TIMESTAMP").max(
            "STATUS",
            "ETO_DATE",
            "NOTES",
            "AVAILABILITY"
        ) \
        .select(
            F.col("TEAM"), 
            F.col("MAJOR_AREA"),
            F.col("AREA"), 
            F.col("UWI"), 
            F.col("TIMESTAMP"), 
            F.col('"MAX(STATUS)"').alias("STATUS"), 
            F.col('"MAX(ETO_DATE)"').alias("ETO_DATE"), 
            F.col('"MAX(NOTES)"').alias("NOTES"), 
            F.col('"MAX(AVAILABILITY)"').alias("AVAILABILITY")
        ) \
        .with_column("GAS_FLOW", F.lit(None).cast("FLOAT")) \
        .with_column("GAS_PRESS", F.lit(None).cast("FLOAT")) \
        .with_column("GAS_TEMP", F.lit(None).cast("FLOAT")) \
        .with_column("GAS_DP", F.lit(None).cast("FLOAT")) \
        .with_column("PLUNGER", F.lit(None).cast("FLOAT")) \
        .with_column("ESDV_OPEN", F.lit(None).cast("FLOAT")) \
        .with_column("ESDV_CLOSED", F.lit(None).cast("FLOAT")) \
        .with_column("CHOKE_POSITION", F.lit(None).cast("FLOAT"))
    
    # print(soft_tags.limit(5).show())
    
    # union the two dataframes
    combined_data = combined_data.union_by_name(soft_tags)
    
    # print(combined_data.limit(5).show())
                    
    if update_or_overwrite == 'UPDATE':
        cols_to_update = {c: combined_data[c] for c in combined_data.schema.names}
        # metadata_col_to_update = {"META_UPDATED_AT": F.current_timestamp()}
        updates = {
            **cols_to_update,
            # **metadata_col_to_update
            }

        sf_table = session.table('HARMONIZED.WELLS_HISTORIAN')
        sf_table.merge(combined_data, (sf_table['TIMESTAMP'] == combined_data['TIMESTAMP']) & (sf_table['UWI'] == combined_data['UWI']), \
                            [F.when_matched().update(updates), F.when_not_matched().insert(updates)])

    if update_or_overwrite == 'OVERWRITE':
        # excludes META columns...
        # overwrite the table 
        combined_data.write.mode("overwrite").saveAsTable("HARMONIZED.WELLS_HISTORIAN")
    
    if use_wh_scaling:
        print("Scaling down warehouse size to XSMALL")
        _ = session.sql('ALTER WAREHOUSE CDE_WH SET WAREHOUSE_SIZE = XSMALL').collect()


def main(session: Session) -> str:
    # Create the WELLS_HISTORIAN table  if they don't exist
    if not table_exists(session, schema='HARMONIZED', name='WELLS_HISTORIAN'):
        create_wells_historian_table(session)
        
    # if current date is the first of the month, update the stream
    # if datetime.today().day == 1:
    update_historian_stream(session)
    update_wells_historian_table(session)

    return f"Successfully processed WELLS_HISTORIAN table."


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
