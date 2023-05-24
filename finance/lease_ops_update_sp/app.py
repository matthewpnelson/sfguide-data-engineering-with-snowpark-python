
from datetime import datetime, timedelta
from snowflake.snowpark import Session
#import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F


def table_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    return exists
def create_table(session):
    _ = session.sql("CREATE TABLE HARMONIZED.LEASE_OPS_G15 LIKE HARMONIZED.LEASE_OPS_G15_V").collect()

def update_table(session):
    
    data = session.sql("""
                        select	ca.major_acct||'.'||ca.minor_acct as acct_code,
                        ca.ACCT_year as year,
                        ca.org_id,
                        rt.reporting_entity_code as team,
                        rma.reporting_entity_code as major_area,
                        ra.reporting_entity_code as area,
                        'VOL' as class_code,
                        ca.major_acct,
                        ma.major_acct_desc,
                        ca.minor_acct,
                        acc.acct_desc,
                        acc.prod_code,
                        CA.CC_NUM,
                        ca.ACCT_per_date as DATE,
                        to_char(ca.ACCT_per_date, 'YYYYMM') as YEARMONTH,
                        ca.cc_actual_amt as amt,
                        0 as vol_metric,
                        0 as vol_imp,
                        0 as mcfem
                        from RAW_QBYTE.reporting_entities rt
                        join RAW_QBYTE.reporting_entities rma on rt.hierarchy_code = rma.hierarchy_code and rt.reporting_level_code = rma.parent_reporting_level_code and rt.reporting_entity_id = rma.parent_reporting_entity_id
                        join RAW_QBYTE.reporting_entities ra on rma.hierarchy_code = ra.hierarchy_code and rma.reporting_level_code = ra.parent_reporting_level_code and rma.reporting_entity_id = ra.parent_reporting_entity_id
                        join RAW_QBYTE.reporting_entities rf on ra.hierarchy_code = rf.hierarchy_code and ra.reporting_level_code = rf.parent_reporting_level_code and ra.reporting_entity_id = rf.parent_reporting_entity_id
                        join RAW_QBYTE.reporting_entities rc on rf.hierarchy_code = rc.hierarchy_code and rf.reporting_level_code = rc.parent_reporting_level_code and rf.reporting_entity_id = rc.parent_reporting_entity_id
                        join RAW_QBYTE.cc_actuals ca on rc.reporting_entity_code = ca.cc_num
                        join RAW_QBYTE.accounts acc on acc.major_acct = ca.major_acct and acc.minor_acct = ca.minor_acct
                        join RAW_QBYTE.major_accounts ma on ca.major_acct = ma.major_acct
                        left join RAW_QBYTE.products prod on acc.prod_code = prod.prod_code
                        where rc.hierarchy_code = '2017MGMT'
                        and rc.reporting_level_code = 'CC'
                        and ca.curr_code = 'CAD'
                        and ma.major_acct = '1500'
                        and ca.minor_acct <> 'CLR'
                        and ma.gross_or_net_code = 'N'
                        and ca.ACCT_per_date > '2020-01-01'
                        
                        UNION 
                        
                        select ca.major_acct||'.'||ca.minor_acct as acct_code,
                        ca.ACCT_year as year,
                        ca.org_id,
                        rt.reporting_entity_code as team,
                        rma.reporting_entity_code as major_area,
                        ra.reporting_entity_code as area,
                        'VOL' as class_code,
                        ca.major_acct,
                        ma.major_acct_desc,
                        ca.minor_acct,
                        acc.acct_desc,
                        acc.prod_code,
                        CA.CC_NUM,
                        ca.ACCT_per_date as DATE,
                        to_char(ca.ACCT_per_date, 'YYYYMM') as YEARMONTH,
                        0 as amt,
                        round(ca.cc_actual_vol* 1 , 1) * -1 as vol_metric,
                        ca.cc_actual_vol* prod.SI_TO_IMP_CONV_FACTOR *-1 as vol_imp,
                        ca.cc_actual_vol * prod.mcfe6_thermal * -1 as mcfem
                        from RAW_QBYTE.reporting_entities rt
                        join RAW_QBYTE.reporting_entities rma on rt.hierarchy_code = rma.hierarchy_code and rt.reporting_level_code = rma.parent_reporting_level_code and rt.reporting_entity_id = rma.parent_reporting_entity_id
                        join RAW_QBYTE.reporting_entities ra on rma.hierarchy_code = ra.hierarchy_code and rma.reporting_level_code = ra.parent_reporting_level_code and rma.reporting_entity_id = ra.parent_reporting_entity_id
                        join RAW_QBYTE.reporting_entities rf on ra.hierarchy_code = rf.hierarchy_code and ra.reporting_level_code = rf.parent_reporting_level_code and ra.reporting_entity_id = rf.parent_reporting_entity_id
                        join RAW_QBYTE.reporting_entities rc on rf.hierarchy_code = rc.hierarchy_code and rf.reporting_level_code = rc.parent_reporting_level_code and rf.reporting_entity_id = rc.parent_reporting_entity_id
                        join RAW_QBYTE.cc_actuals ca on rc.reporting_entity_code = ca.cc_num
                        join RAW_QBYTE.accounts acc on acc.major_acct = ca.major_acct and acc.minor_acct = ca.minor_acct
                        join RAW_QBYTE.major_accounts ma on ca.major_acct = ma.major_acct
                        left join RAW_QBYTE.products prod on acc.prod_code = prod.prod_code
                        where rc.hierarchy_code = '2017MGMT'
                        and rc.reporting_level_code = 'CC'
                        and ca.curr_code = 'CAD'
                        and ma.class_code in ('RV')
                        and ca.minor_acct <> 'CLR'
                        and ma.gross_or_net_code = 'N'
                        and ca.ACCT_per_date > '2020-01-01'
                        
                        UNION
                        
                        select ca.major_acct||'.'||ca.minor_acct as acct_code,
                        ca.ACCT_year as year,
                        ca.org_id,
                        rt.reporting_entity_code as team,
                        rma.reporting_entity_code as major_area,
                        ra.reporting_entity_code as area,
                        ma.class_code,
                        ca.major_acct,
                        ma.major_acct_desc,
                        ca.minor_acct,
                        acc.acct_desc,
                        acc.prod_code,
                        CA.CC_NUM,
                        ca.ACCT_per_date as DATE,
                        to_char(ca.ACCT_per_date, 'YYYYMM') as YEARMONTH,
                        ca.cc_actual_amt as amt,
                        0 as vol_metric,
                        0 as vol_imp,
                        0 as mcfem
                        from RAW_QBYTE.reporting_entities rt
                        join RAW_QBYTE.reporting_entities rma on rt.hierarchy_code = rma.hierarchy_code and rt.reporting_level_code = rma.parent_reporting_level_code and rt.reporting_entity_id = rma.parent_reporting_entity_id
                        join RAW_QBYTE.reporting_entities ra on rma.hierarchy_code = ra.hierarchy_code and rma.reporting_level_code = ra.parent_reporting_level_code and rma.reporting_entity_id = ra.parent_reporting_entity_id
                        join RAW_QBYTE.reporting_entities rf on ra.hierarchy_code = rf.hierarchy_code and ra.reporting_level_code = rf.parent_reporting_level_code and ra.reporting_entity_id = rf.parent_reporting_entity_id
                        join RAW_QBYTE.reporting_entities rc on rf.hierarchy_code = rc.hierarchy_code and rf.reporting_level_code = rc.parent_reporting_level_code and rf.reporting_entity_id = rc.parent_reporting_entity_id
                        join RAW_QBYTE.cc_actuals ca on rc.reporting_entity_code = ca.cc_num
                        join RAW_QBYTE.accounts acc on acc.major_acct = ca.major_acct and acc.minor_acct = ca.minor_acct
                        join RAW_QBYTE.major_accounts ma on ca.major_acct = ma.major_acct
                        left join RAW_QBYTE.products prod on acc.prod_code = prod.prod_code
                        where rc.hierarchy_code = '2017MGMT'
                        and rc.reporting_level_code = 'CC'
                        and ca.curr_code = 'CAD'
                        and ma.class_code in ('PE', 'RV', 'RY', 'GA', 'OA', 'EX')
                        and ca.minor_acct <> 'CLR'
                        and ma.gross_or_net_code = 'N'
                        and ca.ACCT_per_date > '2020-01-01'
                                """)
    
    print(data.limit(5).show())
    
    
    g_15 = session.sql("""
                        select ag.display_seq_num, agr.rollup_acct_group_code, ag.acct_group_desc, aga.major_acct as t_major, aga.minor_acct as t_minor
                        from RAW_QBYTE.account_group_accounts aga
                        join RAW_QBYTE.account_groups ag on aga.acct_group_type_code = ag.acct_group_type_code and aga.acct_group_code = ag.acct_group_code
                        left join RAW_QBYTE.account_group_rollups agr on aga.acct_group_type_code = agr.acct_group_type_code and aga.acct_group_code = agr.acct_group_code
                        join RAW_QBYTE.major_accounts ma on aga.major_acct = ma.major_acct
                        join RAW_QBYTE.accounts ac on aga.major_acct = ac.major_acct and aga.minor_acct = ac.minor_acct
                        where aga.acct_group_type_code = 'GRP15'
                        and ma.gross_or_net_code in ('N')
                        order by ma.class_code, ag.display_seq_num, ag.acct_group_desc, aga.major_acct, aga.minor_acct
                    """)
    
    joined = data.join(g_15, (data.major_acct == g_15.t_major) & (data.minor_acct == g_15.t_minor), 'inner')
    
    
    
    joined = joined \
            .with_column("BOE", F.call_builtin("IFF", F.col("PROD_CODE") == 'GAS', F.col("VOL_IMP") / 6, F.col("VOL_IMP"))) \
            .with_column("AMT", F.call_builtin("IFF", F.col("ACCT_GROUP_DESC") == 'PROPRIETARY MIDSTREAM (INCOME) FEES', -F.col("AMT"), F.col("AMT")))
    
    
    organizations = session.table("RAW_QBYTE.ORGANIZATIONS").select(F.col("ORG_ID"), F.col("ORG_NAME"))
    
    cost_centres = session.table("RAW_QBYTE.COST_CENTRES").select(F.col("CC_NUM"), \
                                                                F.col("CC_TYPE_CODE"), \
                                                                F.col("CC_NAME"), \
                                                                F.col("OWNERSHIP_ORG_ID"), \
                                                                F.col("CREATE_DATE"), \
                                                                F.col("TERM_DATE"), \
                                                                F.col("OPERATOR_CLIENT_ID"), \
                                                                F.col("WELL_STAT_CODE"), \
                                                                F.col("PRIMARY_PROD_CODE"), \
                                                                F.col("CONTRACT_OPERATOR_CLIENT_ID") \
                                                                    )
    
    cc_info = cost_centres.join(organizations, cost_centres.OWNERSHIP_ORG_ID == organizations.ORG_ID, how='left').rename("ORG_NAME", "OWNERSHIP_ORG_NAME").drop("ORG_ID")
    # print(cc_info.limit(5).show())
    business_associates = session.table("RAW_QBYTE.BUSINESS_ASSOCIATES").select(F.col("ID"), \
                                                                            F.col("NAME_1").alias("NAME"))
    
    cc_info = cc_info.join(business_associates, cc_info.OPERATOR_CLIENT_ID == business_associates.ID, how='left').rename("NAME", "OPERATOR_NAME").drop("ID")
    # print(cc_info.limit(5).show())
    cc_info = cc_info.join(business_associates, cc_info.CONTRACT_OPERATOR_CLIENT_ID == business_associates.ID, how='left').rename("NAME", "CONTRACT_OPERATOR_NAME").drop("ID")
    
    # print(cc_info.limit(5).show())
    
    joined = joined.join(cc_info, on="CC_NUM", how='left', rsufix="_r").drop("CC_NUM_r")
    
    
    codes = session.table("RAW_QBYTE.CODES").select(F.col("CODE"), \
                                                    F.col("CODE_DESC").alias("CC_TYPE_CODE_DESC")) \
                                            .where(F.col("CODE_TYPE_CODE") == 'CC_TYPE_CODE')
    
    joined = joined.join(codes, joined.CC_TYPE_CODE == codes.CODE, how='left').drop("CODE")
    print(joined.limit(5).show())
    
    # Create table if it does not exist, create a view first to infer schema. TODO: add schema manually
    if not table_exists(session, schema='HARMONIZED', name='LEASE_OPS_G15'):
        joined.create_or_replace_view('HARMONIZED.LEASE_OPS_G15_V')
        create_table(session)
    
    # overwrite the table 
    joined.write.mode("overwrite").saveAsTable("HARMONIZED.LEASE_OPS_G15")
    


def main(session: Session) -> str:
    update_table(session)
    
    return f"Successfully processed LEASE_OPS_G15 table."


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
