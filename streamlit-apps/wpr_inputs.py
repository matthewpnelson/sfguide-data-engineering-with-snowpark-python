import datetime
import re
import numpy as np
import pandas as pd
import streamlit as st
import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T
import snowflake.snowpark.exceptions as E
import plotly.express as px
import plotly.graph_objects as go
st.set_page_config(layout="wide", page_title="WPR Comments", page_icon="🧮")

st.title("Weekly Production Report: Comment Entry")
st.write("This app is used to enter comments for the Canlin Weekly Production Report. Week over Week comments must be added each week. Versus RPC comments are ")

def table_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    return exists

def create_majarea_table(session, schema, name):
    SHARED_COLUMNS= [
                        T.StructField("TEAM", T.StringType()),
                        T.StructField("MAJOR_AREA", T.StringType()),
                        T.StructField("WEEK_START", T.TimestampType()),
                        T.StructField("COMMENT", T.StringType())
                    ]
    
    SCHEMA = T.StructType(SHARED_COLUMNS)

    table = session.create_dataframe([[None]*len(SCHEMA.names)], schema=SCHEMA) \
                        .na.drop() \
                        .write.mode('overwrite').save_as_table(f'{schema}.{name}')
                        
    return table
def create_team_table(session, schema, name):
    SHARED_COLUMNS= [
                        T.StructField("TEAM", T.StringType()),
                        T.StructField("WEEK_START", T.TimestampType()),
                        T.StructField("COMMENT", T.StringType())
                    ]
    
    SCHEMA = T.StructType(SHARED_COLUMNS)

    table = session.create_dataframe([[None]*len(SCHEMA.names)], schema=SCHEMA) \
                        .na.drop() \
                        .write.mode('overwrite').save_as_table(f'{schema}.{name}')
                        
    return table


from snowflake.snowpark import Session
connection_parameters = {
   "account": "canlinenergy-analytics",
   "user": "op_input",
   "password": "NuySRBaL3XWfzLsg",
   "role": "OP_INPUT_ROLE",  # optional
   "warehouse": "CDE_WH",  # optional
   "database": "CANLIN",  # optional
#    "schema": "<your snowflake schema>",  # optional
 }  

session = Session.builder.configs(connection_parameters).create()

selected_week = st.sidebar.date_input('Select Week (Monday through Sunday)', datetime.date.today() - datetime.timedelta(days=7))
team_selection = st.sidebar.selectbox(
    'Select Team',
    ['WILDCAT', 'EAST', 'EDSON', 'PRA', 'CENTRAL']
)

# truncate date to week start
week_start = selected_week - datetime.timedelta(days=selected_week.weekday())
week_end = week_start + datetime.timedelta(days=6)

# write and format dates as "Monday May 1, 2023" 
st.subheader(f"Entering for {week_start.strftime('%A')} {week_start.strftime('%B')} {week_start.strftime('%d')}, {week_start.strftime('%Y')} through {week_end.strftime('%A')} {week_end.strftime('%B')} {week_end.strftime('%d')}, {week_end.strftime('%Y')}")

def get_comments_data(_session, week_start, team_selection):
    
    #  Create table if it does not exist
    if not table_exists(session, schema='MANUAL_INPUTS', name='WPR_TEAM_COMMENTS_WOW'):
        create_team_table(session, 'MANUAL_INPUTS', 'WPR_TEAM_COMMENTS_WOW')
    #  Create table if it does not exist
    if not table_exists(session, schema='MANUAL_INPUTS', name='WPR_MAJORAREA_COMMENTS_WOW'):
        create_majarea_table(session, 'MANUAL_INPUTS', 'WPR_MAJORAREA_COMMENTS_WOW')
    #  Create table if it does not exist
    if not table_exists(session, schema='MANUAL_INPUTS', name='WPR_TEAM_COMMENTS_RPC'):
        create_team_table(session, 'MANUAL_INPUTS', 'WPR_TEAM_COMMENTS_RPC')
    #  Create table if it does not exist
    if not table_exists(session, schema='MANUAL_INPUTS', name='WPR_MAJORAREA_COMMENTS_RPC'):
        create_majarea_table(session, 'MANUAL_INPUTS', 'WPR_MAJORAREA_COMMENTS_RPC')
    
    wpr_team_comments_wow = session.table('MANUAL_INPUTS.WPR_TEAM_COMMENTS_WOW') \
                    .filter((F.col('WEEK_START') == week_start) & (F.col('TEAM') == team_selection)) \
                        .select(
                            F.col("TEAM"),
                            F.col("WEEK_START"),
                            F.col("COMMENT")
                        ).to_pandas()
    
    wpr_majarea_comments_wow = session.table('MANUAL_INPUTS.WPR_MAJORAREA_COMMENTS_WOW') \
                    .filter((F.col('WEEK_START') == week_start) & (F.col('TEAM') == team_selection)) \
                        .select(
                            F.col("TEAM"),
                            F.col("MAJOR_AREA"),
                            F.col("WEEK_START"),
                            F.col("COMMENT")
                        ).to_pandas()
                        
    wpr_team_comments_rpc = session.table('MANUAL_INPUTS.WPR_TEAM_COMMENTS_RPC') \
                    .filter((F.col('WEEK_START') == week_start) & (F.col('TEAM') == team_selection)) \
                        .select(
                            F.col("TEAM"),
                            F.col("WEEK_START"),
                            F.col("COMMENT")
                        ).to_pandas()
                        
    wpr_majarea_comments_rpc = session.table('MANUAL_INPUTS.WPR_MAJORAREA_COMMENTS_RPC') \
                    .filter((F.col('WEEK_START') == week_start) & (F.col('TEAM') == team_selection)) \
                        .select(
                            F.col("TEAM"),
                            F.col("MAJOR_AREA"),
                            F.col("WEEK_START"),
                            F.col("COMMENT")
                        ).to_pandas()
    
    # get distinct MAJOR_AREA for selected team from table CANLIN.ANALYTICS.WEEKLY_PRODUCTION_REPORT
    majarea_list = session.table('CANLIN.ANALYTICS.WEEKLY_PRODUCTION_REPORT') \
                        .filter((F.col('TEAM') == team_selection) & (F.col('SALES_BOE_WEEKLY_AVG') > 0)) \
                        .select(
                            F.col("MAJOR_AREA")
                        ).distinct().to_pandas()['MAJOR_AREA'].tolist()               
    
    if wpr_majarea_comments_wow.empty:
    
        # add a single row for each team
        for maj_area in majarea_list:
            session.sql(f"""INSERT INTO MANUAL_INPUTS.WPR_MAJORAREA_COMMENTS_WOW (TEAM, MAJOR_AREA, WEEK_START, COMMENT) VALUES ('{team_selection}', '{maj_area}', '{week_start}', '')""").collect()
        
        wpr_majarea_comments_wow = session.table('MANUAL_INPUTS.WPR_MAJORAREA_COMMENTS_WOW') \
                    .filter((F.col('WEEK_START') == week_start) & (F.col('TEAM') == team_selection)) \
                        .select(
                            F.col("TEAM"),
                            F.col("MAJOR_AREA"),
                            F.col("WEEK_START"),
                            F.col("COMMENT")
                        ).to_pandas()
    
    if wpr_majarea_comments_rpc.empty:
    
        # get last week's comments if any
        wpr_majarea_comments_rpc = session.table('MANUAL_INPUTS.WPR_MAJORAREA_COMMENTS_RPC') \
                    .filter((F.col('WEEK_START') == week_start - datetime.timedelta(days=7)) & (F.col('TEAM') == team_selection)) \
                        .select(
                            F.col("TEAM"),
                            F.col("MAJOR_AREA"),
                            F.col("WEEK_START"),
                            F.col("COMMENT")
                        ).to_pandas()
                        
        # add a single row for each team, filling in last week's comments if any else blank
        for maj_area in majarea_list:
            if wpr_majarea_comments_rpc.empty:
                session.sql(f"""INSERT INTO MANUAL_INPUTS.WPR_MAJORAREA_COMMENTS_RPC (TEAM, MAJOR_AREA, WEEK_START, COMMENT) VALUES ('{team_selection}', '{maj_area}', '{week_start}', '')""").collect()
            else:
                session.sql(f"""INSERT INTO MANUAL_INPUTS.WPR_MAJORAREA_COMMENTS_RPC (TEAM, MAJOR_AREA, WEEK_START, COMMENT) VALUES ('{team_selection}', '{maj_area}', '{week_start}', '{wpr_majarea_comments_rpc[wpr_majarea_comments_rpc['MAJOR_AREA'] == maj_area]['COMMENT'].iloc[0]}')""").collect()
        
        wpr_majarea_comments_rpc = session.table('MANUAL_INPUTS.WPR_MAJORAREA_COMMENTS_RPC') \
                    .filter((F.col('WEEK_START') >= week_start) & (F.col('TEAM') == team_selection)) \
                        .select(
                            F.col("TEAM"),
                            F.col("MAJOR_AREA"),
                            F.col("WEEK_START"),
                            F.col("COMMENT")
                        ).to_pandas()
    
        
    return wpr_team_comments_wow, wpr_majarea_comments_wow, wpr_team_comments_rpc, wpr_majarea_comments_rpc, majarea_list

@st.cache_data()
def get_wpr_data(_session, week_start, team_selection):
   
    wpr_data = session.table('CANLIN.ANALYTICS.WEEKLY_PRODUCTION_REPORT') \
                    .filter((F.col('WEEK_START') >= week_start) & (F.col('TEAM') == team_selection)) \
                        .select(
                            F.col("TEAM"),
                            F.col("MAJOR_AREA"),
                            F.col("AREA"),
                            F.col("UWI"),
                            F.col("WEEK_START"),
                            F.col("SALES_BOE_WEEKLY_AVG"),
                            F.col("SALES_M3E_WEEKLY_AVG"),
                            F.col("SALES_BOE_WEEKLY_AVG_PRIOR_WEEK"),
                            F.col("SALES_M3E_WEEKLY_AVG_PRIOR_WEEK"),
                            F.col("SALES_BOE_VS_RPC_WEEKLY_AVG"),
                            F.col("SALES_M3E_VS_RPC_WEEKLY_AVG"),
                            F.col("SALES_BOE_VS_PRIOR_WEEK_AVG"),
                            F.col("SALES_M3E_VS_PRIOR_WEEK_AVG"),
                            F.col("SALES_BOE_270D_RPC_WEEKLY_AVG")                             
                        ).to_pandas()
        
    return wpr_data

wpr_data = get_wpr_data(session, week_start, team_selection)

wpr_data_team = wpr_data.groupby("TEAM").agg(
                            {"SALES_BOE_WEEKLY_AVG": "sum",
                            "SALES_M3E_WEEKLY_AVG": "sum",
                            "SALES_BOE_WEEKLY_AVG_PRIOR_WEEK": "sum",
                            "SALES_M3E_WEEKLY_AVG_PRIOR_WEEK": "sum",
                            "SALES_BOE_VS_RPC_WEEKLY_AVG": "sum",
                            "SALES_M3E_VS_RPC_WEEKLY_AVG": "sum",
                            "SALES_BOE_VS_PRIOR_WEEK_AVG": "sum",
                            "SALES_M3E_VS_PRIOR_WEEK_AVG": "sum",
                            "SALES_BOE_270D_RPC_WEEKLY_AVG": "sum"
                            }
                        )

wpr_data_major_area = wpr_data.groupby(["TEAM", "MAJOR_AREA"]).agg(
     {"SALES_BOE_WEEKLY_AVG": "sum",
                            "SALES_M3E_WEEKLY_AVG": "sum",
                            "SALES_BOE_WEEKLY_AVG_PRIOR_WEEK": "sum",
                            "SALES_M3E_WEEKLY_AVG_PRIOR_WEEK": "sum",
                            "SALES_BOE_VS_RPC_WEEKLY_AVG": "sum",
                            "SALES_M3E_VS_RPC_WEEKLY_AVG": "sum",
                            "SALES_BOE_VS_PRIOR_WEEK_AVG": "sum",
                            "SALES_M3E_VS_PRIOR_WEEK_AVG": "sum",
                            "SALES_BOE_270D_RPC_WEEKLY_AVG": "sum"
                            }
                        )

wpr_data_area = wpr_data.groupby(["TEAM", "MAJOR_AREA", "AREA"]).agg(
     {"SALES_BOE_WEEKLY_AVG": "sum",
                            "SALES_M3E_WEEKLY_AVG": "sum",
                            "SALES_BOE_WEEKLY_AVG_PRIOR_WEEK": "sum",
                            "SALES_M3E_WEEKLY_AVG_PRIOR_WEEK": "sum",
                            "SALES_BOE_VS_RPC_WEEKLY_AVG": "sum",
                            "SALES_M3E_VS_RPC_WEEKLY_AVG": "sum",
                            "SALES_BOE_VS_PRIOR_WEEK_AVG": "sum",
                            "SALES_M3E_VS_PRIOR_WEEK_AVG": "sum",
                            "SALES_BOE_270D_RPC_WEEKLY_AVG": "sum"
                            }
                        )

tab1, tab2, tab3 = st.tabs(["Week over Week", "Versus RPC", "Understand the Numbers"])
wpr_team_comments_wow, wpr_majarea_comments_wow, wpr_team_comments_rpc, wpr_majarea_comments_rpc, majarea_list = get_comments_data(session, week_start, team_selection)

# generate plotly color map for major area list
color_map = {}
for i in range(len(majarea_list)):
    color_map[majarea_list[i]] = px.colors.qualitative.Plotly[i]

### WEEK OVER WEEK ###
col1, col2 = tab1.columns(2)
    
bar_fig1 = px.bar(wpr_data_major_area.reset_index(), y="MAJOR_AREA", x="SALES_BOE_VS_PRIOR_WEEK_AVG", height=300, text_auto='.2s', 
                 title='Week over Week Changes in Sales (BOE) by Major Area', color='MAJOR_AREA', orientation='h', hover_name="MAJOR_AREA", hover_data=['SALES_BOE_VS_PRIOR_WEEK_AVG'], color_discrete_map=color_map)
bar_fig1.update_layout(
    xaxis=dict(
        showline=True,
        showgrid=False,
        showticklabels=True,
        linecolor='rgb(204, 204, 204)',
        linewidth=2,
        ticks='outside',
        tickfont=dict(
            family='Arial',
            size=12,
            color='rgb(82, 82, 82)',
        ),
        title=None
    ),
    # yaxis=dict(
    #     showgrid=False,
    #     zeroline=False,
    #     showline=False,
    #     showticklabels=False,
    #     title=None,
    # ),
    autosize=False,
    margin=dict(
        autoexpand=False,
        l=100,
        r=20,
        t=110,
        
    ),
    showlegend=False,
    plot_bgcolor='white'
)
bar_fig1.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
col2.plotly_chart(bar_fig1, use_container_width=True)

bar_fig = px.bar(wpr_data.reset_index(), y="AREA", x="SALES_BOE_VS_PRIOR_WEEK_AVG", height=600, text_auto='.2s', 
                 title='Week over Week Changes in Sales (BOE) by Area', color='MAJOR_AREA', orientation='h', hover_name="MAJOR_AREA", hover_data=['UWI', 'SALES_BOE_VS_PRIOR_WEEK_AVG'], color_discrete_map=color_map)
bar_fig.update_layout(
    xaxis=dict(
        showline=True,
        showgrid=False,
        showticklabels=True,
        linecolor='rgb(204, 204, 204)',
        linewidth=2,
        ticks='outside',
        tickfont=dict(
            family='Arial',
            size=12,
            color='rgb(82, 82, 82)',
        ),
        title=None
    ),
    # yaxis=dict(
    #     showgrid=False,
    #     zeroline=False,
    #     showline=False,
    #     showticklabels=False,
    #     title=None,
    # ),
    autosize=False,
    margin=dict(
        autoexpand=False,
        l=100,
        r=20,
        t=110,
        
    ),
    showlegend=False,
    plot_bgcolor='white'
)
bar_fig.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
col2.plotly_chart(bar_fig, use_container_width=True)






with col1.form("team_wow_form"):
    # pull in existing comment if it exists
    team_comment_new = st.text_area(label=team_selection, value=wpr_team_comments_wow[wpr_team_comments_wow['TEAM'] == team_selection]['COMMENT'].values[0] if team_selection in wpr_team_comments_wow['TEAM'].values else '', placeholder='Enter an overall Team comment here')

    submitted = st.form_submit_button(label='Save Team Level Comment', type='primary')

    if submitted:

        try: 
            # delete any existing comment
            session.sql(f"""DELETE FROM MANUAL_INPUTS.WPR_TEAM_COMMENTS_WOW WHERE TEAM = '{team_selection}' AND WEEK_START = '{week_start}'""").collect()
        finally:
            session.sql(f"""INSERT INTO MANUAL_INPUTS.WPR_TEAM_COMMENTS_WOW (TEAM, WEEK_START, COMMENT) VALUES ('{team_selection}', '{week_start}', '{team_comment_new}')""").collect()
            st.write(f'Comment for {team_selection} submitted')
            
def delete_all_major_area_comments(for_date):
    session.sql(f"""DELETE FROM CANLIN.MANUAL_INPUTS.WPR_MAJORAREA_COMMENTS_WOW WHERE TEAM = '{team_selection}' AND WEEK_START = '{for_date}' """).collect()
    # wpr_team_comments_wow, wpr_majarea_comments_wow, majarea_list = get_comments_data(session, week_start, team_selection)
    
def submit_new_major_area_comments(new_comments):
        for_date = new_comments['WEEK_START'].values[0]
        delete_all_major_area_comments(for_date)
        session.use_schema("MANUAL_INPUTS")
        session.write_pandas(new_comments, table_name='WPR_MAJORAREA_COMMENTS_WOW', database='CANLIN', schema='MANUAL_INPUTS', overwrite=False)
        
        # wpr_team_comments_wow, wpr_majarea_comments_wow, majarea_list = get_comments_data(session, week_start, team_selection)
        st.success('Comments Uploaded & Overwritten in Database', icon="✅")
            

with col1.form("major_area_wow_form"):
    # build a dictionary with the major area as the key and the comment as the value.
    new_comments = {}
    for majarea in majarea_list:
        new_comments[majarea] = st.text_area(label=majarea, value=wpr_majarea_comments_wow[wpr_majarea_comments_wow['MAJOR_AREA'] == majarea]['COMMENT'].values[0] if majarea in wpr_majarea_comments_wow['MAJOR_AREA'].values else '', placeholder='Enter comment here')
          
    # team_comment_new = st.text_area(label=team_selection, value=wpr_team_comments_wow[wpr_team_comments_wow['TEAM'] == team_selection]['COMMENT'].values[0] if team_selection in wpr_team_comments_wow['TEAM'].values else 'No comments...')

    submitted2 = st.form_submit_button(label='Save Major Area Level Comments', type='primary')

    if submitted2:
        
        new_comments_df = pd.DataFrame.from_dict(new_comments, orient='index', columns=['COMMENT'])
        new_comments_df['TEAM'] = team_selection
        new_comments_df['WEEK_START'] = pd.to_datetime(week_start).strftime('%Y-%m-%d %H:%M:%S.%f %z')
        new_comments_df['MAJOR_AREA'] = new_comments_df.index
        
        submit_new_major_area_comments(new_comments_df)
        
### VS RPC ###
col3, col4 = tab2.columns(2)
    
bar_fig1 = px.bar(wpr_data_major_area.reset_index(), y="MAJOR_AREA", x="SALES_BOE_VS_RPC_WEEKLY_AVG", height=300, text_auto='.2s', 
                 title='Current Week vs RPC in Sales (BOE) by Major Area', color='MAJOR_AREA', orientation='h', hover_name="MAJOR_AREA", hover_data=['SALES_BOE_VS_RPC_WEEKLY_AVG'], color_discrete_map=color_map)
bar_fig1.update_layout(
    xaxis=dict(
        showline=True,
        showgrid=False,
        showticklabels=True,
        linecolor='rgb(204, 204, 204)',
        linewidth=2,
        ticks='outside',
        tickfont=dict(
            family='Arial',
            size=12,
            color='rgb(82, 82, 82)',
        ),
        title=None
    ),
    # yaxis=dict(
    #     showgrid=False,
    #     zeroline=False,
    #     showline=False,
    #     showticklabels=False,
    #     title=None,
    # ),
    autosize=False,
    margin=dict(
        autoexpand=False,
        l=100,
        r=20,
        t=110,
        
    ),
    showlegend=False,
    plot_bgcolor='white'
)
bar_fig1.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
col4.plotly_chart(bar_fig1, use_container_width=True)

bar_fig = px.bar(wpr_data.reset_index(), y="AREA", x="SALES_BOE_VS_RPC_WEEKLY_AVG", height=600, text_auto='.2s', 
                 title='Current Week vs RPC in Sales (BOE) by Area', color='MAJOR_AREA', orientation='h', hover_name="MAJOR_AREA", hover_data=['UWI', 'SALES_BOE_VS_RPC_WEEKLY_AVG'], color_discrete_map=color_map)
bar_fig.update_layout(
    xaxis=dict(
        showline=True,
        showgrid=False,
        showticklabels=True,
        linecolor='rgb(204, 204, 204)',
        linewidth=2,
        ticks='outside',
        tickfont=dict(
            family='Arial',
            size=12,
            color='rgb(82, 82, 82)',
        ),
        title=None
    ),
    # yaxis=dict(
    #     showgrid=False,
    #     zeroline=False,
    #     showline=False,
    #     showticklabels=False,
    #     title=None,
    # ),
    autosize=False,
    margin=dict(
        autoexpand=False,
        l=100,
        r=20,
        t=110,
        
    ),
    showlegend=False,
    plot_bgcolor='white'
)
bar_fig.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
col4.plotly_chart(bar_fig, use_container_width=True)






st.caption('RPC is a target rate for each individual well, which can look deceiving when rolled up because it is rare for all wells to hit target at the same time. Comments are still helpful to understand the overall story for a team.')
with col3.form("team_rpc_form"):
    team_comment_new = st.text_area(label=team_selection, value=wpr_team_comments_rpc[wpr_team_comments_rpc['TEAM'] == team_selection]['COMMENT'].values[0] if team_selection in wpr_team_comments_rpc['TEAM'].values else '', placeholder='We will try to pre-populate this with your comment from the previous week, if it exists.')

    submitted = st.form_submit_button(label='Save Team Level Comment', type='primary')

    if submitted:

        try: 
            # delete any existing comment
            session.sql(f"""DELETE FROM MANUAL_INPUTS.WPR_TEAM_COMMENTS_RPC WHERE TEAM = '{team_selection}' AND WEEK_START = '{week_start}'""").collect()
        finally:
            session.sql(f"""INSERT INTO MANUAL_INPUTS.WPR_TEAM_COMMENTS_RPC (TEAM, WEEK_START, COMMENT) VALUES ('{team_selection}', '{week_start}', '{team_comment_new}')""").collect()
            st.write(f'Comment for {team_selection} submitted')
            
def delete_all_major_area_comments_rpc(for_date):
    session.sql(f"""DELETE FROM CANLIN.MANUAL_INPUTS.WPR_MAJORAREA_COMMENTS_RPC WHERE TEAM = '{team_selection}' AND WEEK_START = '{for_date}' """).collect()
    # wpr_team_comments_rpc, wpr_majarea_comments_rpc, majarea_list = get_comments_data(session, week_start, team_selection)
    
def submit_new_major_area_comments_rpc(new_comments):
        for_date = new_comments['WEEK_START'].values[0]
        delete_all_major_area_comments_rpc(for_date)
        session.use_schema("MANUAL_INPUTS")
        session.write_pandas(new_comments, table_name='WPR_MAJORAREA_COMMENTS_RPC', database='CANLIN', schema='MANUAL_INPUTS', overwrite=False)
        
        # wpr_team_comments_rpc, wpr_majarea_comments_rpc, majarea_list = get_comments_data(session, week_start, team_selection)
        st.success('Comments Uploaded & Overwritten in Database', icon="✅")
            

with col3.form("major_area_rpc_form"):
    # build a dictionary with the major area as the key and the comment as the value.
    new_comments = {}
    for majarea in majarea_list:
        new_comments[majarea] = st.text_area(label=majarea, value=wpr_majarea_comments_rpc[wpr_majarea_comments_rpc['MAJOR_AREA'] == majarea]['COMMENT'].values[0] if majarea in wpr_majarea_comments_rpc['MAJOR_AREA'].values else '', placeholder='We will try to pre-populate this with your comment from the previous week, if it exists.')
          
    # team_comment_new = st.text_area(label=team_selection, value=wpr_team_comments_rpc[wpr_team_comments_rpc['TEAM'] == team_selection]['COMMENT'].values[0] if team_selection in wpr_team_comments_rpc['TEAM'].values else 'No comments...')

    submitted2 = st.form_submit_button(label='Save Major Area Level Comments', type='primary')

    if submitted2:
        
        new_comments_df = pd.DataFrame.from_dict(new_comments, orient='index', columns=['COMMENT'])
        new_comments_df['TEAM'] = team_selection
        new_comments_df['WEEK_START'] = pd.to_datetime(week_start).strftime('%Y-%m-%d %H:%M:%S.%f %z')
        new_comments_df['MAJOR_AREA'] = new_comments_df.index
        
        submit_new_major_area_comments_rpc(new_comments_df)

            
# # update comments with experimental_data_editor
# with st.form("major_area_rpc_form"):
#     new_comments = st.experimental_data_editor(wpr_majarea_comments_rpc, key='new_comments', use_container_width=True, num_rows="fixed")
    
#     submitted2 = st.form_submit_button(label='Save Team Level Comment', type='primary')

#     if submitted2:

#         submit_new_major_area_comments(new_comments)

# set colors to red/green
fig = px.treemap(wpr_data.replace(0, np.nan), path=[px.Constant('CORPORATE'), 'TEAM', 'MAJOR_AREA', 'AREA', 'UWI'], values='SALES_BOE_WEEKLY_AVG',
                  color='SALES_BOE_VS_PRIOR_WEEK_AVG', hover_data=['SALES_BOE_WEEKLY_AVG', 'SALES_BOE_VS_PRIOR_WEEK_AVG'], height=1200,
                   color_continuous_scale=px.colors.diverging.RdYlGn, title='Sales (BOE) Changes Week over Week')
tab3.plotly_chart(fig, use_container_width=True)