# start a basic streamlit app

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
st.set_page_config(layout="wide", page_title="Labeling: Operational Events - Wells", page_icon="🧮")

st.title("Labeling Application: Operational Events - Wells")
st.write("This app is used to label operational data for training a predictive machine learning model.")


def table_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    return exists

def create_table(session):
    SHARED_COLUMNS= [
                        T.StructField("UWI", T.StringType()),
                        T.StructField("TIMESTAMP", T.TimestampType()),
                        T.StructField("LABEL", T.StringType())
                    ]
    
    SCHEMA = T.StructType(SHARED_COLUMNS)

    table = session.create_dataframe([[None]*len(SCHEMA.names)], schema=SCHEMA) \
                        .na.drop() \
                        .write.mode('overwrite').save_as_table('LABELS.OPERATIONS_WELL')
                        
    return table

import os, sys
current_dir = os.getcwd()
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from utils import snowpark_utils


# The following expires every day - use once streamlit is running in snowflake
# session = snowpark_utils.get_snowpark_session()

from snowflake.snowpark import Session
connection_parameters = {
   "account": "canlinenergy-analytics",
   "user": "mnelson",
   "password": "IxH6L5kg0MCYaq1Yqv9e",
   "role": "CDE_ROLE",  # optional
   "warehouse": "CDE_WH",  # optional
   "database": "CANLIN",  # optional
#    "schema": "<your snowflake schema>",  # optional
 }  
session = Session.builder.configs(connection_parameters).create()



tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["Labeling Interface", "Full Training Dataset", "Labeling Stats", "Label Options", "How does Machine Learning Work at Canlin?", "Change Dates"])
col1, col2 = tab1.columns(2)

@st.cache_data()
def get_area_list(_session):
    areas = f"""SELECT DISTINCT AREA FROM CANLIN.HARMONIZED.WELLS_HISTORIAN_1H """
    areas = session.sql(areas).collect()
    return areas


selected_area = st.sidebar.selectbox("Select an Area", get_area_list(session))



@st.cache_data()
def get_uwi_list(_session, selected_area=None):
    if selected_area is None:
        uwis = f"""SELECT DISTINCT UWI FROM CANLIN.HARMONIZED.WELLS_HISTORIAN_1H"""
    else:
        uwis = f"""SELECT DISTINCT UWI FROM CANLIN.HARMONIZED.WELLS_HISTORIAN_1H WHERE AREA = '{selected_area}'"""
    uwis = session.sql(uwis).collect()
    return uwis

# @st.cache_data()
def get_labels_data(_session, selected_uwi):
    
    #  Create table if it does not exist
    if not table_exists(session, schema='LABELS', name='OPERATIONS_WELL'):
        create_table(session)
    
    sf_labels_uwi = f"""SELECT UWI, TIMESTAMP, LABEL FROM CANLIN.LABELS.OPERATIONS_WELL WHERE UWI = '{selected_uwi}' ORDER BY TIMESTAMP asc """
    sf_labels_uwi_df = session.sql(sf_labels_uwi).to_pandas()
    
    if sf_labels_uwi_df.empty:
        # select minimum of the timestamp column for the selected well
        min_date = session.table("CANLIN.HARMONIZED.WELLS_HISTORIAN_1H").select("TIMESTAMP").filter(F.col("UWI") == selected_uwi).agg(F.min(F.col("TIMESTAMP"))).collect()[0][0]
        # add a single row 
        session.sql(f"""INSERT INTO CANLIN.LABELS.OPERATIONS_WELL (UWI, TIMESTAMP, LABEL) VALUES ('{selected_uwi}', '{min_date}', 'Normal Operation')""").collect()
        sf_labels_uwi = f"""SELECT UWI, TIMESTAMP, LABEL FROM CANLIN.LABELS.OPERATIONS_WELL WHERE UWI = '{selected_uwi}' ORDER BY TIMESTAMP asc """
        sf_labels_uwi_df = session.sql(sf_labels_uwi).to_pandas()
    
    return sf_labels_uwi_df

# @st.cache_data()
def get_historian_data(_session, selected_uwi, labels, date_min=None, date_max=None):
    historian_query = f"""SELECT * FROM CANLIN.HARMONIZED.WELLS_HISTORIAN_1H WHERE UWI = '{selected_uwi}' ORDER BY UWI, TIMESTAMP asc """
    historian = session.sql(historian_query)
    
    sf_labels_uwi_query = f"""SELECT UWI, TIMESTAMP, LABEL FROM CANLIN.LABELS.OPERATIONS_WELL WHERE UWI = '{selected_uwi}' ORDER BY TIMESTAMP asc """
    sf_labels_uwi = session.sql(sf_labels_uwi_query)
    
    anomaly_score_query = f"""SELECT UWI, TIMESTAMP, ANOMALY_SCORE FROM CANLIN.PREDICTIONS.WELLS_ANOMALY_SCORE WHERE UWI = '{selected_uwi}' ORDER BY TIMESTAMP asc """
    anomaly_score = session.sql(anomaly_score_query)
    
    df = historian.join(sf_labels_uwi, on=["UWI", "TIMESTAMP"], how="left", rsuffix='_l') \
                    .join(anomaly_score, on=["UWI", "TIMESTAMP"], how="left", rsuffix='_a') \
                    .to_pandas()
    df = df.fillna(method='ffill')
    if date_min is not None and date_max is not None:
        df = df[(df['TIMESTAMP'] >= date_min) & (df['TIMESTAMP'] <= date_max)]
    
    return df

selected_uwi = st.sidebar.selectbox("Select a UWI", get_uwi_list(session, selected_area))

labels = get_labels_data(session, selected_uwi)

date_min = pd.to_datetime(tab6.date_input("Date Range - Start", datetime.date(2022, 1, 1)))
date_max = pd.to_datetime(tab6.date_input("Date Range - End",  datetime.date.today()))

historian = get_historian_data(session, selected_uwi, labels, date_min, date_max)

# st.sidebar.button("Refresh the charts", on_click=get_historian_data(session, selected_uwi, labels))

label_options = [{'Normal Operation': ['Normal Operation']},
                {'Hydrate': ['PRE- Hydrate Forming', 'DT- Downhole Hydrate', 'DT- Pipeline Hydrate', 'DT- Facility Hydrate']},
                {'Liquid Loading': ['PRE- Well Loading Up', 'DT- Well Loaded', 'DT- Shut-in for Buildup']},
                {'Downhole Issues': ['PRE- Wax or Scale Buildup', 'DT- Wax or Scale', 'PRE- Mechanical Wear', 'DT- Mechanical Failure']},
                {'Artificial Lift': ['Pump Failure']},
                {'Downstream': ['Downstream Impact']},
                {'Unpredictable': ['Power Outage', 'Coms Outage', 'ESD Closure']},
                ]

# create a label:category map from the values:keys above. include all values in the list of each label_options entity
label_category_map =  {}
for label_option in label_options:
    for key, value in label_option.items():
        for v in value:
            label_category_map[v] = key         
st.write(label_category_map)

# combine the lists present in the values of each label_options entity
label_options_list = [item for sublist in [list(x.values())[0] for x in label_options] for item in sublist]

label_colors = {'Normal Operation': 'lightgray',
                'PRE- Hydrate Forming': 'red',
                'DT- Downhole Hydrate': 'red',
                'DT- Pipeline Hydrate': 'red',
                'DT- Facility Hydrate': 'red',
                'PRE- Well Loading Up': 'orange',
                'DT- Well Loaded': 'orange',
                'DT- Shut-in for Buildup': 'orange',
                'PRE- Wax/Scale Buildup': 'blue',
                'DT- Wax/Scale': 'blue',
                'PRE- Mechanical Wear': 'green',
                'DT- Mechanical Failure': 'green',
                'Pump Failure': 'purple',
                'Downstream Impact': 'brown',
                'Power Outage': 'black',
                'Coms Outage': 'black',
                'ESD Closure': 'black',
                
                
                }

label_dashes = {'Normal Operation': 'solid',
                'PRE- Hydrate Forming': 'dot',
                'DT- Downhole Hydrate': 'solid',
                'DT- Pipeline Hydrate': 'solid',
                'DT- Facility Hydrate': 'solid',
                'PRE- Well Loading Up': 'dot',
                'DT- Well Loaded': 'solid',
                'DT- Shut-in for Buildup': 'solid',
                'PRE- Wax/Scale Buildup': 'dot',
                'DT- Wax/Scale': 'solid',
                'PRE- Mechanical Wear': 'dot',
                'DT- Mechanical Failure': 'solid',
                'Pump Failure': 'dash',
                'Downstream Impact': 'dash',
                'Power Outage': 'dash',
                'Coms Outage': 'dash',
                'ESD Closure': 'dash',
                
                }

tab4.write(label_options)


def delete_all_well_labels():
    session.sql(f"""DELETE FROM CANLIN.LABELS.OPERATIONS_WELL WHERE UWI = '{selected_uwi}' """).collect()
    labels = get_labels_data(session, selected_uwi)
    historian = get_historian_data(session, selected_uwi, labels, date_min, date_max)
    
# function to convert string to snake_case
def snake_case(string):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', string)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower().replace(' ', '')
    # trainset can only have labels up to 16 characters
    if len(s2) > 16:
        s2 = s2[:16]
    return s2

label_options_list_snake_case = [snake_case(x) for x in label_options_list]
label_options_list_snake_case_map = dict(zip(label_options_list_snake_case, label_options_list))

# st.write(label_options_list_snake_case_map)
# function to reverse snake_case to original 
def reverse_snake_case(string):
    return label_options_list_snake_case_map[string]

def generate_csv_for_trainset():
    
    # session.use_schema('TRAINING_SETS')
        
    # # Create select unique UWI's
    # wells = session.table("GAS_WELL_OPERATIONS").select(F.col("UWI")).distinct().collect()
    
    # # for each unique UWI, filter the table and output a csv file
    # for index, well in enumerate(wells):
    #     well = well[0]
    #     well_data = session.table("GAS_WELL_OPERATIONS").filter(F.col("UWI") == well)
        
    #     team = well_data.select(F.col("TEAM")).distinct().collect()[0][0]
    #     area = well_data.select(F.col("AREA")).distinct().collect()[0][0]
    #     print(f"well: {well}, team: {team}, area: {area}")
        
    #     # to pandas df
    #     well_data = well_data.to_pandas()
    
    temp_df = historian[['UWI', 'TIMESTAMP', 'LABEL', 'GAS_FLOW', 'GAS_PRESS', 'GAS_TEMP', 'GAS_DP', 'ANOMALY_SCORE']]
    
    # apply snake_case to LABEL column
    temp_df['LABEL'] = temp_df['LABEL'].apply(snake_case)
        
    # pivot all columns except uwi_id and event_timestamp into a column named 'series'
    temp_df = temp_df.melt(id_vars=['UWI','TIMESTAMP', 'LABEL'], var_name='series', value_name='value')
    
    temp_df.drop(columns=['UWI'], inplace=True)
    
    
    
    # set value column as float type, remove nan values
    temp_df['value'] = temp_df['value'].astype(float)
    temp_df = temp_df.dropna(subset=['value'])
    
    # Resample values to 6 hour chunks, partition by series and keep label as is and take mean of value
    # temp_df = temp_df.groupby(['series', pd.Grouper(key='TIMESTAMP', freq='6H')]).agg({'value': 'mean', 'label': 'first'}).reset_index()
    
    temp_df.rename(columns={'TIMESTAMP': 'timestamp', 'LABEL': 'label'}, inplace=True)
    
    # some days do not exist in the dataset, so we need to fill in the missing days
    # first get all the dates
    # dates = pd.date_range(start=temp_df['timestamp'].min(), end=temp_df['timestamp'].max(), freq='6H')
    # create a dataframe with all the dates
    # temp_df2 = pd.DataFrame({'timestamp': dates, 'value': np.nan, 'series': 'GAS_FLOW'})
    
    # # add a new row for each of the possible labels, with timestamps set as the max current timestamp + 1 day
    # # we do this just to get the labels pre-added so the operators don't have to manually add them each time in Trainset
    # for label in label_options_list:
    #     temp_df2 = pd.concat([temp_df2, pd.DataFrame({'timestamp': [temp_df2['timestamp'].max() + pd.Timedelta(hours=2)], 'value': [np.nan], 'series': ['GAS_FLOW'], 'label': [label]})], axis=0)
        

    # concat the two dataframes
    # temp_df = pd.concat([temp_df, temp_df2], axis=0)
    
    
    # where duplicate timestamps exist, keep the first one
    temp_df.drop_duplicates(subset=['timestamp', 'series'], keep='first', inplace=True)
    
    print(temp_df)
    temp_df.sort_values(['series', 'timestamp'], inplace=True)
    
    # fill in the missing values
    temp_df['value'] = temp_df['value'].fillna(method="ffill", limit=2).fillna(0)
    
    temp_df['timestamp'] = temp_df['timestamp'].dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    
    temp_df = temp_df[['series','timestamp','value', 'label']]
    
    # sort series so that gas_flow is first
    temp_df['series'] = pd.Categorical(temp_df['series'], ['GAS_FLOW', 'GAS_PRESS', 'GAS_TEMP', 'GAS_DP'])
    temp_df.sort_values(['series', 'timestamp'], inplace=True)
    
    return temp_df.to_csv(index=False)
   



with col1.expander("Use Trainset to Label", expanded=True):
    st.subheader("http://10.21.122.29:5000/labeler")
    
    st.download_button('Download File for Trainset', generate_csv_for_trainset(), f'{selected_uwi}_labels.csv', 'text/csv')
    st.caption('Copy-paste and use these Labels exactly:')

    # build a dataframe of the labels with columns: category, label, copy_for_trainset
    label_df = pd.DataFrame({
        'category': [label_category_map[label]  for label in label_options_list],
        'label': label_options_list,
        'copy_for_trainset': [snake_case(label) for label in label_options_list]
        })
    st.table(label_df)
    

# ### SINGLE LABEL ADDITION
# # form to add a new row to labels
# with col1.expander("Add a New Label", expanded=True):
#     with st.form("my_form"):
#         d = st.date_input(
#         "What date did the well change to this new label?",
#         datetime.date(2022, 1, 1))
#         t = st.time_input('What time (on the hour)', datetime.time(9, 0), step=3600)
        
#         label = st.selectbox("Select a label", label_options_list)

#         submitted = st.form_submit_button("Add Label", type='primary')
#         if submitted:
#             session.sql(f"""INSERT INTO CANLIN.LABELS.OPERATIONS_WELL (UWI, TIMESTAMP, LABEL) VALUES ('{selected_uwi}', '{datetime.datetime.combine(d, t)}', '{label}')""").collect()
#             labels = get_labels_data(session, selected_uwi)
#             historian = get_historian_data(session, selected_uwi, labels, date_min, date_max)
#             st.success('Label Saved', icon="✅")



with col1.expander("Upload a CSV File of Labels to replace current Labels", expanded=False):
    # Download current labels as csv
    # st.download_button('Download Current Labels for this Well', labels.to_csv(), f'{selected_uwi}_labels.csv', 'text/csv')
    st.warning("Uploading and submitting a csv file will delete all existing labels for this well")
    
    # Upload a CSV of Labels
    dataframe = None
    uploaded_file = st.file_uploader("Choose a CSV file", accept_multiple_files=False)
    if uploaded_file is not None:
        uwi_from_file_name = uploaded_file.name[0:16]
        dataframe = pd.read_csv(uploaded_file, sep=',', header=0)
        
        # long to wide format, carry through label column
        dataframe = dataframe.pivot(index=['timestamp', 'label'], columns='series', values='value').reset_index()
        print(dataframe.head())
        # rename columns
        dataframe.rename(columns={'timestamp': 'TIMESTAMP', 'label': 'LABEL'}, inplace=True)
        
        dataframe['UWI'] = uwi_from_file_name
        
        # convert timestamp column to datetime in YYYY-MM-DD HH:MM format
        dataframe['TIMESTAMP'] =  pd.to_datetime(dataframe['TIMESTAMP'], format='%Y-%m-%d %H:%M')
        
        dataframe = dataframe[['UWI', 'TIMESTAMP', 'LABEL']]
        
        # apply label_options_list_snake_case_map to LABEL column
        dataframe['LABEL'] = dataframe['LABEL'].map(label_options_list_snake_case_map)
        
        dataframe.drop_duplicates(subset=['UWI', 'TIMESTAMP'], keep='first', inplace=True)
        
        # drop empty rows
        dataframe = dataframe.dropna()
        # convert TIMESTAMP column to datetime
        # if UWI column in dataframe does not include ONLY the selected_uwi, throw error
        if not dataframe['UWI'].unique()[0] == selected_uwi:
            st.error("The UWI column in the uploaded file does not match the selected UWI")
        if not set(dataframe['LABEL'].unique()).issubset(set(label_options_list)):
            st.error("Some of the provided labels are not valid labels. Refer to the Label Options Tab.")
        st.write(dataframe)
        
        
    file_error = False
    if dataframe is None or dataframe['UWI'].unique()[0] != selected_uwi or not set(dataframe['LABEL'].unique()).issubset(set(label_options_list)):
        file_error = True
        
        
    def submit_uploaded_file():
        delete_all_well_labels()
        session.use_schema("LABELS")
        session.write_pandas(dataframe, table_name='OPERATIONS_WELL', database='CANLIN', schema='LABELS', overwrite=False)
        
        # for index, row in dataframe.iterrows():
        #     session.sql(f"""INSERT INTO CANLIN.LABELS.OPERATIONS_WELL (UWI, TIMESTAMP, LABEL) VALUES ('{selected_uwi}', '{row['TIMESTAMP']}', '{row['LABEL']}')""").collect()
        # session.sql(f"""INSERT INTO CANLIN.LABELS.OPERATIONS_WELL (UWI, TIMESTAMP, LABEL) VALUES ('{selected_uwi}', '{datetime.datetime.combine(d, t)}', '{label}')""").collect()
        labels = get_labels_data(session, selected_uwi)
        historian = get_historian_data(session, selected_uwi, labels, date_min, date_max)
        st.success('Labels Uploaded & Overwritten in Database', icon="✅")
    
    st.button("Submit & Overwrite Labels", on_click=submit_uploaded_file, disabled=file_error, type='primary')
    # with st.form("upload_form"):
        
    #     submitted = st.form_submit_button("Submit & Overwrite Labels", disabled=file_error)
    #     if submitted:
    #         delete_all_well_labels()
    #         dataframe.iterrows().apply(lambda x:
    #             session.sql(f"""INSERT INTO CANLIN.LABELS.OPERATIONS_WELL (UWI, TIMESTAMP, LABEL) VALUES ('{selected_uwi}', '{x['TIMESTAMP']}', '{x['LABEL']}')""").collect(), axis=0)
    #         # session.sql(f"""INSERT INTO CANLIN.LABELS.OPERATIONS_WELL (UWI, TIMESTAMP, LABEL) VALUES ('{selected_uwi}', '{datetime.datetime.combine(d, t)}', '{label}')""").collect()
    #         labels = get_labels_data(session, selected_uwi)
    #         historian = get_historian_data(session, selected_uwi, labels, date_min, date_max)
    #         st.experimental_rerun()
    #         st.success('Label Saved', icon="✅")

# with col1.expander("Delete an Existing Label"):
#     with st.form("delete_form"):
#         deletable = labels.apply(lambda x: f"{x['TIMESTAMP']} - {x['LABEL']}", axis=1)
#         label_to_delete = st.selectbox("Select a label to delete", deletable)
#         submitted = st.form_submit_button("Delete this label", type='primary')
        
#         if submitted:
#             session.sql(f"""DELETE FROM CANLIN.LABELS.OPERATIONS_WELL WHERE UWI = '{selected_uwi}' AND TIMESTAMP = '{label_to_delete.split(' - ')[0]}'""").collect()
#             labels = get_labels_data(session, selected_uwi)
#             historian = get_historian_data(session, selected_uwi, labels, date_min, date_max)
#             st.success('Label Deleted', icon="✅")


with col1.expander("Delete ALL Existing Labels for this Well and Start Over", expanded=False):
    st.warning("This will delete all existing labels for this well. Please confirm.")
    st.button(f'Delete All Labels for {selected_area}: {selected_uwi}', on_click=delete_all_well_labels, type='primary')
    
col2.dataframe(labels, use_container_width=True)


tab2.write(historian)
    

# create a list of all values in the label_options list, and flatten into a single list
label_options = [item for sublist in label_options for item in sublist.values()]
label_options = [item for sublist in label_options for item in sublist] 

### LABEL HISTOGRAM CHART ###

# if any labels in WELL_LABELS are not in label_options, print an error
bar_fig = px.bar(historian['LABEL'].value_counts(), y="LABEL", height=300, text_auto='.2s', 
                 color_discrete_map=label_colors, 
                 title='Label Counts')
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
    yaxis=dict(
        showgrid=False,
        zeroline=False,
        showline=False,
        showticklabels=False,
        title=None,
    ),
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

tab3.plotly_chart(bar_fig, use_container_width=True)

if not historian['LABEL'].isin(label_options).all():
    st.error("Some labels in the data editor are not in the label options. Please edit your changes before saving.")
    st.sidebar.error("Label Errors")

### GAS FLOW & LABELS CHART ###

fig = go.Figure(layout={'height': 500})

for label in historian['LABEL'].unique():
    
    label_df = historian.copy()
    
    label_df['GAS_FLOW'] = label_df['GAS_FLOW'].where((label_df['LABEL'] == label) , None)
    
    fig.add_trace(go.Scatter(
        x=label_df['TIMESTAMP'],
        y=label_df['GAS_FLOW'],
        name=label,
        line=dict(color=label_colors[label] if label in label_colors else 'red', width=1, dash=label_dashes[label] if label in label_dashes else 'red'),
    ))
    
    
fig.update_layout(
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
    ),

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

tab1.plotly_chart(fig, use_container_width=True)

well_labels_filled_melt = historian[['TIMESTAMP', 'GAS_DP', 'GAS_PRESS', 'GAS_TEMP', 'ANOMALY_SCORE']].melt(id_vars=['TIMESTAMP'], var_name='TAG', value_name='VALUE').sort_values(by=['TIMESTAMP'])
fig2 = px.line(well_labels_filled_melt, x="TIMESTAMP", y="VALUE", facet_row="TAG", color="TAG", height=1200)
fig2.update_layout(
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
    ),
    autosize=True,
    margin=dict(
        autoexpand=False,
        l=100,
        r=20,
        t=110,
    ),
    plot_bgcolor='white'
)
fig2.update_yaxes(        showgrid=False,
        zeroline=False,
        showline=False,
        showticklabels=False,
        matches=None)
tab1.plotly_chart(fig2, use_container_width=True)











### DATA FLOW


import streamlit as st
import graphviz

# st.set_page_config(layout="wide")
tab5.header('Choose a process to display')

workflows =  [
    'IROC ML', 
            #   'Legend',
            #   'Databases',
            #   'IROC L1',
            #   'Well Risk',
            #   'All'
              ]

process_display = tab5.selectbox('Process', workflows)
tab5.header(f'Canlin Advanced Analytics - Data Flow DAG - {process_display}')


graph = graphviz.Digraph('overview', format='svg')
# graph.attr(concentrate='true')
graph.attr(ranksep='0.2')
graph.attr(rankdir='LR')
# if process_display in ['IROC ML', 'Well Risk']:
#     graph.attr(rankdir='TB')

# {'black': RGB 65 65 65, 'yellow': RGB 234 214 55, 'red': RGB 236 34 39, 'darkblue': RGB 6 90 130, 'lightblue': RGB 91 192 235, 'blue': RGB 56 143 182, 'orange': RGB 243 167 18, 'brown': RGB 97 33 15, 'green': RGB 120 196 68, 'purple': RGB 149 79 114,'grey': RGB 184 184 188}
canlin_colors = {'black': '#414141', 'yellow': '#ead637', 'red': '#ec2227', 'darkblue': '#065a82', 'lightblue': '#5bc0eb', 'blue': '#388fb6', 'orange': '#f3a712', 'brown': '#61210f', 'green': '#78c444', 'purple': '#954f72', 'grey': '#b8b8bc'}

def create_node(name: str, label: str, valid_filter_list: list=workflows, subgraph: graphviz.Graph=None, **kwargs):
    if process_display in valid_filter_list:
        if subgraph:
            return subgraph.node(name=name, label=label, **kwargs)
        else:
            return graph.node(name=name, label=label, **kwargs)
    else:
        return None
    
def create_edge(from_node: str, to_node: str, valid_filter_list: list=workflows, subgraph: graphviz.Graph=None, **kwargs):
    if process_display in valid_filter_list:
        if subgraph:
            return subgraph.edge(from_node, to_node, **kwargs)
        else:
            return graph.edge(from_node, to_node, **kwargs)
    else:
        return None
    

### EDGES ###

edge_kwargs = {'color': canlin_colors['black'], 'style': 'dashed'}
# create_edge(from_node='noco', to_node='Outage Tracker Input', valid_filter_list=['All', 'IROC ML'], **edge_kwargs)

## DIRECT CONNECTIONS
direct_edge_kwargs = {'color': canlin_colors['lightblue'], 'style': 'dashed'}
# create_edge(from_node='noco', to_node='Outage Tracker Input', valid_filter_list=['All', 'Outage Tracker'], **direct_edge_kwargs)
# create_edge(from_node='noco', to_node='wpr-input', valid_filter_list=['All', 'Weekly Production Report'], **direct_edge_kwargs)
# create_edge(from_node='noco', to_node='ignition-well-tag-mapping', valid_filter_list=['All', 'IROC L1', 'IROC ML'], **direct_edge_kwargs)
# create_edge(from_node='noco', to_node='well-risk-manual', valid_filter_list=['All', 'Well Risk'], **direct_edge_kwargs)

# create_edge(from_node='labeling', to_node='well-risk-labels', valid_filter_list=['All', 'Well Risk'], **direct_edge_kwargs)

## SQL SCRIPTS
sql_edge_kwargs = {'color': canlin_colors['red'], 'style': 'dashed'}
# create_edge('datagps', 'downtime-sp', valid_filter_list=['All', 'Weekly Production Report'], **sql_edge_kwargs , tooltip='RPT_WEEKLY_DOWNTIME_REPORT_ALLOCATED_VOLS_5 (DATAGPS) SP', href='http://xcaltableau01/#/datasources/37908?:origin=card_share_link', target='_blank')
# create_edge('avocet', 'datagps', valid_filter_list=['All', 'Weekly Production Report'], **sql_edge_kwargs, tooltip='RPT_WEEKLY_DOWNTIME_REPORT_ALLOCATED_VOLS_5 (DATAGPS) SP')

## PYTHON SCRIPTS
python_edge_kwargs = {'color': canlin_colors['red'],  'style': 'dashed', 'penwidth': '1.5'}
# create_edge('ignition', 'HARMONIZED.WELLS_HISTORIAN', valid_filter_list=['All', 'IROC L1'], **python_edge_kwargs)
# create_edge('ignition', 'ignition-well-365d', valid_filter_list=['All', 'IROC L1'], **python_edge_kwargs)
# create_edge('ignition-well-tag-mapping', 'HARMONIZED.WELLS_HISTORIAN', valid_filter_list=['All', 'IROC L1'], **python_edge_kwargs)
# create_edge('ignition-well-tag-mapping', 'ignition-well-365d', valid_filter_list=['All', 'IROC L1', 'IROC ML'], **python_edge_kwargs)
# create_edge('ignition-well-tag-mapping', 'ignition-soft-tags', valid_filter_list=['All', 'IROC L1', 'IROC ML'], **python_edge_kwargs)
# create_edge('ignition', 'ignition-tags', valid_filter_list=['All', 'IROC L1'], **python_edge_kwargs)


## TABLEAU FLOWS
flow_edge_kwargs = {'color': canlin_colors['darkblue'], 'style': 'dashed', 'penwidth': '1.5'}
create_edge('valnav-budget', '2023-budget', valid_filter_list=['All', 'Weekly Production Report'], **flow_edge_kwargs, tooltip='Tableau Prep Flow - Economic Detail Flow - BUDGET_2023')
create_edge('avocet', 'avocet-daily', valid_filter_list=['All', 'Weekly Production Report'], **flow_edge_kwargs, tooltip='Tableau Prep Flow - Avocet Datasets', href='http://xcaltableau01/#/flows/76?:origin=card_share_link', target='_blank')
create_edge('avocet', 'avocet-daily-detail', valid_filter_list=['All', 'IROC L1', 'Well Risk'], **flow_edge_kwargs, tooltip='Tableau Prep Flow - Avocet Datasets Detail', href='http://xcaltableau01/#/flows/199?:origin=card_share_link', target='_blank')
create_edge('qbyte', 'lease-ops', valid_filter_list=['All', 'Lease Ops', 'Weekly Production Report', 'IROC L1'], **flow_edge_kwargs, tooltip='Tableau Prep Flow - Qbyte Lease Op Datasets', href='http://xcaltableau01/#/flows/13?:origin=card_share_link', target='_blank')

create_edge('avocet', 'corpwell', valid_filter_list=['All', 'Corporate Well List'], **flow_edge_kwargs, tooltip='Tableau Prep Flow - Corporate Well Index', href='http://xcaltableau01/#/flows/37?:origin=card_share_link', target='_blank')
create_edge('landman', 'corpwell', valid_filter_list=['All', 'Corporate Well List'], **flow_edge_kwargs, tooltip='Tableau Prep Flow - Corporate Well Index', href='http://xcaltableau01/#/flows/37?:origin=card_share_link', target='_blank')
create_edge('qbyte', 'corpwell', valid_filter_list=['All', 'Corporate Well List'], **flow_edge_kwargs, tooltip='Tableau Prep Flow - Corporate Well Index', href='http://xcaltableau01/#/flows/37?:origin=card_share_link', target='_blank')
create_edge('gdc', 'corpwell', valid_filter_list=['All', 'Corporate Well List', 'Well Risk'], **flow_edge_kwargs, tooltip='Tableau Prep Flow - Corporate Well Index', href='http://xcaltableau01/#/flows/37?:origin=card_share_link', target='_blank')
create_edge('valnav-budget', 'corpwell', valid_filter_list=['All', 'Corporate Well List'], **flow_edge_kwargs, tooltip='Tableau Prep Flow - Corporate Well Index', href='http://xcaltableau01/#/flows/37?:origin=card_share_link', target='_blank')
create_edge('valnav-reserves', 'corpwell', valid_filter_list=['All', 'Corporate Well List'], **flow_edge_kwargs, tooltip='Tableau Prep Flow - Corporate Well Index', href='http://xcaltableau01/#/flows/37?:origin=card_share_link', target='_blank')

create_edge('avocet-daily', 'weekly-production-report', valid_filter_list=['All', 'Weekly Production Report'], **flow_edge_kwargs, tooltip='Tableau Prep Flow - Weekly Production Report Datasets - Noco', href='http://xcaltableau01/#/flows/210?:origin=card_share_link', target='_blank')
create_edge('wpr-input', 'weekly-production-report', valid_filter_list=['All', 'Weekly Production Report'], **flow_edge_kwargs, tooltip='Tableau Prep Flow - Weekly Production Report Datasets - Noco', href='http://xcaltableau01/#/flows/210?:origin=card_share_link', target='_blank')
create_edge('lease-ops', 'weekly-production-report', valid_filter_list=['All', 'Weekly Production Report'], **flow_edge_kwargs, tooltip='Tableau Prep Flow - Weekly Production Report Datasets - Noco', href='http://xcaltableau01/#/flows/210?:origin=card_share_link', target='_blank')
create_edge('2023-budget', 'weekly-production-report', valid_filter_list=['All', 'Weekly Production Report'], **flow_edge_kwargs, tooltip='Tableau Prep Flow - Weekly Production Report Datasets - Noco', href='http://xcaltableau01/#/flows/210?:origin=card_share_link', target='_blank')
create_edge('weekly-production-report', 'weekly-production-report-dashboards', valid_filter_list=['All', 'Weekly Production Report'], **flow_edge_kwargs, tooltip='Tableau Prep Flow - Weekly Production Report Datasets - Noco', href='http://xcaltableau01/#/flows/210?:origin=card_share_link', target='_blank')

# create_edge('ignition', 'ignition-soft-tags', valid_filter_list=['All', 'IROC L1', 'IROC ML'], **flow_edge_kwargs)
# create_edge('ignition-soft-tags', 'ignition-well-statuses', valid_filter_list=['All', 'IROC L1', 'IROC ML'], **flow_edge_kwargs)
# create_edge('lease-ops', 'simple-financial', valid_filter_list=['All', 'IROC L1', 'IROC ML'], **flow_edge_kwargs)
# create_edge('simple-financial', 'uwi-metrics', valid_filter_list=['All', 'IROC L1', 'IROC ML'], **flow_edge_kwargs)
# create_edge('avocet-daily-detail', 'uwi-metrics', valid_filter_list=['All', 'IROC L1'], **flow_edge_kwargs, tooltip='UWI Metrics', href='http://xcaltableau01/#/flows/217?:origin=card_share_link', target='_blank')
# create_edge('uwi-metrics', 'ignition-well-statuses', valid_filter_list=['All', 'IROC L1', 'IROC ML'], **flow_edge_kwargs)




## TABLEAU SERVER - TABLEAU SERVER
tableau_edge_kwargs = {'color': canlin_colors['black'], 'style': 'dashed'}
create_edge('lease-ops', 'lease-ops-dashboards', valid_filter_list=['All', 'Lease Ops'], **tableau_edge_kwargs)
create_edge('corpwell', 'corporate-well-dashboards', valid_filter_list=['All', 'Corporate Well List'], **tableau_edge_kwargs)
create_edge('downtime-sp', 'weekly-production-report-dashboards', valid_filter_list=['All', 'Weekly Production Report'], **tableau_edge_kwargs, tooltip='Tableau Prep Flow - Weekly Production Report Datasets - Noco', href='http://xcaltableau01/#/flows/210?:origin=card_share_link', target='_blank')
# create_edge('HARMONIZED.WELLS_HISTORIAN', 'iroc-l1-dashboards', valid_filter_list=['All', 'IROC L1'], **tableau_edge_kwargs)
create_edge('ANALYTICS.IROC_WELL_STATUSES', 'iroc-l1-dashboards', valid_filter_list=['All', 'IROC L1'], **tableau_edge_kwargs)
create_edge('avocet-daily-detail', 'well-needle-report', valid_filter_list=['All', 'IROC L1'], **tableau_edge_kwargs)
create_edge('avocet-daily-detail', 'wpr-helper', valid_filter_list=['All', 'IROC L1'], **tableau_edge_kwargs)


# ## FEAST
feast_edge_kwargs = {'color': canlin_colors['purple'], 'style': 'dashed', 'penwidth': '1.5'}
# create_edge('avocet-daily-detail', 'feast', valid_filter_list=['All', 'IROC ML', 'Well Risk'], **feast_edge_kwargs)
# create_edge('ignition-well-365d', 'feast', valid_filter_list=['All', 'IROC ML'], **feast_edge_kwargs)
# create_edge('corpwell', 'feast', valid_filter_list=['All', 'IROC ML', 'Well Risk'], **feast_edge_kwargs)
# create_edge('ignition-well-statuses', 'feast', valid_filter_list=['All', 'IROC ML'], **feast_edge_kwargs)

# create_edge('feast', 'anomaly-detection-dataset', valid_filter_list=['All', 'IROC ML'], **feast_edge_kwargs)
create_edge('HARMONIZED.WELLS_HISTORIAN_1H', 'anomaly-detection-train', valid_filter_list=['All', 'IROC ML'], **feast_edge_kwargs)
create_edge('anomaly-detection-train', 'anomaly-detection-model', valid_filter_list=['All', 'IROC ML'], **feast_edge_kwargs)
create_edge('anomaly-detection-model', 'anomaly-detection-dashboards', valid_filter_list=['All', 'IROC ML'], **feast_edge_kwargs)
create_edge('anomaly-detection-dashboards', 'anomaly-detection-model', valid_filter_list=['All', 'IROC ML'], **feast_edge_kwargs)
# create_edge('HARMONIZED.WELLS_HISTORIAN_1H', 'anomaly-detection-dashboards', valid_filter_list=['All', 'IROC ML'], **feast_edge_kwargs)

create_edge('TRAINING_SETS.OPERATIONS_WELLS', 'operational-events-train', valid_filter_list=['All', 'IROC ML'], **feast_edge_kwargs)
create_edge('operational-events-train', 'operational-events-model', valid_filter_list=['All', 'IROC ML'], **feast_edge_kwargs)
create_edge('operational-events-model', 'operational-events-dashboards', valid_filter_list=['All', 'IROC ML'], **feast_edge_kwargs)
create_edge('operational-events-dashboards', 'operational-events-model', valid_filter_list=['All', 'IROC ML'], **feast_edge_kwargs)

create_edge('TRAINING_SETS.OPERATIONS_COMPRESSORS', 'comp-v-p-failure-train', valid_filter_list=['All', 'IROC ML'], **feast_edge_kwargs)
create_edge('comp-v-p-failure-train', 'comp-v-p-failure-model', valid_filter_list=['All', 'IROC ML'], **feast_edge_kwargs)
create_edge('comp-v-p-failure-model', 'comp-v-p-failure-dashboards', valid_filter_list=['All', 'IROC ML'], **feast_edge_kwargs)
create_edge('comp-v-p-failure-dashboards', 'comp-v-p-failure-model', valid_filter_list=['All', 'IROC ML'], **feast_edge_kwargs)
# create_edge('operational-events-dataset', 'operational-events-dashboards', valid_filter_list=['All', 'IROC ML'], **feast_edge_kwargs)

# create_edge('well-risk-manual', 'feast', valid_filter_list=['All', 'Well Risk'], **feast_edge_kwargs)
# create_edge('well-risk-labels', 'feast', valid_filter_list=['All', 'Well Risk'], **feast_edge_kwargs)
# create_edge('feast', 'well-risk-dataset', valid_filter_list=['All', 'Well Risk'], **feast_edge_kwargs)
# create_edge('well-risk-dataset', 'well-risk-train', valid_filter_list=['All', 'Well Risk'], **feast_edge_kwargs)
# create_edge('well-risk-train', 'well-risk-model', valid_filter_list=['All', 'Well Risk'], **feast_edge_kwargs)
# create_edge('well-risk-model', 'well-risk-dashboards', valid_filter_list=['All', 'Well Risk'], **feast_edge_kwargs)
# create_edge('well-risk-dashboards', 'well-risk-model', valid_filter_list=['All', 'Well Risk'], **feast_edge_kwargs)
# create_edge('well-risk-dataset', 'well-risk-dashboards', valid_filter_list=['All', 'Well Risk'], **feast_edge_kwargs)



### NODES ###

db_kwargs = {'shape': 'cylinder', 'color': 'black', 'style': 'filled', 'fillcolor': canlin_colors['grey']}
with graph.subgraph(name='cluster_databases') as db:
    db.attr(color='black', label='Raw Databases', style='dotted')
    create_node(name='avocet', label='Avocet FDC', valid_filter_list=['All', 'Weekly Production Report', 'Corporate Well List', 'Databases', 'IROC L1', 'Well Risk'], subgraph=db, **db_kwargs)
    create_node(name='qbyte', label='Qbyte', valid_filter_list=['All', 'Weekly Production Report', 'Corporate Well List', 'Lease Ops', 'Databases', 'IROC L1'], subgraph=db, **db_kwargs)
    create_node(name='gdc', label='GDC (Public Data)', valid_filter_list=['All', 'Corporate Well List', 'Databases', 'Well Risk'], subgraph=db, **db_kwargs)
    create_node(name='ignition', label='Ignition', valid_filter_list=['All', 'Databases', 'IROC L1', 'IROC ML'], subgraph=db, **db_kwargs)
    # create_node(name='oplynx', label='OpLynx', valid_filter_list=['All', 'Databases'], subgraph=db, **db_kwargs)
    # create_node(name='landman', label='Landman', valid_filter_list=['All', 'Corporate Well List', 'Databases'], subgraph=db, **db_kwargs)
    # create_node(name='valnav-budget', label='Valnav Budget', valid_filter_list=['All', 'Weekly Production Report', 'Corporate Well List', 'Databases'], subgraph=db, **db_kwargs)
    # create_node(name='valnav-reserves', label='Valnav Reserves', valid_filter_list=['All', 'Corporate Well List', 'Databases'], subgraph=db, **db_kwargs)
    # create_node(name='wellview', label='Wellview', valid_filter_list=['All', 'Well Risk', 'Databases'], subgraph=db, **db_kwargs)
    
with graph.subgraph(name='cluster_user_databases') as user_db:
    user_db.attr(color='black', label='User Input', style='dotted')
    create_node(name='noco', label='NocoDB', valid_filter_list=['All', 'Weekly Production Report', 'Databases', 'IROC L1', 'IROC ML', 'Well Risk'], subgraph=user_db, **db_kwargs)
    create_node(name='labeling', label='Labeling Application (Streamlit)', valid_filter_list=['All', 'Weekly Production Report', 'Databases', 'IROC ML', 'Well Risk'], subgraph=user_db, **db_kwargs)

    
create_node(name='datagps', label='DataGPS', valid_filter_list=['All', 'Weekly Production Report', 'Databases'], **db_kwargs)
    
# with graph.subgraph(name='cluster_bronze_datasets') as bronze:
#     bronze.attr(color='black', label='Bronze Datasets (Tableau)', style='dotted')
    


raw_kwargs = {'shape': 'note', 'fontcolor': canlin_colors['black'], 'color': 'black'}
bronze_kwargs = {'shape': 'note', 'fontcolor': canlin_colors['brown'], 'color': canlin_colors['brown']}
silver_kwargs = {'shape': 'note', 'fontcolor': canlin_colors['grey'], 'color': canlin_colors['grey']}
gold_kwargs = {'shape': 'note', 'fontcolor': canlin_colors['orange'], 'color': canlin_colors['orange']}
with graph.subgraph(name='cluster_data_engineering') as data:
    data.attr(color='black', label='Data Engineering (Snowflake)', style='dotted')
    
    create_node(name='lease-ops', label='Lease Ops Data, Group 15 (FLOW)', valid_filter_list=['All', 'Weekly Production Report', 'Lease Ops', 'IROC L1'], **bronze_kwargs, subgraph=data)
    create_node(name='corpwell', label='Corporate UWI List (Distinct)', valid_filter_list=['All', 'Weekly Production Report', 'Corporate Well List', 'Well Risk'], **bronze_kwargs, subgraph=data)
    create_node(name='avocet-daily', label='Avocet Daily Production', valid_filter_list=['All', 'Weekly Production Report',], **bronze_kwargs, subgraph=data)
    create_node(name='avocet-daily-detail', label='Avocet Daily Production by Well Detail', valid_filter_list=['All', 'IROC L1', 'Well Risk'], **bronze_kwargs, subgraph=data)
    create_node(name='wpr-input', label='WPR Input DB', valid_filter_list=['All', 'Weekly Production Report'], **bronze_kwargs, subgraph=data)
    create_node(name='2023-budget', label='CORP_BUDGET_2023V1 - Final', valid_filter_list=['All', 'Weekly Production Report'], **bronze_kwargs, subgraph=data)

    # create_node(name='ignition-well-tag-mapping', label='Ignition Well - Tag Mapping', valid_filter_list=['All', 'IROC L1', 'IROC ML'], **bronze_kwargs, subgraph=data)
    # create_node(name='well-risk-manual', label='Well Risk Manual Database', valid_filter_list=['All', 'Well Risk'], **bronze_kwargs, subgraph=data)
    # create_node(name='ignition-soft-tags', label='Ignition Soft Tags', valid_filter_list=['All', 'IROC L1', 'IROC ML'], **bronze_kwargs, subgraph=data)
    
    # create_node(name='downtime-sp', label=' Old Downtime Stored Procedure', valid_filter_list=['All', 'Weekly Production Report'], **silver_kwargs,subgraph=data, href='http://xcaltableau01/#/flows/210?:origin=card_share_link', target='_blank')
    # create_node(name='ignition-well-365d', label='Ignition - Well Data [365D][Daily]', valid_filter_list=['All', 'IROC L1', 'IROC ML'], **silver_kwargs,subgraph=data )
    
    
    create_node(name='RAW_IGNITION.SQLT_DATA_****_**', label='RAW_IGNITION.SQLT_DATA_****_**', valid_filter_list=['All', 'IROC ML'], **raw_kwargs,subgraph=data )
    create_node(name='RAW_IGNITION.SQLTH_TE', label='RAW_IGNITION.SQLTH_TE', valid_filter_list=['All', 'IROC ML'], **raw_kwargs,subgraph=data )
    create_node(name='RAW_IGNITION.WILDCAT_IGNITION_TAG_ATTRIBUTES', label='RAW_IGNITION.WILDCAT_IGNITION_TAG_ATTRIBUTES', valid_filter_list=['All', 'IROC ML'], **raw_kwargs,subgraph=data )
    create_node(name='RAW_IGNITION.SQLT_DATA_TABLEAU', label='RAW_IGNITION.SQLT_DATA_TABLEAU', valid_filter_list=['All', 'IROC ML'], **raw_kwargs,subgraph=data )
    
    create_node(name='RAW_NOCODB.WELLS', label='RAW_NOCODB.WELLS', valid_filter_list=['All', 'IROC ML'], **raw_kwargs,subgraph=data )
    create_node(name='RAW_NOCODB.COMPRESSORS', label='RAW_NOCODB.COMPRESSORS', valid_filter_list=['All', 'IROC ML'], **silver_kwargs,subgraph=data )
    
    create_node(name='HARMONIZED.WELLS_HISTORIAN', label='HARMONIZED.WELLS_HISTORIAN Dataset', valid_filter_list=['All', 'IROC ML'], **raw_kwargs,subgraph=data )
    create_node(name='HARMONIZED.WELLS_HISTORIAN_1H', label='HARMONIZED.WELLS_HISTORIAN_1H Dataset', valid_filter_list=['All', 'IROC ML'], **raw_kwargs,subgraph=data )
    # create_node(name='HARMONIZED.WELLS_HISTORIAN_MOST_RECENT', label='HARMONIZED.WELLS_HISTORIAN_MOST_RECENT Dataset', valid_filter_list=['All', 'IROC ML'], **raw_kwargs,subgraph=data )
    
    create_node(name='HARMONIZED.COMPRESSORS_HISTORIAN', label='HARMONIZED.COMPRESSORS_HISTORIAN Dataset', valid_filter_list=['All', 'IROC ML'], **silver_kwargs,subgraph=data )
    create_node(name='HARMONIZED.COMPRESSORS_HISTORIAN_1H', label='HARMONIZED.COMPRESSORS_HISTORIAN_1H Dataset', valid_filter_list=['All', 'IROC ML'], **silver_kwargs,subgraph=data )
    # create_node(name='HARMONIZED.COMPRESSORS_HISTORIAN_MOST_RECENT', label='HARMONIZED.COMPRESSORS_HISTORIAN_MOST_RECENT Dataset', valid_filter_list=['All', 'IROC ML'], **raw_kwargs,subgraph=data )
    
    create_node(name='ANALYTICS.IROC_WELL_STATUSES', label='ANALYTICS.IROC_WELL_STATUSES Dataset', valid_filter_list=['All', 'IROC L1'], **raw_kwargs,subgraph=data )
    
    create_node(name='LABELS.OPERATIONS_WELL', label='LABELS.OPERATIONS_WELLS', valid_filter_list=['All', 'IROC ML'], **silver_kwargs,subgraph=data )
    create_node(name='LABELS.OPERATIONS_COMPRESSOR', label='LABELS.OPERATIONS_COMPRESSORS', valid_filter_list=['All', 'IROC ML'], **silver_kwargs,subgraph=data )
    
    create_node(name='TRAINING_SETS.OPERATIONS_WELLS', label='TRAINING_SETS.OPERATIONS_WELLS', valid_filter_list=['All', 'IROC ML'], **raw_kwargs,subgraph=data )
    create_node(name='TRAINING_SETS.OPERATIONS_COMPRESSORS', label='TRAINING_SETS.OPERATIONS_COMPRESSORS', valid_filter_list=['All', 'IROC ML'], **silver_kwargs,subgraph=data )
    
    create_edge('ignition', 'RAW_IGNITION.SQLT_DATA_****_**', valid_filter_list=['All', 'IROC ML'], **flow_edge_kwargs)
    create_edge('ignition', 'RAW_IGNITION.SQLTH_TE', valid_filter_list=['All', 'IROC ML'], **flow_edge_kwargs)
    create_edge('ignition', 'RAW_IGNITION.WILDCAT_IGNITION_TAG_ATTRIBUTES', valid_filter_list=['All', 'IROC ML'], **flow_edge_kwargs)
    create_edge('ignition', 'RAW_IGNITION.SQLT_DATA_TABLEAU', valid_filter_list=['All', 'IROC ML'], **flow_edge_kwargs)
    create_edge('noco', 'RAW_NOCODB.WELLS', valid_filter_list=['All', 'IROC ML'], **flow_edge_kwargs)
    create_edge('noco', 'RAW_NOCODB.COMPRESSORS', valid_filter_list=['All', 'IROC ML'], **flow_edge_kwargs)
    
    create_edge('labeling', 'LABELS.OPERATIONS_WELL', valid_filter_list=['All', 'IROC ML'], **direct_edge_kwargs)
    create_edge('labeling', 'LABELS.OPERATIONS_COMPRESSOR', valid_filter_list=['All', 'IROC ML'], **direct_edge_kwargs)

    for i in ['RAW_IGNITION.SQLT_DATA_****_**', 'RAW_IGNITION.SQLTH_TE', 'RAW_IGNITION.WILDCAT_IGNITION_TAG_ATTRIBUTES', 'RAW_IGNITION.SQLT_DATA_TABLEAU', 'RAW_NOCODB.WELLS']:
         create_edge(i, 'HARMONIZED.WELLS_HISTORIAN', valid_filter_list=['All', 'IROC ML'], **python_edge_kwargs)
         
    for i in ['RAW_IGNITION.SQLT_DATA_****_**', 'RAW_IGNITION.SQLTH_TE', 'RAW_IGNITION.WILDCAT_IGNITION_TAG_ATTRIBUTES', 'RAW_IGNITION.SQLT_DATA_TABLEAU', 'RAW_NOCODB.COMPRESSORS']:
         create_edge(i, 'HARMONIZED.COMPRESSORS_HISTORIAN', valid_filter_list=['All', 'IROC ML'], **python_edge_kwargs)

    create_edge('HARMONIZED.WELLS_HISTORIAN', 'HARMONIZED.WELLS_HISTORIAN_1H', valid_filter_list=['All', 'IROC ML'], **python_edge_kwargs)
    create_edge('HARMONIZED.WELLS_HISTORIAN_1H', 'HARMONIZED.WELLS_HISTORIAN_MOST_RECENT', valid_filter_list=['All', 'IROC L1'], **sql_edge_kwargs)
    
    for i in ['HARMONIZED.WELLS_HISTORIAN_MOST_RECENT', 'HARMONIZED.AVOCET_WELL_STATS']:
         create_edge(i, 'ANALYTICS.IROC_WELL_STATUSES', valid_filter_list=['All', 'IROC L1'], **sql_edge_kwargs)
         
    for i in ['HARMONIZED.WELLS_HISTORIAN_1H', 'LABELS.OPERATIONS_WELL']:
         create_edge(i, 'TRAINING_SETS.OPERATIONS_WELLS', valid_filter_list=['All', 'IROC ML'], **sql_edge_kwargs)
         
    create_edge('HARMONIZED.COMPRESSORS_HISTORIAN', 'HARMONIZED.COMPRESSORS_HISTORIAN_1H', valid_filter_list=['All', 'IROC ML'], **python_edge_kwargs)
    create_edge('HARMONIZED.COMPRESSORS_HISTORIAN_1H', 'HARMONIZED.COMPRESSORS_HISTORIAN_MOST_RECENT', valid_filter_list=['All', 'IROC L1'], **sql_edge_kwargs)
    for i in ['HARMONIZED.COMPRESSORS_HISTORIAN_1H', 'LABELS.OPERATIONS_COMPRESSOR']:
         create_edge(i, 'TRAINING_SETS.OPERATIONS_COMPRESSORS', valid_filter_list=['All', 'IROC ML'], **sql_edge_kwargs)
    
    
    # create_node(name='simple-financial', label='Simple Financial Summary', valid_filter_list=['All', 'IROC L1', 'IROC ML'], **silver_kwargs,subgraph=data )
    # create_node(name='uwi-metrics', label='UWI Metrics', valid_filter_list=['All', 'IROC L1', 'IROC ML'], **silver_kwargs,subgraph=data )
    
    # create_node(name='weekly-production-report', label='Weekly Production Report', valid_filter_list=['All', 'Weekly Production Report'], **gold_kwargs, subgraph=data)
    # create_node(name='ignition-well-statuses', label='Ignition Well Statuses', valid_filter_list=['All', 'IROC L1', 'IROC ML'],  **gold_kwargs, subgraph=data)
    
    

# with graph.subgraph(name='cluster_gold_datasets') as gold:
#     gold.attr(color='black', label='Gold Datasets (Tableau)', style='dotted')

label_kwargs = {'shape': 'note', 'fontcolor': canlin_colors['green'], 'color': canlin_colors['green']}
with graph.subgraph(name='cluster_labels') as labels:
    labels.attr(color='black', label='Ground Truth Labels', style='dotted')
    create_node(name='well-risk-labels', label='Well Risk Labels', valid_filter_list=['All', 'Well Risk'], **label_kwargs, subgraph=labels)
    
feature_kwargs = {'shape': 'note', 'fontcolor': canlin_colors['purple'], 'color': canlin_colors['purple']}
# with graph.subgraph(name='cluster_feature_datasets') as features:
#     features.attr(color='black', label='Feature Datasets', style='dotted')
#     create_node(name='anomaly-detection-dataset', label='Anomaly Detection Dataset', valid_filter_list=['All', 'IROC ML'], subgraph=features, **feature_kwargs)
#     create_node(name='operational-events-dataset', label='Operational Events Dataset', valid_filter_list=['All', 'IROC ML'], subgraph=features, **feature_kwargs)
#     create_node(name='well-risk-dataset', label='Well Risk Dataset', valid_filter_list=['All', 'Well Risk'], subgraph=features, **feature_kwargs)


### WPR Process
# if process_display == 'All' or process_display == 'Weekly Production Report':
#     with graph.subgraph(name='cluster_wpr_process') as c:
#         c.attr(color='black', label='WPR Process', style='dashed')



    # create_node(name='operational-events-dataset', label='Operational Events Dataset', valid_filter_list=['All', 'Weekly Production Report', 'IROC ML'], subgraph=node_legend, **feature_kwargs)


## Feature Store / Data Transformation
fs_kwargs = {'style': 'filled', 'fontcolor': 'white', 'shape': 'doubleoctagon', 'fillcolor': canlin_colors['purple'] }
# create_node(name='feast', label='FEAST Feature Store', valid_filter_list=['All', 'IROC ML', 'Well Risk'], **fs_kwargs)



## Model Training
train_kwargs = {'style': 'filled', 'fontcolor': 'white', 'shape': 'box', 'fillcolor': canlin_colors['blue'] }
with graph.subgraph(name='cluster_train') as train:
    train.attr(color='black', label='Model Training / Evaluation (DataRobot)', style='dotted')
    create_node(name='anomaly-detection-train', label='Anomaly Detection Model Training', valid_filter_list=['All', 'IROC ML'], subgraph=train, **train_kwargs)
    create_node(name='operational-events-train', label='Operational Events Model Training', valid_filter_list=['All', 'IROC ML'], subgraph=train, **train_kwargs)
    create_node(name='comp-v-p-failure-train', label='Compressor Valve/Packing Failure Model Training', valid_filter_list=['All', 'IROC ML'], subgraph=train, **train_kwargs)
    create_node(name='well-risk-train', label='Well Risk Model Training', valid_filter_list=['All', 'Well Risk'], subgraph=train, **train_kwargs)


## Model Deployments / Inference
model_kwargs = {'fontcolor': 'black', 'shape': 'box3d', 'color': canlin_colors['green'] }
with graph.subgraph(name='cluster_model') as model:
    model.attr(color='black', label='Model Deployment (DataRobot)', style='dotted')
    create_node(name='anomaly-detection-model', label='Anomaly Detection Model', valid_filter_list=['All', 'IROC ML'], subgraph=model, **model_kwargs)
    create_node(name='operational-events-model', label='Operational Events Model', valid_filter_list=['All',  'IROC ML'], subgraph=model, **model_kwargs)
    create_node(name='comp-v-p-failure-model', label='Compressor Valve/Packing Failure Model', valid_filter_list=['All',  'IROC ML'], subgraph=model, **model_kwargs)
    create_node(name='well-risk-model', label='Well Risk Model', valid_filter_list=['All',  'Well Risk'], subgraph=model, **model_kwargs)



## Reference Dashboards
rd_kwargs = {'fontcolor': 'black', 'shape': 'component', 'color': canlin_colors['blue'] }
with graph.subgraph(name='cluster_reference_dashboards', node_attr={'fontcolor': 'black', 'shape': 'doubleoctagon', 'color': 'blue' }) as rd:
    rd.attr(color='black', label='Reference Dashboards', style='dotted')
    
    create_node(name='lease-ops-dashboards', label='Lease Ops Dashboards', valid_filter_list=['All', 'Lease Ops'], subgraph=rd, **rd_kwargs)
    create_node(name='weekly-production-report-dashboards', label='WPR Dashboards', valid_filter_list=['All', 'Weekly Production Report'], subgraph=rd, **rd_kwargs, href='http://xcaltableau01/#/workbooks/90?:origin=card_share_link', target='_blank')
    create_node(name='corporate-well-dashboards', label='Corpwell Dashboards', valid_filter_list=['All', 'Corporate Well List'], subgraph=rd, **rd_kwargs)
    
    create_node(name='well-needle-report', label='IROC Simple Well Needle Report', valid_filter_list=['All', 'IROC L1'], subgraph=rd, **rd_kwargs, href='http://xcaltableau01/#/workbooks/293?:origin=card_share_link', target='_blank')
    create_node(name='iroc-l1-dashboards', label='IROC L1 Dashboards', valid_filter_list=['All', 'IROC L1'], subgraph=rd, **rd_kwargs, href='http://xcaltableau01/#/workbooks/334?:origin=card_share_link', target='_blank')
    create_node(name='ignition-tags', label='Ignition Tags', valid_filter_list=['All', 'IROC L1'], subgraph=rd, **rd_kwargs, href='http://xcaltableau01/#/workbooks/319?:origin=card_share_link', target='_blank')
    create_node(name='wpr-helper', label='Weekly Production Report Forecast Helper', valid_filter_list=['All', 'IROC L1'], subgraph=rd, **rd_kwargs, href='http://xcaltableau01/#/workbooks/322?:origin=card_share_link', target='_blank')
    
    create_node(name='anomaly-detection-dashboards', label='Anomaly Detection', valid_filter_list=['All', 'IROC L1', 'IROC ML'], subgraph=rd, **rd_kwargs)
    create_node(name='operational-events-dashboards', label='Operational Events', valid_filter_list=['All', 'IROC L1', 'IROC ML'], subgraph=rd, **rd_kwargs)
    create_node(name='comp-v-p-failure-dashboards', label='Compressor Valve/Packing Failure', valid_filter_list=['All', 'IROC L1', 'IROC ML'], subgraph=rd, **rd_kwargs)
    create_node(name='well-risk-dashboards', label='Well Risk Ranking', valid_filter_list=['All', 'Well Risk'], subgraph=rd, **rd_kwargs)
    


with graph.subgraph(name='cluster_nodes_legend') as node_legend:
    node_legend.attr(color='black', label='Node Legend', style='dotted', rankdir='LR')
    create_node(name='data-source-legend', label='Raw Databases', **db_kwargs, valid_filter_list=['Legend'], subgraph=node_legend )
    create_node(name='bronze-legend', label='Bronze Level Datasets', **bronze_kwargs, valid_filter_list=['Legend'], subgraph=node_legend )
    create_node(name='silver-legend', label='Silver Level Datasets', **silver_kwargs, valid_filter_list=['Legend'], subgraph=node_legend )
    create_node(name='gold-legend', label='Gold Level Datasets', **gold_kwargs, valid_filter_list=['Legend'], subgraph=node_legend )
    create_node(name='fs-legend', label='Feature Store', **fs_kwargs, valid_filter_list=['Legend'], subgraph=node_legend )
    create_node(name='feature-legend', label='Feature Datasets', **feature_kwargs, valid_filter_list=['Legend'], subgraph=node_legend )
    create_node(name='train-legend', label='Training Process', **train_kwargs, valid_filter_list=['Legend'], subgraph=node_legend )
    create_node(name='model-legend', label='ML Model', **model_kwargs, valid_filter_list=['Legend'], subgraph=node_legend )
    create_node(name='dashboard-legend', label='Dashboards', **rd_kwargs, valid_filter_list=['Legend'], subgraph=node_legend )

with graph.subgraph(name='cluster_edges_legend') as edge_legend:
    edge_legend.attr(color='black', label='Edge Legend', style='dotted', rankdir='LR')
    create_node(name='legend-start', label='', shape='point', valid_filter_list=['Legend'])
    create_node(name='legend-end', label='', shape='point', valid_filter_list=['Legend'])
    create_edge('legend-start', 'legend-end', label='Direct Table Connection', **direct_edge_kwargs, valid_filter_list=['Legend'], subgraph=edge_legend, labeldistance='0.0' )
    create_edge('legend-start', 'legend-end', label='SQL Script', **sql_edge_kwargs, valid_filter_list=['Legend'], subgraph=edge_legend, labeldistance='0.0' )
    create_edge('legend-start', 'legend-end', label='Tableau Server Direct Connection', **tableau_edge_kwargs, valid_filter_list=['Legend'], subgraph=edge_legend, labeldistance='0.0' )
    create_edge('legend-start', 'legend-end', label='Python Script', **python_edge_kwargs, valid_filter_list=['Legend'], subgraph=edge_legend, labeldistance='0.0' )
    create_edge('legend-start', 'legend-end', label='FEAST Feature View / Service', **feast_edge_kwargs, valid_filter_list=['Legend'], subgraph=edge_legend, labeldistance='0.0' )
    create_edge('legend-start', 'legend-end', label='Tableau Prep Flow', **flow_edge_kwargs, valid_filter_list=['Legend'], subgraph=edge_legend, labeldistance='0.0')


    
# graph.edge('NocoDB', 'Outage Tracker Input')
# graph.edge('NocoDB', 'Network Structure (to/from)')
# graph.edge('Outage Tracker Input', 'Streamlit App')
# graph.edge('Network Structure (to/from)', 'Streamlit App')
# graph.edge('Avocet Production', 'FEAST Feature View')

# graph.edge('Streamlit App', 'Network Overview')
# graph.edge('Streamlit App', 'Outages Dashboard')
# graph.edge('FEAST Feature View', 'FEAST Saved Dataset', 'manual refresh in Streamlit App - eventually use Airflow', color='blue')
# graph.edge('FEAST Saved Dataset', 'Streamlit App')


tab5.graphviz_chart(graph)