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
st.set_page_config(layout="wide", page_title="Well Risk Ranking", page_icon="🧮")

st.title("Corporate Well Risk Ranking: Labeling Tool")
st.write("This app is used to assign rankings for the Corporate Well Risk Ranking Model.")

def table_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    return exists

def create_table(session, schema, name):
    SHARED_COLUMNS= [
                        T.StructField("WELLBORE_ID", T.StringType()),
                        T.StructField("RISK_LABEL", T.StringType()),
                        T.StructField("COMMENT", T.StringType()),
                        T.StructField("SUBMITTED_BY", T.StringType()),
                    ]
    
    SCHEMA = T.StructType(SHARED_COLUMNS)

    table = session.create_dataframe([[None]*len(SCHEMA.names)], schema=SCHEMA) \
                        .na.drop() \
                        .write.mode('overwrite').save_as_table(f'{schema}.{name}')
                        
    return table


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

# selected_week = st.sidebar.date_input('Select Week (Monday through Sunday)', datetime.date.today() - datetime.timedelta(days=7))
# team_selection = st.sidebar.selectbox(
#     'Select Team',
#     ['WILDCAT', 'EAST', 'EDSON', 'PRA', 'CENTRAL']
# )

# truncate date to week start
# week_start = selected_week - datetime.timedelta(days=selected_week.weekday())
# week_end = week_start + datetime.timedelta(days=6)

# write and format dates as "Monday May 1, 2023" 
# st.subheader(f"Entering for {week_start.strftime('%A')} {week_start.strftime('%B')} {week_start.strftime('%d')}, {week_start.strftime('%Y')} through {week_end.strftime('%A')} {week_end.strftime('%B')} {week_end.strftime('%d')}, {week_end.strftime('%Y')}")

def get_labels_data(_session):
    
    #  Create table if it does not exist
    if not table_exists(session, schema='LABELS', name='WELL_RISK'):
        create_table(session, 'LABELS', 'WELL_RISK')
   
    well_risk_labels = session.table('LABELS.WELL_RISK') \
                    .to_pandas()
    
    return well_risk_labels

@st.cache_data()
def get_training_data(_session):
   
    df = session.table('CANLIN.TRAINING_SETS.CORPORATE_WELL_RISK') \
                       .to_pandas()
        
    return df

# @st.cache_data()
def get_predictions_data(_session):
   
    df = session.table('CANLIN.PREDICTIONS.CORPORATE_WELL_RISK') \
                       .to_pandas()
        
    return df

risk_mapping = {
    'VERY LOW': 0,
    'LOW': 1,
    'MEDIUM': 2,
    'HIGH': 3,
    'VERY HIGH': 4
}

unfiltered_df = get_training_data(session)
df = unfiltered_df.copy(deep=True)
labels = get_labels_data(session)
predictions = get_predictions_data(session)

topcol1,   v_low, low, med, high, v_high = st.columns([2,1,1,1,1,1])
# topcol1.metric(label=f"Wellbores to Label (50% of non EAST)", value=round(df[df['TEAM'] != 'EAST'].shape[0] * 0.5), delta=f'{labels.shape[0]} wellbores ranked so far')
pred_source = topcol1.radio("Show Predictions From:", ['ML Model', 'Simple Logic'], index=0, horizontal=True)



tab1, tab2, tab3, tab4 = st.tabs(["Assign Well Risk", "Full Dataset", "Understand the Numbers", "Simple Logic Parameters"])

col1, col2, col3 = tab1.columns(3)

col1.code("Choose a Wellbore")

# select a team
team_selection = col1.selectbox(
    'Select Team',
    ['WILDCAT', 'EAST', 'EDSON', 'PRA', 'CENTRAL'],
    0
)

# select an area
area_selection = col1.selectbox(
    'Select Area',
    df[df['TEAM'] == team_selection]['AREA'].unique(),
    5
)


def map_risk_label(x):
    if x == None:
        return np.random.normal(0, scale=1)
    else:
        return risk_mapping[x]


team_counts = df.groupby(['TEAM', 'AREA', 'RISK_LABEL']).count()['WELLBORE_ID']
# tab3.write(team_counts)


# checkbox to ignore previously labeled wells
ignore_labeled = col1.checkbox('Hide Previously Labeled Wells')

if ignore_labeled:
    # get list of wells from labels table
    labeled_wells = labels['WELLBORE_ID'].unique()
    # filter out labeled wells
    df = df[~df['WELLBORE_ID'].isin(labeled_wells)]

df =  df[(df['TEAM'] == team_selection) & (df['AREA'] == area_selection)]


def format_id(id):
    # convert string from 100110608211W6 to 100-11-06-082-11W6
    return re.sub(r'(\d{3})(\d{2})(\d{2})(\d{3})(\d{2})(\w{2})', r'\1-\2-\3-\4-\5\6', id)

# radio select a well
well_selection = col1.radio(
    'Select Wellbore',
    df['WELLBORE_ID'].unique(),
    format_func=format_id
)

col2.code("Primary Risk Factors")
im1, im2 = col2.columns(2)
im1.metric('Well Age',
           f"{round(df[df['WELLBORE_ID'] == well_selection]['WELLHEAD_AGE_YEARS'].values[0], 2)} years",
           f"{'-' if df[df['WELLBORE_ID'] == well_selection]['WELLHEAD_AGE_YEARS'].values[0] <= df['WELLHEAD_AGE_YEARS'].mean() else ''}Corporate Average: {round(df['WELLHEAD_AGE_YEARS'].mean(),1)} years",
            delta_color='inverse'
           )
im2.metric('H2S %',
           f"{round(df[df['WELLBORE_ID'] == well_selection]['H2S'].values[0]*100, 1)}%",
           f"Corporate Average: {round(unfiltered_df['H2S'].mean()*100,1)}%",
           delta_color='inverse'
           )
# im1.metric('Distance to Population',
#            f"{round(df[df['WELLBORE_ID'] == well_selection]['WELLHEAD_AGE_YEARS'].values[0], 2)} years",
#         #    f"Corporate Average: {round(df['WELLHEAD_AGE_YEARS'].mean(),1)} years"
#            )
# im2.metric('Distance to Environmental',
#            f"{round(df[df['WELLBORE_ID'] == well_selection]['WELLHEAD_AGE_YEARS'].values[0], 2)} years",
#         #    f"Corporate Average: {round(df['WELLHEAD_AGE_YEARS'].mean(),1)} years"
#            )
im1.metric('Produced in Last Year?', f"{'No' if df[df['WELLBORE_ID'] == well_selection]['GROSS_BOE_365D_AVG'].values[0] == 0 else 'Yes'}")
im2.metric('Is an Injector?', f"{'No' if df[df['WELLBORE_ID'] == well_selection]['IS_INJECTOR'].values[0] == 'Not Injector' else 'Yes'}")

col2.code("Other Well Data")
with col2.expander("Show Well Data"):
    identifier_columns = ['TEAM', 'MAJOR_AREA', 'AREA', 'WELLBORE_ID', 'PROVINCE', 'WELL_LICENSE_NUMBER', 'OPERATOR', 'HAS_CANLIN_INTERESTS' ]
    st.caption('Well Identifier Columns')
    st.json(df[df['WELLBORE_ID'] == well_selection][identifier_columns].to_dict(orient='records')[0])

    status_columns = ['WELLBORE_PRODUCING_STATUS', 'PROD_ZONE', 'DAYS_SINCE_GDC_LAST_PRODUCTION_DATE', ]
    st.caption('Status Columns')
    st.json(df[df['WELLBORE_ID'] == well_selection][status_columns].to_dict(orient='records')[0])

    uwi_columns = ['UWI_COUNT', 'PRODUCING_UWI_COUNT', 'UWIS_W_STATUS' ]
    st.caption('UWI Columns')
    st.json(df[df['WELLBORE_ID'] == well_selection][uwi_columns].to_dict(orient='records')[0])

    production_columns = [ col for col in df.columns if '365D_AVG' in col ]
    st.caption('Production Columns (Avocet)')
    st.json(df[df['WELLBORE_ID'] == well_selection][production_columns].to_dict(orient='records')[0])


    date_columns = ['SPUD_DATE', 'RIG_RELEASE_DATE', 'ON_PRODUCTION_DATE', ]
    st.caption('Date Columns')
    st.json(df[df['WELLBORE_ID'] == well_selection][date_columns].to_dict(orient='records')[0])

    eol_columns = ['RECLAIM_STATUS', 'SURFACE_ABANDON_DATE', 'SURFACE_ABANDON_TYPE', ]
    st.caption('End of Life Columns')
    st.json(df[df['WELLBORE_ID'] == well_selection][eol_columns].to_dict(orient='records')[0])

    wellbore_columns = ['PROFILE_TYPE', 'DEPTH_DATUM_ELEV', 'MAX_TVD', 'PLUGBACK_DEPTH']
    st.caption('Wellbore Columns')
    st.json(df[df['WELLBORE_ID'] == well_selection][wellbore_columns].to_dict(orient='records')[0])

    # df columns with AOF in name
    aof_columns = [ col for col in df.columns if 'AOF' in col ]
    st.caption('AOF Columns')
    st.json(df[df['WELLBORE_ID'] == well_selection][aof_columns].to_dict(orient='records')[0])

    # all other columns
    other_columns = [ col for col in df.columns if col not in identifier_columns + status_columns + uwi_columns + production_columns + date_columns + eol_columns + wellbore_columns + aof_columns ]
    st.caption('Other Columns')
    st.json(df[df['WELLBORE_ID'] == well_selection][other_columns].to_dict(orient='records')[0], expanded=False)

tab2.caption("FULL WELL RISK DATASET")
tab2.write(df)


# col2.json(df[df['WELLBORE_ID'] == well_selection].to_dict(orient='records')[0])
current_well_risk = labels[labels['WELLBORE_ID'] == well_selection]['RISK_LABEL'].values[0] if len(labels[labels['WELLBORE_ID'] == well_selection]) > 0 else None

model_predicted_well_risk = predictions[predictions['WELLBORE_ID'] == well_selection]['CLASS_1'].values[0] if len(predictions[predictions['WELLBORE_ID'] == well_selection]) > 0 else None
model_exp_1_name = predictions[predictions['WELLBORE_ID'] == well_selection]['CLASS_1_EXPLANATION_1_FEATURE_NAME'].values[0] if len(predictions[predictions['WELLBORE_ID'] == well_selection]) > 0 else None
model_exp_1_qualitative_strength = predictions[predictions['WELLBORE_ID'] == well_selection]['CLASS_1_EXPLANATION_1_QUALITATIVE_STRENGTH'].values[0] if len(predictions[predictions['WELLBORE_ID'] == well_selection]) > 0 else None
model_exp_1_value = predictions[predictions['WELLBORE_ID'] == well_selection]['CLASS_1_EXPLANATION_1_ACTUAL_VALUE'].values[0] if len(predictions[predictions['WELLBORE_ID'] == well_selection]) > 0 else None
model_exp_2_name = predictions[predictions['WELLBORE_ID'] == well_selection]['CLASS_1_EXPLANATION_2_FEATURE_NAME'].values[0] if len(predictions[predictions['WELLBORE_ID'] == well_selection]) > 0 else None
model_exp_2_qualitative_strength = predictions[predictions['WELLBORE_ID'] == well_selection]['CLASS_1_EXPLANATION_2_QUALITATIVE_STRENGTH'].values[0] if len(predictions[predictions['WELLBORE_ID'] == well_selection]) > 0 else None
model_exp_2_value = predictions[predictions['WELLBORE_ID'] == well_selection]['CLASS_1_EXPLANATION_2_ACTUAL_VALUE'].values[0] if len(predictions[predictions['WELLBORE_ID'] == well_selection]) > 0 else None
model_exp_3_name = predictions[predictions['WELLBORE_ID'] == well_selection]['CLASS_1_EXPLANATION_3_FEATURE_NAME'].values[0] if len(predictions[predictions['WELLBORE_ID'] == well_selection]) > 0 else None
model_exp_3_qualitative_strength = predictions[predictions['WELLBORE_ID'] == well_selection]['CLASS_1_EXPLANATION_3_QUALITATIVE_STRENGTH'].values[0] if len(predictions[predictions['WELLBORE_ID'] == well_selection]) > 0 else None
model_exp_3_value = predictions[predictions['WELLBORE_ID'] == well_selection]['CLASS_1_EXPLANATION_3_ACTUAL_VALUE'].values[0] if len(predictions[predictions['WELLBORE_ID'] == well_selection]) > 0 else None    
    

if labels.shape[0] <= 500:
    model_predicted_well_risk = 'NOT ENOUGH LABELS'


comparison = None if current_well_risk is None or model_predicted_well_risk == 'NOT ENOUGH LABELS' or risk_mapping[current_well_risk] == risk_mapping[model_predicted_well_risk]  else  f'- Current Model Predicts {model_predicted_well_risk} Risk' if risk_mapping[current_well_risk] > risk_mapping[model_predicted_well_risk] else f'+ Current Model Predicts {model_predicted_well_risk} Risk'
col3.code('Ground Truth Label')
col3.metric(f'CURRENT RISK RANKING FOR {format_id(well_selection)}:', current_well_risk, comparison, delta_color='inverse')

with col3.expander("Change Risk Ranking"):
    submitted_by = st.selectbox('Submitted By', [None, 'Marsha Peacock', 'Brian Keller', 'Tom Cook', 'Dave Patterson', 'Tim Adams', 'Paige Sloan', 'Matthew Nelson', 'Other'])
    with st.form("well_label_form"):
        # selectbox for risk ranking label 
        well_label_new = st.selectbox('Change/Submit Risk Ranking', ['VERY LOW', 'LOW', 'MEDIUM', 'HIGH', 'VERY HIGH'], index=0 if current_well_risk is None else ['VERY LOW', 'LOW', 'MEDIUM', 'HIGH', 'VERY HIGH'].index(current_well_risk))
        comment = st.text_input(label='Comment', value=labels[labels['WELLBORE_ID'] == well_selection]['COMMENT'].values[0] if len(labels[labels['WELLBORE_ID'] == well_selection]) > 0 else '')
        # well_label_new = st.text_input(label=team_selection, value=wpr_team_comments_wow[wpr_team_comments_wow['TEAM'] == team_selection]['COMMENT'].values[0] if team_selection in wpr_team_comments_wow['TEAM'].values else '')
        disabled = True if submitted_by is None else False
        if submitted_by is None:
            st.error('Please select your name from the dropdown above')
        submitted = st.form_submit_button(label='Save New Ranking', type='primary', disabled=disabled)

        if submitted:

            try: 
                # delete any existing comment
                session.sql(f"""DELETE FROM LABELS.WELL_RISK WHERE WELLBORE_ID = '{well_selection}'""").collect()
            finally:
                # insert new label
                session.sql(f"""INSERT INTO LABELS.WELL_RISK (WELLBORE_ID, RISK_LABEL, COMMENT, SUBMITTED_BY) VALUES ('{well_selection}', '{well_label_new}', '{comment}', '{submitted_by}')""").collect()
                
                st.write(f'New Risk Ranking Label for {well_selection} submitted')
                labels = get_labels_data(session)
                st.experimental_rerun()


ml_model_container = col3.container()


# col2.map(df.rename(columns={'SURFACE_LATITUDE': 'lat', 'SURFACE_LONGITUDE': 'lon'}), use_container_width=True)
map_container = tab3.container()




tab4.header("Simple Model Thresholds (Starting Point)")

th1, th2, th3, th4, th5 = tab4.columns(5)

h2s_threshold = th1.number_input('H2S % Threshold', min_value=0.0, value=1.0)
age_threshold = th2.number_input('Wellhead Age Threshold', min_value=0, value=40)
aof_rate_threshold = th3.number_input('AOF Flow Rate Threshold', min_value=0.0, value=200.0)
# use_is_injector = th4.checkbox('Use Injector Flag', value=True)
# pop_threshold = th3.number_input('Proximity to Population Threshold')
# env_threshold = th4.number_input('Proximity to Environmental Threshold')
# prod_threshold = th5.number_input('Days since last Production Threshold', min_value=35, value=365)


def rank_well(row):
    
    risky_count = 0
    
    if row['H2S'] > h2s_threshold/100:
        risky_count += 1
    
    if row['WELLHEAD_AGE_YEARS'] > age_threshold:
        risky_count += 1
        
    # if row['PROXIMITY_TO_POPULATION'] > pop_threshold:
    #     risky_count += 1
        
    # if row['PROXIMITY_TO_ENVIRONMENT'] > env_threshold:
    #     risky_count += 1
    
    if row['AOF_FLOW_RATE_E3M3D'] > aof_rate_threshold:
        risky_count += 1
    
    if row['IS_INJECTOR'] == 'Is Injector':
        risky_count += 1
        
    # didn't produce in last year
    if row['GROSS_BOE_365D_AVG'] == 0:
        risky_count += 1 
    
    # if row['DAYS_SINCE_GDC_LAST_PRODUCTION_DATE'] > prod_threshold:
    #     risky_count += 1
    
   
        
    if risky_count == 0:
        return 'VERY LOW'
    elif risky_count == 1:
        return 'LOW'
    elif risky_count == 2:
        return 'MEDIUM'
    elif risky_count == 3:
        return 'HIGH'
    elif risky_count >= 4:
        return 'VERY HIGH'
criteria_count = 4 
unfiltered_df['SIMPLE_MODEL_PREDICTION'] = unfiltered_df.apply(rank_well, axis=1)

# counts
tab4.subheader('Simple Model Prediction Counts')
counts = unfiltered_df['SIMPLE_MODEL_PREDICTION'].value_counts().reset_index().rename(columns={'index': 'SIMPLE_MODEL_PREDICTION', 'SIMPLE_MODEL_PREDICTION': 'COUNT'})
# tab4.write(counts)
t4c1,   t4_v_low, t4_low, t4_med, t4_high, t4_v_high = tab4.columns([2,1,1,1,1,1])


t4_v_low.metric(label=f'Very Low (0 of {criteria_count})', value=0 if counts[counts['SIMPLE_MODEL_PREDICTION'] == 'VERY LOW'].shape[0] == 0 else counts[counts['SIMPLE_MODEL_PREDICTION'] == 'VERY LOW']['COUNT'])
t4_low.metric(label=f'Low (1 of {criteria_count})', value=0 if counts[counts['SIMPLE_MODEL_PREDICTION'] == 'LOW'].shape[0] == 0 else counts[counts['SIMPLE_MODEL_PREDICTION'] == 'LOW']['COUNT'])
t4_med.metric(label=f'Medium (2 of {criteria_count})', value=0 if counts[counts['SIMPLE_MODEL_PREDICTION'] == 'MEDIUM'].shape[0] == 0 else counts[counts['SIMPLE_MODEL_PREDICTION'] == 'MEDIUM']['COUNT'])
t4_high.metric(label=f'High (3 of {criteria_count})', value=0 if counts[counts['SIMPLE_MODEL_PREDICTION'] == 'HIGH'].shape[0] == 0 else counts[counts['SIMPLE_MODEL_PREDICTION'] == 'HIGH']['COUNT'])
t4_v_high.metric(label=f'Very High (>=4)', value=0 if counts[counts['SIMPLE_MODEL_PREDICTION'] == 'VERY HIGH'].shape[0] == 0 else counts[counts['SIMPLE_MODEL_PREDICTION'] == 'VERY HIGH']['COUNT'])
tab4.write(unfiltered_df)


# Temp - submit button to save SIMPLE_MODEL_PREDICTION to database
def submit_simple_model_predictions():
    session.use_schema("LABELS")
    session.write_pandas(unfiltered_df[['WELLBORE_ID', 'SIMPLE_MODEL_PREDICTION']].rename(columns={'SIMPLE_MODEL_PREDICTION': 'RISK_LABEL'}), table_name='WELL_RISK_SIMPLE_MODEL_PREDICTION', database='CANLIN', schema='LABELS', overwrite=True, auto_create_table=True)
    
submit_simple_button = tab4.button('Submit Simple Model Predictions to Database', on_click=submit_simple_model_predictions)



import pydeck as pdk

# chart_data = pd.DataFrame(
#    np.random.randn(1000, 2) / [50, 50] + [37.76, -122.4],
#    columns=['lat', 'lon'])

chart_data = unfiltered_df[['WELLBORE_ID', 'SURFACE_LATITUDE', 'SURFACE_LONGITUDE', 'RISK_LABEL', 'TEAM', 'SIMPLE_MODEL_PREDICTION']].rename(columns={'SURFACE_LATITUDE': 'lat', 'SURFACE_LONGITUDE': 'lon'})
# map_container.write(chart_data[chart_data['lat'].isna()])
# drop missing lat/lon
chart_data = chart_data.dropna(subset=['lat', 'lon'])
# drop lat/lon if np.NaN
chart_data = chart_data[~chart_data['lat'].isna()]
chart_data = chart_data[~chart_data['lon'].isna()]
# apply risk mapping and create RISK_LABEL_VALUE  column
chart_data['RISK_LABEL_VALUE'] = chart_data['SIMPLE_MODEL_PREDICTION'].fillna('VERY LOW').apply(lambda x: risk_mapping[x])
# chart_data['coordinates'] = chart_data[['lon', 'lat']].values.tolist()
# chart_data = chart_data.rename(columns={'WELLBORE_ID': 'name', })
# chart_data = chart_data[['name', 'coordinates']]
# st.write(chart_data)
# chart_data = pd.DataFrame(
#    np.random.randn(1000, 2) / [50, 50] + [37.76, -122.4],
#    columns=['lat', 'lon'])

# st.write(chart_data)

# layer = pdk.Layer(
#     "ScatterplotLayer",
#     chart_data,
    
# )
layer = pdk.Layer(
            'ScatterplotLayer',
            data=chart_data,
            get_position='[lon, lat]',
            get_color='TEAM',
            get_radius=200,
        )
hexagon_layer = pdk.Layer(
           'HexagonLayer',
           data=chart_data,
           get_position='[lon, lat]',
           radius=5000,
           elevation_scale=4,
           elevation_range=[0, 1000],
           pickable=True,
           extruded=True,
        )
heatmap_layer = pdk.Layer(
            'HeatmapLayer',
            chart_data,
            radius=500,
            get_position='[lon, lat]',
            getWeight='RISK_LABEL_VALUE',
            aggregation='SUM',
)
    

view_state = pdk.ViewState(latitude=51.6, longitude=-113.15, zoom=4, bearing=0, pitch=5)
# view_state = pdk.ViewState(
#         latitude=37.76,
#         longitude=-122.4,
#         zoom=11,
#         pitch=50,
#     )

r = pdk.Deck(
    map_style=None,
    initial_view_state=view_state,
    layers=[
        # hexagon_layer,
        heatmap_layer,
        layer
    ],
)
map_container.pydeck_chart(r)

def prediction(risk):
    if pred_source == 'ML Model':
        if labels.shape[0] < 500:
            return '-'
        return predictions[predictions['CLASS_1'] == risk].shape[0]
    if pred_source == 'Simple Logic':
        return unfiltered_df[unfiltered_df['SIMPLE_MODEL_PREDICTION'] == risk].shape[0]

    # prediction = labels[labels['RISK_LABEL'] == risk].shape[0] if pred_source == 'ML Model' else unfiltered_df[unfiltered_df['SIMPLE_MODEL_PREDICTION'] == risk].shape[0]
    # label = labels[labels['RISK_LABEL'] == risk].shape[0]
    # print(prediction, label)
    # if prediction > label:
    #     return f' +{prediction - label}'
    # elif prediction < label:
    #     return f' -{label - prediction}'
    # else:
    #     return '' 


v_low.metric(label='Very Low', delta=f"{labels[labels['RISK_LABEL'] == 'VERY LOW'].shape[0]} Labeled", value=prediction('VERY LOW'), delta_color='off')
low.metric(label='Low', delta=f"{labels[labels['RISK_LABEL'] == 'LOW'].shape[0]} Labeled", value=prediction('LOW'), delta_color='off')
med.metric(label='Medium', delta=f"{labels[labels['RISK_LABEL'] == 'MEDIUM'].shape[0]} Labeled", value=prediction('MEDIUM'), delta_color='off')
high.metric(label='High', delta=f"{labels[labels['RISK_LABEL'] == 'HIGH'].shape[0]} Labeled", value=prediction('HIGH'), delta_color='off')
v_high.metric(label='Very High', delta=f"{labels[labels['RISK_LABEL'] == 'VERY HIGH'].shape[0]} Labeled", value=prediction('VERY HIGH'), delta_color='off')

if pred_source == 'ML Model':
    ml_model_container.subheader('ML Model Prediction')
    ml_model_container.caption("The Risk Ranking Machine Learning model is trained once a week on the above labels as assigned by Canlin Staff. Based on this model, a predicted Risk Rank can be generated based on the similarities between the current well and all other labeled wells in the model.")
    ml_model_container.code(f"Current Model Prediction = {model_predicted_well_risk}")
    if model_predicted_well_risk != 'NOT ENOUGH LABELS':
        ml_model_container.code(f"""
                            {model_exp_1_qualitative_strength} {model_exp_1_name}: {model_exp_1_value}
                            {model_exp_2_qualitative_strength} {model_exp_2_name}: {model_exp_2_value}
                            {model_exp_3_qualitative_strength} {model_exp_3_name}: {model_exp_3_value}
                            """)
    else:
        ml_model_container.error('Not enough labels to train the model and make a prediction. Please label more wells.')
    ml_model_container.caption("If there is a difference between what the model predicts, and what has been assigned as the Label by Canlin Staff, understand that the model will attempt to reconcile this difference the next time it is trained. If a difference still remains it means that the current well Label follows a materially different set of logic from the rest of Canlin. If this well is considered an outlier, this is not an issue.")



if pred_source == 'Simple Logic':
    ml_model_container.subheader('Simple Logic Assignment')
    ml_model_container.caption("The Risk Ranking Simple Logic model is based on the thresholds set by Canlin Staff for only a few parameters. You can adjust these parameters on the fly in the 'Simple Logic Parameters' tab.")
    ml_model_container.code(f"Simple Logic Assignment = {unfiltered_df[unfiltered_df['WELLBORE_ID'] == well_selection]['SIMPLE_MODEL_PREDICTION'].values[0]}")
    ml_model_container.code(f"""
                            {'+ ' if unfiltered_df[unfiltered_df['WELLBORE_ID'] == well_selection]['H2S'].values[0]*100 > h2s_threshold else ''}H2S Threshold ({h2s_threshold}%): {round(unfiltered_df[unfiltered_df['WELLBORE_ID'] == well_selection]['H2S'].values[0]*100, 1)}%
                            {'+ ' if unfiltered_df[unfiltered_df['WELLBORE_ID'] == well_selection]['WELLHEAD_AGE_YEARS'].values[0] > age_threshold else ''}Wellhead Age Threshold ({age_threshold} years): {round(unfiltered_df[unfiltered_df['WELLBORE_ID'] == well_selection]['WELLHEAD_AGE_YEARS'].values[0], 1)} years
                            {'+ ' if unfiltered_df[unfiltered_df['WELLBORE_ID'] == well_selection]['AOF_FLOW_RATE_E3M3D'].values[0] > aof_rate_threshold else ''}AOF Flow Rate Threshold ({aof_rate_threshold} E3M3/d): {round(unfiltered_df[unfiltered_df['WELLBORE_ID'] == well_selection]['AOF_FLOW_RATE_E3M3D'].values[0], 1)} E3M3/d
                            {'+ ' if unfiltered_df[unfiltered_df['WELLBORE_ID'] == well_selection]['IS_INJECTOR'].values[0] == 'Is Injector' else ''}Is an Injector?: {unfiltered_df[unfiltered_df['WELLBORE_ID'] == well_selection]['IS_INJECTOR'].values[0]}
                            {'+ ' if unfiltered_df[unfiltered_df['WELLBORE_ID'] == well_selection]['GROSS_BOE_365D_AVG'].values[0] == 0 else ''}Production in Prior Year: {round(unfiltered_df[unfiltered_df['WELLBORE_ID'] == well_selection]['GROSS_BOE_365D_AVG'].values[0], 1)} E3M3/d
                            """)
     
    #   if row['WELLHEAD_AGE_YEARS'] > age_threshold:
    #     risky_count += 1
        
    # # if row['PROXIMITY_TO_POPULATION'] > pop_threshold:
    # #     risky_count += 1
        
    # # if row['PROXIMITY_TO_ENVIRONMENT'] > env_threshold:
    # #     risky_count += 1
    
    # if row['AOF_FLOW_RATE_E3M3D'] > aof_rate_threshold:
    #     risky_count += 1
    
    # if use_is_injector and row['IS_INJECTOR'] == 'Is Injector':
    #     risky_count += 1
        
    # # didn't produce in last year
    # if row['GROSS_BOE_365D_AVG'] == 0:
    #     risky_count += 1 