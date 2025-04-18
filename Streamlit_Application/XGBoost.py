import os
import streamlit as st
from google.cloud import bigquery
import pandas as pd
import numpy as np
import datetime
import filepath

# BigQuery SQL query for prediction
def make_prediction(inputs):

    query = f"""
        SELECT
        *
        FROM
            ML.PREDICT(MODEL `is-data-engineering-project.mlmodel.VThree_XGBoost_model`,
                (SELECT
                    DATE(CAST({inputs['year']} AS INT64), CAST({inputs['month']} AS INT64), 1) AS combined_date,
                    CAST({inputs['floor_area_sqm']} AS FLOAT64) AS floor_area_sqm,
                    CAST('{inputs['flat_type']}' AS STRING) AS flat_type,
                    CAST({inputs['Price_Index_lagged']} AS FLOAT64) AS Price_index_lagged,
                    CAST({inputs['lease_commence_date']} AS INT64) AS lease_commence_date,
                    CAST('{inputs['storey_range']}' AS STRING) AS storey_range,
                    CAST({inputs['nearest_gep_school_distance_km']} AS FLOAT64) AS nearest_gep_school_distance_km,
                    CAST('{inputs['town']}' AS STRING) AS town,
                    CAST({inputs['remaining_lease_months']} AS INT64) AS remaining_lease_months,
                    CAST({inputs['nearest_mrt_exit_distance_km']} AS FLOAT64) AS nearest_mrt_exit_distance_km,
                    CAST('{inputs['flat_model']}' AS STRING) AS flat_model,
                    CAST({inputs['nearest_park_distance_km']} AS FLOAT64) AS nearest_park_distance_km,
                )
            );
    """

    # Run and fetch query
    result = client.query(query).to_dataframe()
    
    #get prediction
    prediction = result['predicted_resale_price'].iloc[0]
    
    return prediction


# BigQuery SQL query for categorical
def get_categorical_list(variable):

    query = f"""
        SELECT DISTINCT {variable}
        FROM `is-data-engineering-project.final_df_cleaned_Dataset.final_df_cleaned_transformed`
        ORDER BY {variable} ASC;
    """
    # Run and fetch query
    result = client.query(query)
    
    #get list
    var = [row[variable] for row in result]
    
    return var

# BigQuery SQL query for continuous
def get_continuous_range(variable):

    query = f"""
        SELECT 
        MIN({variable}) AS min_var,
        MAX({variable}) AS max_var
        FROM `is-data-engineering-project.final_df_cleaned_Dataset.final_df_cleaned_transformed`;
    """
    # Run and fetch query
    result = client.query(query)
    
    rows = list(result.result())

    # Extract the min and max values
    row = rows[0]
    min_value = row["min_var"]
    max_value = row["max_var"]
    
    return min_value, max_value


filepath = filepath.path

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = filepath + 'is-data-engineering-project-585a688f8987.json'


# Initialise BigQuery Client
client = bigquery.Client()


#streamlit header
st.markdown("<h1 style='font-size: 20px;'> Historical Resale Price Predictor (XGBoost Model)</h1>", unsafe_allow_html=True)

if 'town_list' not in st.session_state:
    # Load the lists and ranges if not already cached
    st.session_state.town_list_xgb = get_categorical_list('town')
    st.session_state.storey_list_xgb = get_categorical_list('storey_range')
    st.session_state.flat_type_list_xgb = get_categorical_list('flat_type')
    st.session_state.flat_model_list_xgb = get_categorical_list('flat_model')
    st.session_state.year_list_xgb = get_categorical_list('year')

    # Load the continuous ranges
    st.session_state.min_floor_area_sqm_xgb, st.session_state.max_floor_area_sqm_xgb = get_continuous_range('floor_area_sqm')
    st.session_state.min_lease_commence_date_xgb, st.session_state.max_lease_commence_date_xgb = get_continuous_range('lease_commence_date')
    st.session_state.min_remaining_lease_months_xgb, st.session_state.max_remaining_lease_months_xgb = get_continuous_range('remaining_lease_months')
    st.session_state.min_nearest_gep_school_distance_km_xgb, st.session_state.max_nearest_gep_school_distance_km_xgb = get_continuous_range('nearest_gep_school_distance_km')
    st.session_state.min_nearest_mrt_exit_distance_km_xgb, st.session_state.max_nearest_mrt_exit_distance_km_xgb = get_continuous_range('nearest_mrt_exit_distance_km')
    st.session_state.min_nearest_park_distance_km_xgb, st.session_state.max_nearest_park_distance_km_xgb = get_continuous_range('nearest_park_distance_km')
    st.session_state.min_Price_index_lagged_xgb, st.session_state.max_Price_index_lagged_xgb = get_continuous_range('Price_Index_Lagged')

# Access the cached values in session_state
town_list = st.session_state.town_list_xgb
storey_list = st.session_state.storey_list_xgb
flat_type_list = st.session_state.flat_type_list_xgb
flat_model_list = st.session_state.flat_model_list_xgb
year_list = st.session_state.year_list_xgb

min_floor_area_sqm = st.session_state.min_floor_area_sqm_xgb
max_floor_area_sqm = st.session_state.max_floor_area_sqm_xgb
min_lease_commence_date = st.session_state.min_lease_commence_date_xgb
max_lease_commence_date = st.session_state.max_lease_commence_date_xgb
min_remaining_lease_months = st.session_state.min_remaining_lease_months_xgb
max_remaining_lease_months = st.session_state.max_remaining_lease_months_xgb
min_nearest_gep_school_distance_km = st.session_state.min_nearest_gep_school_distance_km_xgb
max_nearest_gep_school_distance_km = st.session_state.max_nearest_gep_school_distance_km_xgb
min_nearest_mrt_exit_distance_km = st.session_state.min_nearest_mrt_exit_distance_km_xgb
max_nearest_mrt_exit_distance_km = st.session_state.max_nearest_mrt_exit_distance_km_xgb
min_nearest_park_distance_km = st.session_state.min_nearest_park_distance_km_xgb
max_nearest_park_distance_km = st.session_state.max_nearest_park_distance_km_xgb
min_Price_index_lagged = st.session_state.min_Price_index_lagged_xgb
max_Price_index_lagged = st.session_state.max_Price_index_lagged_xgb
                                                                                          


col1, col2, col3, col4 = st.columns(4)

#Input fields
with col1:
    year = st.selectbox('Year', options=year_list, index=0)
with col2:
    month = st.selectbox('Month', options=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], index=0)
with col3:
    town = st.selectbox('Town', town_list, index=18)
with col4:
    flat_type = st.selectbox('Flat Type', flat_type_list, index=0) #default is index 0 - '3 ROOM'

col5, col6, col7, col8 = st.columns(4)
with col5:
    storey_range = st.selectbox('Storey Range', storey_list, index=2)
with col6:
    floor_area_sqm = st.number_input('Floor Area (Sqm)', min_value=min_floor_area_sqm, max_value=max_floor_area_sqm, step=0.01, value=60.0)
with col7:
    lease_commence_date = st.number_input('Lease Commence Year', min_value=min_lease_commence_date, max_value=max_lease_commence_date, step=1, value=1970)
with col8:
    remaining_lease_months = st.number_input('Remaining Lease Months', min_value=min_remaining_lease_months, max_value=max_remaining_lease_months, step=1, value=630)

col9, col10, col11, col12 = st.columns(4)
with col9:
    flat_model = st.selectbox('Flat Model', flat_model_list, index=0)
with col10:
    nearest_gep_school_distance_km = st.number_input('Nearest GEP School (km)', min_value=min_nearest_gep_school_distance_km, \
                                                     max_value = max_nearest_gep_school_distance_km, step=0.01, value=3.3656398879975438)
with col11:
    nearest_mrt_exit_distance_km = st.number_input('Nearest MRT (km)', min_value=min_nearest_mrt_exit_distance_km, \
                                                   max_value=max_nearest_mrt_exit_distance_km, step=0.01, value=0.46787468945210753)
with col12:
    nearest_park_distance_km = st.number_input('Nearest Park (km)', min_value=min_nearest_park_distance_km, \
                                               max_value=max_nearest_park_distance_km, step=0.01, value=1.4402029396863)

Price_index_lagged = st.number_input('Price Index Lagged', min_value=min_Price_index_lagged, max_value=max_Price_index_lagged, step=0.01, value=134.6)


#If streamlit button press, do prediction
if st.button('Predict'):
    inputs = {
            'year': year,
            'month': month,
            'floor_area_sqm': floor_area_sqm,
            'flat_type': flat_type,
            'Price_Index_lagged': Price_index_lagged,
            'lease_commence_date': lease_commence_date,
            'storey_range': storey_range,
            'nearest_gep_school_distance_km': nearest_gep_school_distance_km,
            'town': town,
            'remaining_lease_months': remaining_lease_months,
            'nearest_mrt_exit_distance_km': nearest_mrt_exit_distance_km,
            'flat_model': flat_model,
            'nearest_park_distance_km': nearest_park_distance_km
            
    }

    prediction = make_prediction(inputs)
    st.write(f"The predicted resale price is: ${prediction:,.2f}")

