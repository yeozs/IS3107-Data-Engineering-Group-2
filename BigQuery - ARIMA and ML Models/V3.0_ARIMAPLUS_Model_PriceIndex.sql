-- Step 1: Create the ARIMA Plus Model on train set
CREATE OR REPLACE MODEL `is-data-engineering-project.mlmodel.VThree_arima_model_PriceIndex`
OPTIONS(model_type='ARIMA_PLUS', 
    time_series_timestamp_col='combined_date', 
    time_series_data_col='median_Price_Index_Lagged',
    data_frequency = 'MONTHLY') AS 
SELECT 
    combined_date,
    ANY_VALUE(median_Price_Index_Lagged) AS median_Price_Index_Lagged
FROM (
  SELECT 
    DATE(CONCAT(CAST(year AS STRING), '-', LPAD(CAST(month AS STRING), 2, '0'), '-01')) AS combined_date,
    PERCENTILE_CONT(Price_Index_Lagged, 0.5) OVER (PARTITION BY year, month) AS median_Price_Index_Lagged
  FROM 
    `is-data-engineering-project.final_df_cleaned_Dataset.final_df_cleaned_transformed`
)
GROUP BY combined_date
ORDER BY combined_date
LIMIT 95;


-- Step 2: Model Prediction
CREATE OR REPLACE TABLE `is-data-engineering-project.final_df_cleaned_Dataset.ARIMA_2Y_forecast_results`
AS
  SELECT *
  FROM ML.FORECAST(MODEL `is-data-engineering-project.mlmodel.VThree_arima_model_PriceIndex`, 
      STRUCT(0.9 AS confidence_level, 25 AS horizon)); #use '12 AS horizon,' for 1 year

-- Step 3: Evaluate Model on Test Set
SELECT *
FROM
  ML.EVALUATE(MODEL `is-data-engineering-project.mlmodel.VThree_arima_model_PriceIndex`,
    (
    SELECT 
    combined_date,
    ANY_VALUE(median_Price_Index_Lagged) AS median_Price_Index_Lagged
  FROM (
    SELECT
      DATE(CONCAT(CAST(year AS STRING), '-', LPAD(CAST(month AS STRING), 2, '0'), '-01')) AS combined_date,
      PERCENTILE_CONT(Price_Index_Lagged, 0.5) OVER (PARTITION BY year, month) AS median_Price_Index_Lagged
    FROM 
    `is-data-engineering-project.final_df_cleaned_Dataset.final_df_cleaned_transformed`
    )
    GROUP BY combined_date
    ORDER BY combined_date
    LIMIT 95 OFFSET 1
    ),
    STRUCT(0.9 AS confidence_level));

-- Step 4: Create new table for streamlit LinReg Processing
CREATE OR REPLACE TABLE `is-data-engineering-project.final_df_cleaned_Dataset.ARIMA_PriceIndexForecast_Streamlit` AS
  SELECT
      EXTRACT(YEAR FROM forecast_timestamp) AS forecast_year,
      EXTRACT(MONTH FROM forecast_timestamp) AS forecast_month,
      DATE(forecast_timestamp) AS combined_date,
      prediction_interval_lower_bound,
      forecast_value,
      prediction_interval_upper_bound,
      FROM `is-data-engineering-project.final_df_cleaned_Dataset.ARIMA_2Y_forecast_results`
      ORDER BY forecast_year, forecast_month;






