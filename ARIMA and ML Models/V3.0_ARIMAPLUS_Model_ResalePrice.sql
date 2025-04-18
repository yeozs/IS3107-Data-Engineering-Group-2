-- Step 0: Create New Table for ARIMA
CREATE OR REPLACE TABLE `is-data-engineering-project.final_df_cleaned_Dataset.ARIMA_TownFlatDate` AS
WITH median_calc AS (
    SELECT 
        town,
        flat_type,
        year,
        month,
        DATE(CONCAT(CAST(year AS STRING), '-', LPAD(CAST(month AS STRING), 2, '0'), '-01')) AS combined_date,
        PERCENTILE_CONT(resale_price, 0.5) OVER (PARTITION BY town, flat_type, year, month) AS median_resale_price
    FROM `is-data-engineering-project.final_df_cleaned_Dataset.final_df_cleaned_transformed`
)
SELECT DISTINCT town, flat_type, combined_date, median_resale_price
FROM median_calc
ORDER BY town, flat_type, combined_date;

#-- Step 0: Create Train Set (Exclude Latest Date)
CREATE OR REPLACE TABLE `is-data-engineering-project.final_df_cleaned_Dataset.ARIMA_TownFlatDateTrain` AS
SELECT * 
FROM `is-data-engineering-project.final_df_cleaned_Dataset.ARIMA_TownFlatDate`
WHERE combined_date < (SELECT MAX(combined_date) FROM `is-data-engineering-project.final_df_cleaned_Dataset.ARIMA_TownFlatDate`)
ORDER BY town, flat_type, combined_date;

-- Step 0: Create Test Set (Only the Latest Date)
CREATE OR REPLACE TABLE `is-data-engineering-project.final_df_cleaned_Dataset.ARIMA_TownFlatDateTest` AS
SELECT * 
FROM `is-data-engineering-project.final_df_cleaned_Dataset.ARIMA_TownFlatDate`
WHERE combined_date = (SELECT MAX(combined_date) FROM `is-data-engineering-project.final_df_cleaned_Dataset.ARIMA_TownFlatDate`)
ORDER BY town, flat_type, combined_date;


-- Step 1: Train ARIMA Model
CREATE OR REPLACE MODEL `is-data-engineering-project.mlmodel.VThree_arima_model`
OPTIONS(
  MODEL_TYPE='ARIMA_PLUS',
  TIME_SERIES_TIMESTAMP_COL='combined_date',
  TIME_SERIES_DATA_COL='median_resale_price',
  TIME_SERIES_ID_COL=['town', 'flat_type'],  -- Trains separate models for town / flat_type
  AUTO_ARIMA=True
) AS
SELECT 
    combined_date,
    median_resale_price,
    town,
    flat_type
FROM `is-data-engineering-project.final_df_cleaned_Dataset.ARIMA_TownFlatDateTrain`
WHERE median_resale_price IS NOT NULL;

-- Step 2: Show Model Future Prediction
CREATE OR REPLACE TABLE `is-data-engineering-project.final_df_cleaned_Dataset.ARIMA_Forecast`
AS
  SELECT *
  FROM ML.FORECAST(MODEL `is-data-engineering-project.mlmodel.VThree_arima_model`, 
      STRUCT(0.95 AS confidence_level, 25 AS horizon)); #2 years + 1 month

-- Step 3: Evaluate Model on Test Set
SELECT
  *
FROM
  ML.EVALUATE(MODEL `is-data-engineering-project.mlmodel.VThree_arima_model`,
    (
    SELECT
      combined_date,
      median_resale_price,
      town,
      flat_type
    FROM
      `is-data-engineering-project.final_df_cleaned_Dataset.ARIMA_TownFlatDateTest`
    WHERE median_resale_price IS NOT NULL),
    STRUCT(0.95 AS confidence_level, 25 AS horizon));



------------- Create Duplicate Workaround Dashboard Tables Due Google Looker Filter Issues-------------
-- Step 4: Create Looker Filter Workaround Duplicate Table for Dashboard Line Chart Views
CREATE OR REPLACE TABLE `is-data-engineering-project.final_df_cleaned_Dataset.ARIMA_Table`
AS
  SELECT 
      town,
      town AS town_chart,
      flat_type,
      flat_type AS flat_type_chart,
      DATE(forecast_timestamp) AS chart_date,
      forecast_value AS median_resale_price,
      standard_error,
      confidence_level,
      prediction_interval_lower_bound,
      prediction_interval_upper_bound,
      confidence_interval_lower_bound,
      confidence_interval_upper_bound,
  FROM `is-data-engineering-project.final_df_cleaned_Dataset.ARIMA_Forecast`
  ORDER BY town, flat_type, chart_date;

-- Step 4: Create Looker Filter Workaround Duplicate Table for Dashboard Line Map Views
CREATE OR REPLACE TABLE `is-data-engineering-project.final_df_cleaned_Dataset.ARIMA_Table_Maps`
AS
  SELECT 
      town,
      town AS town_map,
      flat_type,
      flat_type AS flat_type_map,
      DATE(forecast_timestamp) AS map_date,
      forecast_value AS median_resale_price,
      standard_error,
      confidence_level,
      prediction_interval_lower_bound,
      prediction_interval_upper_bound,
      confidence_interval_lower_bound,
      confidence_interval_upper_bound,
  FROM `is-data-engineering-project.final_df_cleaned_Dataset.ARIMA_Forecast`
  ORDER BY town, flat_type, map_date;


-- Step 5: Delete Tables used for Train, Test split and Model Forecast
DROP TABLE `is-data-engineering-project.final_df_cleaned_Dataset.ARIMA_TownFlatDate`;
DROP TABLE `is-data-engineering-project.final_df_cleaned_Dataset.ARIMA_TownFlatDateTrain`;
DROP TABLE `is-data-engineering-project.final_df_cleaned_Dataset.ARIMA_TownFlatDateTest`;
DROP TABLE `is-data-engineering-project.final_df_cleaned_Dataset.ARIMA_Forecast`;




