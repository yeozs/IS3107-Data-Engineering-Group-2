-- Step 0: Create Train Test Tables
CREATE OR REPLACE TABLE `is-data-engineering-project.final_df_cleaned_Dataset.dataSet` AS
    SELECT DATE(CONCAT(CAST(year AS STRING), '-', LPAD(CAST(month AS STRING), 2, '0'), '-01')) AS combined_date,
    *
    FROM `is-data-engineering-project.final_df_cleaned_Dataset.final_df_cleaned_transformed`
    ORDER BY combined_date;

CREATE OR REPLACE TABLE `is-data-engineering-project.final_df_cleaned_Dataset.trainSet` AS
    SELECT * 
    FROM `is-data-engineering-project.final_df_cleaned_Dataset.dataSet`
    WHERE RAND() <= 0.70  -- 70% for training
    ORDER BY combined_date;

CREATE OR REPLACE TABLE `is-data-engineering-project.final_df_cleaned_Dataset.testSet` AS
    SELECT * 
    FROM `is-data-engineering-project.final_df_cleaned_Dataset.dataSet`
    WHERE RAND() > 0.70   -- 30% for testing
    ORDER BY combined_date;

-- Step 1: Create XGBoost Model
CREATE OR REPLACE MODEL `is-data-engineering-project.mlmodel.VThree_XGBoost_model`
OPTIONS(model_type ='BOOSTED_TREE_REGRESSOR',
        input_label_cols = ['resale_price'],
        max_iterations=100,
        tree_method='auto')
AS SELECT
  combined_date,
  floor_area_sqm,
  flat_type,
  Price_Index_Lagged,
  lease_commence_date,
  storey_range,
  nearest_gep_school_distance_km,
  remaining_lease_months,
  nearest_mrt_exit_distance_km,
  flat_model,
  town,
  nearest_park_distance_km,
  resale_price
FROM `is-data-engineering-project.final_df_cleaned_Dataset.trainSet`;

-- Step 2: Evaluate Model on Test Set
SELECT *
FROM ML.EVALUATE(
  MODEL `is-data-engineering-project.mlmodel.VThree_XGBoost_model`,
  (SELECT * FROM `is-data-engineering-project.final_df_cleaned_Dataset.testSet`)
);

-- Step 3: Predict resale_price
SELECT *
FROM
    ML.PREDICT(MODEL `is-data-engineering-project.mlmodel.VThree_XGBoost_model`,
    (
    SELECT
        resale_price,
        combined_date,
        floor_area_sqm,
        flat_type,
        Price_Index_Lagged,
        lease_commence_date,
        storey_range,
        nearest_gep_school_distance_km,
        remaining_lease_months,
        nearest_mrt_exit_distance_km,
        flat_model,
        town,
        nearest_park_distance_km,
    FROM
        `is-data-engineering-project.final_df_cleaned_Dataset.testSet`
    ));

#-- Step 4: Delete Tables used for Train, Test split
DROP TABLE `is-data-engineering-project.final_df_cleaned_Dataset.dataSet`;
DROP TABLE `is-data-engineering-project.final_df_cleaned_Dataset.trainSet`;
DROP TABLE `is-data-engineering-project.final_df_cleaned_Dataset.testSet`;






