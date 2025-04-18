CREATE OR REPLACE TABLE `is-data-engineering-project.final_df_cleaned_Dataset.final_df_cleaned_transformed` AS
WITH flat_type_counts AS (
    -- Calculate frequency for flat_type
    SELECT 
        flat_type,
        COUNT(*) * 1.0 / SUM(COUNT(*)) OVER () AS flat_type_ratio
    FROM `is-data-engineering-project.final_df_cleaned_Dataset.final_df_cleaned_raw`
    GROUP BY flat_type
),
storey_range_counts AS (
    -- Calculate frequency for storey_range
    SELECT 
        storey_range,
        COUNT(*) * 1.0 / SUM(COUNT(*)) OVER () AS storey_range_ratio
    FROM `is-data-engineering-project.final_df_cleaned_Dataset.final_df_cleaned_raw`
    GROUP BY storey_range
),
flat_model_counts AS (
    -- Calculate frequency for flat_model
    SELECT 
        flat_model,
        COUNT(*) * 1.0 / SUM(COUNT(*)) OVER () AS flat_model_ratio
    FROM `is-data-engineering-project.final_df_cleaned_Dataset.final_df_cleaned_raw`
    GROUP BY flat_model
),
categorized_data AS (
    -- Join the original data with the computed frequencies and perform binning
    SELECT 
        d.*,
        -- Binning flat_type
        CASE 
            WHEN ft.flat_type_ratio > 0.05 THEN d.flat_type 
            ELSE 'Other' 
        END AS binned_flat_type,
        -- Binning storey_range
        CASE 
            WHEN sr.storey_range_ratio > 0.05 THEN d.storey_range 
            ELSE 'Other' 
        END AS binned_flat_range,
        -- Binning flat_model
        CASE 
            WHEN fm.flat_model_ratio > 0.05 THEN d.flat_model 
            ELSE 'Other' 
        END AS binned_flat_model
    FROM `is-data-engineering-project.final_df_cleaned_Dataset.final_df_cleaned_raw` d
    LEFT JOIN flat_type_counts ft ON d.flat_type = ft.flat_type
    LEFT JOIN storey_range_counts sr ON d.storey_range = sr.storey_range
    LEFT JOIN flat_model_counts fm ON d.flat_model = fm.flat_model
)

-- Final Selection with all columns and transformations
SELECT 
    town,
    binned_flat_type AS flat_type,
    binned_flat_range AS storey_range,
    binned_flat_model AS flat_model,
    block,
    street_name,
    floor_area_sqm,
    lease_commence_date,
    resale_price,
    price_per_sqm,
    remaining_lease_months,
    nearest_park_distance_km,
    nearest_mrt_exit_distance_km,
    nearest_moe_primary_school_distance_km,
    nearest_gep_school_distance_km,
    Price_Index_Lagged,
    EXTRACT(YEAR FROM DATE(month)) AS year,
    EXTRACT(MONTH FROM DATE(month)) AS month,
    LN(resale_price) AS log_resale_price,
    LN(price_per_sqm) AS log_price_per_sqm
FROM categorized_data;
