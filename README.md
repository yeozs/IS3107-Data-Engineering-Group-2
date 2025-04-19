Singapore’s public housing system, HDB, provides accommodations for about 85% of Singapore’s population (Lim, 2024). The buying and selling of a HDB flat is not just a significant financial decision. It is often one of the most meaningful milestones in a person’s life (Lim, 2024). This is especially true in the resale sector, where flats are subject to fluctuating market conditions and pricing (Durai and Wang, 2023). Resale stakeholders must carefully evaluate their financial readiness amidst change and uncertainties. This project provides a comprehensive prediction model for Singapore HDB resale flat prices, leveraging historical pricing data alongside economic and geographical variables. Modelling techniques utilised include ARIMA, Linear Regression and XGBoost. This Data Engineering Project utilises the ETL framework.

Project deliverables include:
1) A Google Looker dashboard that provides real-time insights on historical and predicted future HDB flats prices
2) A Streamlit interface utilising machine-learning models to predict resale prices based on historical (Linear Regression) and future (XGBoost) market trends
--------------------------------------------------------------
Navigating Folders:

1. DAG Files [TBC]

2. BigQuery Transformation
- This folder consist of SQL file to transform our Extracted files to proper format for ingestion into Google BigQuery

3. BigQuery ARIMA and ML Models
- The 5 SQL files are as follows:
  1) V3.0_ARIMAPLUS_Model_PriceIndex.sql - SQL to create ARIMA Model for predicting Future Resale Price Index. Results used in Looker Dashboard and for Streamlit Application Future Price Prediction beyond the current dataset timeline (Initially from year Jan 2017 - Dec 2024)
  
  2) V3.0_ARIMAPLUS_Model_ResalePrice.sql - SQL to create ARIMA Model for predicting Future Median Resale Prices, considering town and flat type. Results used in Looker Dashboard.
 
  3) V3.0_Linear_Regression.sql - SQL to create Linear Regression Model for predicting historical resale prices. This model was compared with XGBoost and found to have lower R^2 Score. It was ultimately not utilised in predicting historical resale price in Streamit Application.
 
  4) V3.0_Linear_Regression_Future.sql - SQL to create Linear Regression Model for predicting future resale prices. This model is used in Streamlit Application to predict future resale prices.
 
  5) V3.0_XGBoost_Model.sql - SQL to create XGBoost Model for predicting historical resale prices. This model is used in Streamlit Application to predict historical resale prices.

4. Streamlit Application. Contains files for Streamlit Application. For details look at README.md in folder.
  
5. Important Links

Google Looker Dashboard - https://lookerstudio.google.com/reporting/b926e71a-5e7b-4157-89f2-5f2a0ba1eb57
