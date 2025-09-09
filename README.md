# brasileirao-odds-analysis

# Brasileirão Odds Analysis (Work in Progress)

This project analyzes **betting odds and match results** from the Brasileirão (Brazilian football league) to explore trends, evaluate prediction models, and compare pre-match odds against actual outcomes.

---

##  Current Status

- **Data**: Match results and betting odds are now stored in Parquet format on an S3 bucket, allowing efficient storage and querying.

- **Code**: Scripts fetch data from free APIs and update Parquet files automatically.

- **Automation**: The DAG is now running on AWS MWAA (Managed Workflows for Apache Airflow) to keep the dataset up to date after each round.

- **Querying**: Historical data can be queried using Athena, enabling quick analysis without downloading the entire dataset.

⚠️ **Note**: This project is a work in progress and not yet fully automated or complete.  

---

## Features / Goals

- **Automated Data Collection**: Fetch match results and betting odds from free APIs and update the dataset automatically.

- **Cloud Storage**: Store historical data in Parquet format on S3 for efficiency and scalability.

- **Workflow Automation**: Maintain up-to-date datasets with an Airflow DAG running on MWAA.

- **Query & Analysis**: Use Athena to query historical data without downloading full datasets.

- **Insights & Evaluation**: Analyze odds accuracy, detect trends, and compare predictions against actual match outcomes.

---

## Future Plans

- **More Data**:  
  - Collect squad information, player stats, transfers, injuries, and match lineups via web scraping.  

- **Cloud Integration**:  
  - Further optimize the DAG and Athena queries for efficiency and cost management.  

- **Enhanced Analysis**:  
  - Build visualizations, predictive models, and metrics to evaluate betting odds and improve forecasts.  


---
