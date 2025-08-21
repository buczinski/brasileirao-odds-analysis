Brasileirão Odds Analysis (Work in Progress)

This project aims to analyze betting odds and match results from the Brasileirão (Brazilian football league) to explore trends, evaluate prediction models, and compare pre-match odds against actual outcomes.

Current Status
Data: A few rounds of Brasileirão results and odds have been collected and are available as CSV files.
Code: Scripts for fetching data from free APIs and updating CSV files are included.
Automation: A local DAG (Directed Acyclic Graph) is set up to update the CSV after each round of the tournament.
⚠️ Note: This project is a work in progress and is not yet fully automated or complete.

Features / Goals
Fetch odds and results data using free APIs.
Automate updates after every round using a DAG (intended to eventually run on a cloud VM).
Store historical data on the cloud for easy access and analysis.
Analyze odds accuracy, trends, and patterns in match outcomes.

Future Plans
More Data: Collect squad information, player stats, transfers, injuries, and match lineups using web scraping.
Cloud Integration: Run the DAG on a virtual machine and save data to cloud storage for scalability.
Enhanced Analysis: Implement visualizations, predictive models, and metrics to assess betting odds.
Documentation: Provide clear usage instructions and examples.
