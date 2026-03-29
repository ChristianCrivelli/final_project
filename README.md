# Read Me

## Project Overview
ClearSpend is a fast-growing fintech company processing millions of transactions across the United States. This project implements a complete end-to-end data pipeline to transform raw, fragmented operational data into a clean, centralized, and analytics-ready dimensional data warehouse.

The platform is designed to provide reliable insights for three key business units:

Finance Team: Monthly revenue, refund rates, and geographic revenue distribution.

Customer Analytics: Customer lifetime value, online vs. in-store behavior, and active card counts.

Merchant Partnerships: Top-performing merchants, industry growth, and error rate analysis.

## Architecture & Data Flow
The pipeline follows a multi-layered (Medallion) architecture to ensure data quality and separation of concerns:

## Ingestion Layer (Bronze): 
Raw CSV exports (Transactions, Users, Cards, and MCC data) are loaded into a staging schema without type enforcement to ensure 100% data capture.

## Transformation Layer (Silver): 
Data is cleaned and standardized. This includes handling currency formatting (e.g., removing '$' and parsing 'k' notation), normalizing categorical fields like card_brand and employment_status, and enforcing data types.

## Curated Layer (Gold): 
A dimensional model (Star Schema) is built to facilitate fast reporting. This layer contains fact tables and business-specific dimensions like dim_users, dim_cards, and dim_merchant.

## Repository Structure
pipeline.py: The main orchestrator that runs the entire end-to-end process in the correct sequence.

setup_db.py: Initializes and resets the SQL Server or PostgreSQL database.

ingestion_*.py: Scripts to define the schema and bulk-load raw CSV data.

transformation_*.py: Modular scripts for cleaning specific datasets (Users, Cards, Transactions, MCC).

curated_*.py: Defines the final warehouse schema and populates the Star Schema.

config.py: Centralized configuration for database connections and local data paths.

## Getting Started
### Prerequisites
Python 3.x

Database: SQL Server (Express) or PostgreSQL.

Drivers: ODBC Driver 17 for SQL Server (if using SQL Server).

Libraries: pandas, numpy, pyodbc, psycopg.

### Data Warehouse Design
The platform utilizes a Star Schema to optimize query performance for business analysts.

Fact Table: facts_table contains grain-level transaction data including amounts, dates, and foreign keys to dimensions.

Dimensions:

dim_users: Detailed customer demographics and financial status.

dim_cards: Card-specific attributes including issuer risk and bank types.

dim_merchant: Standardized merchant locations and descriptions.
