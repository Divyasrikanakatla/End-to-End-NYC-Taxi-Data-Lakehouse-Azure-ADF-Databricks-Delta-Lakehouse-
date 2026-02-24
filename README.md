🚕** End-to-End NYC Taxi Data Lakehouse on Azure (ADF + Databricks)**
This project demonstrates the design and implementation of a scalable, end-to-end modern data lakehouse solution on Microsoft Azure using real-world NYC Taxi trip data.
The solution leverages Azure Data Factory for orchestration, Azure Data Lake Storage Gen2 for storage, and Azure Databricks with Delta Lake for transformation and analytics following the Medallion Architecture (Bronze, Silver, Gold).
________________________________________
🏗** Architecture Overview**
•	Data Ingestion: Azure Data Factory
•	Storage: Azure Data Lake Storage Gen2
•	Processing & Transformation: Azure Databricks (PySpark)
•	Storage Format: Delta Lake
•	Data Modeling: Star Schema (Gold Layer)
•	Security: Unity Catalog with RBAC
________________________________________
🥉🥈🥇** Medallion Architecture**
Bronze Layer
•	Raw NYC Taxi trip data ingestion
•	Minimal transformation
•	Schema enforcement
Silver Layer
•	Data cleansing and validation
•	Deduplication
•	Data type casting and enrichment
Gold Layer
•	Business-level aggregations
•	Fact and dimension tables
•	Optimized for analytical queries
________________________________________
⚙️** Key Features**
•	End-to-end automated ETL pipeline
•	Batch data processing using PySpark
•	Delta Lake ACID transactions
•	Schema evolution handling
•	Secure external locations with managed identity (RBAC)
•	Star schema modeling for reporting
________________________________________
📊** Tech Stack**
•	Azure Data Factory
•	Azure Data Lake Storage Gen2
•	Azure Databricks
•	Delta Lake
•	PySpark
•	SQL
•	Unity Catalog
________________________________________
🚀 What This Project Demonstrates
•	Cloud-based modern data engineering architecture
•	Medallion data modeling approach
•	End-to-end pipeline orchestration
•	Secure data access using Azure RBAC

