# DWH Car Sales – Data Warehouse for Automotive Sales  

## Introduction  
**DWH Car Sales** is a complete Data Warehouse project implemented in **PostgreSQL (PL/pgSQL)**.  
It integrates car sales data from both **online** and **offline** channels, enabling advanced analytics and business intelligence reporting for dealership network.  

The project covers the full ETL process:  
- ingestion of CSV sources,  
- staging transformations,  
- loading into a normalized **3NF layer**,  
- building a **dimensional Data Mart (star schema)** for reporting,  
- logging and monitoring of all ETL processes.  

---

## Key Features  

- **Full ETL pipeline in PostgreSQL (PL/pgSQL)** – from CSV sources to Data Mart.  
- **Layered architecture** – Staging → 3NF → Data Mart (Dimensional Model).  
- **SCD Type 2** for the **Customer dimension**, enabling historical tracking of changes.  
- **Incremental loading (IOTD – Incremental Over Time Data)** – optimized for performance and minimizing reprocessing.  
- **Partitioned Fact Table** (`FCT_SALES`) by date for efficient time-series queries.  
- **Comprehensive logging** – dedicated `LOG_TABLE` and `LOG_PROCEDURE` to track ETL execution, affected rows, and error handling.  
- **Support for hybrid sources** – handling structural differences between **offline** and **online** sales datasets.  

---

## Architecture  

The architecture consists of 4 main layers:  

1. **Sources**  
   - Two CSV datasets:  
     - `car_sales_offline.csv` – traditional dealer transactions.  
     - `car_sales_online.csv` – e-commerce transactions.  

2. **Staging**  
   - Temporary layer for initial data loading and validation.  

3. **BL_3NF (Business Layer – 3rd Normal Form)**  
   - Normalized storage ensuring data integrity and consistency.  
   - Includes key entities: `CE_CUSTOMERS_SDC`, `CE_SALES`, `CE_CARS`, `CE_DEALERS`, etc.  

<p align="center">  
  <img src="screenshots/3NF.png" alt="Model 3NF" width="600"/>  
</p>

4. **BL_DM (Business Layer – Data Mart)**  
   - Star schema for reporting and analytics.  
   - Dimensions (customers, cars, employees, dealers, dates, etc.) and fact table (`FCT_SALES`).  

<p align="center">  
  <img src="screenshots/DM.png" alt="Model 3NF" width="600"/>  
</p> 

5. **BL_CL (Control Layer)**  
   - Logging schema with:  
     - `LOG_TABLE` – ETL execution log.  
     - `LOG_PROCEDURE` – procedure for centralized logging with error handling.  

---
