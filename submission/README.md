## Overview
- This repository implements the ingestion of data from API endpoints, stores as is in Bronze Layer in csv format.
- After the Bronze in Silver layer duplicated rows removed, converted data types to required types and added column for 
validated rows/records.
- In Gold Layer the required dimension table and fact table created on Start schema. 
---

## Summary
### High-Level Flow
1. **API Ingestion Layer**
    - Calls external REST API endpoints
    - Handles authentication (token-based) and automatic token refresh
    - Handles 429 and 500 errors and retry
    - Extracts  JSON data

2. **Bronze Layer**
    - Stores API responses as-is (CSV)
    - Supports Re-Run
    - Added Column's `_ingested_at` and `_source_endpoint`.

3. **Silver Layer**
    - Removed Duplicate Rows
    - Applies schema enforcement and data quality rules

4. **Gold Layer**
    - Created the dimension table's  dim_products, dim_customer, dim_sellers, and fact_order_items fact table.

---

## Technical Decisions and Reasoning

### 1. API Ingestion
- In the API ingestion created class to handle the various task's like bearer token authentication, auto refresh
  and the retry logic.
- created a method `authenticate` to refresh the bearer token when expired.
- For retry logic created a for loop to get data.
- In for loop applied if condition's for different request status code's.
- For token expire(401), used `authenticate` and pinged the API endpoint again.
- For 429 and 500, used sleep time. In every attempt the sleep time increased, so ping to api get delayed.
- Applied the `?date_from=2018-07-01` in every endpoint.
- Used limited number of retries as infinite loop is not practical.

### 2. PySpark for Processing
**Decision:** Used PySpark for transformation and modeling.  
**Reasoning:**
- As PySpark is used for large data processing, as data from  API came  in large quantities.
- If the need for streaming data from API is required in the future, PySpark can work with stream data.

### 3. Silver Layer
**Decision:** Drop Duplicate on row and NULL propagate.  
**Reasoning:**
- Used drop duplicate on every ingested file on their  respective column's except _ingested_at and _source_endpoint.
- As Null's from `orders` not dropped or replaced . Thought they required as-is.
- For `Products` null's replaced them with `0` for int/decimal column's and with `UNKNOWN` for String column.


### 4. Gold Layer
- Created the table's and stored them in spark's in-memory catalog.
- Create dimension table's and then fact table
- Used monotonically_increasing_id() for the surrogate key's of respective dimension and fact table.



---

---

## For Production on Azure / Microsoft Fabric

### Scheduling and Orchestration
- Use **Azure Data Factory** or **Fabric Data Pipelines** for scheduling.
- Use parameterized pipelines for different endpoints.

### Incremental Loads and CDC
- Use **MERGE** logic in Gold tables and Silver files too.

### Storage and Table Format
- Replace csv files in Bronze and Silver Layer with delta format.
- Easier for which files are processed, no need for manifest file.
- Create the Gold Layer too in delta table's.

### Monitoring and Observability
- Use Azure/ADF(pipeline runs) Monitor for monitoring.
- Use Azure Alerts or add alerting in adf pipelines for pipeline success/fail.

### CI/CD and Deployment
- Use Github Actions/ Azure DevOps.
- Create separate env for test/prod.

### Security
- Store secrets (API keys, tokens) in Azure Key Vault
- Use Managed Identity where possible for different azure services.
- RBAC for storage accounts

### Cost Optimization
- Use spark's caching and delta lake optimization.
- If batch run , use incremental loads.
- Use autoscaling .


---
