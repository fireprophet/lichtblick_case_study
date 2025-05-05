
# Consumption and Revenue attached to the attributes Creation Date and Product
is in file cons_rev_attached_creatdat_prodid.csv

# Answered questions

1) How did the average revenue (base price + consumption * working price) develop between 01.10.2020 and 01.01.2021?
It had a strong declining trend, as a result revenue has decreased by 84% to the end of this period.
Additional information is in the file change_of_revenue_between_01102020_and_01012021.csv

2) How many contracts were “in delivery” on 01.01.2021?

Answer: 21618

3) How many new contracts were loaded into the DWH on 01.12.2020?

Answer: 4702

# Instructions for Running the Solution and User Path to the Data Marts

Start the build with the command `docker-compose up -d` (or using the command `docker-compose build -d` to see detailed logs of component execution).  
Go in the browser to URL http://localhost:8001 in the Airflow UI (to login use admin/admin), select the DAG `etl_minio_dag` and  
click **Trigger DAG**, in the window you will be able to select launch parameters:

- `ENTITY` – set of entities for loading, default: `products,prices,contracts`  
- `START_MONTH` – date of the first export if a period is needed (to cover all files, for example 202010)  
- `END_MONTH` – date of the last export if a period is needed (to cover all files, for example 202101)  

Click **Trigger**.

If DAG successfully completed than we can execute request to target tables in pgadmin UI http://localhost:5050/browser/
For UI
Login:
Password:admin123

To register server
Host: postgres`
DB: etldb
User: etluser
Password: etlpass



# Pipeline description
- `airflow`, available on external port 8081 via browser  
- `airflow-scheduler`  
- `minio`, which acts as the data source, providing access to user CSV files  
- `postgres`, which implements the target database into which we load data and performs processing  
- `etl-runner`, a container that accepts API requests from Airflow to connect and load data  
  from the MinIO container where  
  user CSV files are stored in an S3 bucket into the **staging** of the target DB  
- `pgadmin` for implementing a UI for working with the target DB  
- `init`, which implements the initialization of the S3 bucket with data from a volume mounted to the container  

The DAG will start, consisting of the following tasks:

- `check_minio_and_db`, which checks access to S3 and the target DB before starting  
- `generate_load_id`, which creates a unique value serving as a load identifier (and which will be  
  used as a suffix for table naming when loading data into staging)  
- `create_dwh_tables`, launches SQL to create target tables:
  - `dwh.products`, `dwh.prices`, `dwh.contracts`  
- task group `load_entities`, within which a job will be sent to the `etl-runner` to load files  

Within its operation, the `etl-runner`, according to the request parameters, will filter by filename mask  
in the S3 bucket and take the required files, check them for schema compliance, and in case of  
a mismatch between the schema in the file and the fixed schema for loading, it will find the closest attribute name  
by distance from those found in the CSV file and generate a JSON file for this CSV file, which it will place  
in the `matchings` directory at the root of the project. The filenames look like:  
`matching_20201001220037_products.json`

The JSON looks like:
```json
{
  "__delimiter__": ";",
  "releasedversion": {
    "suggested": null,
    "user_submitted": 0,
    "pass_as_null": 1
  }
}
```

In the `suggested` field, the user can specify the field name from the CSV from which to take data to  
fill the required field according to the schema, set `user_submitted` to 1, or set `pass_as_null` to 1  
to fill the field with `no_data`.

After making changes to the files, the corresponding task can be re-run.

`create_staging_views`, Python code that creates, based on file name masks and suffix  
(load identifier), and fills the views:

- `staging.products_staging_view`
- `staging.prices_staging_view`
- `staging.contracts_staging_view`

as a `UNION ALL` of all tables loaded within the latest load.

- `load_products` – launches SQL code to load data into the `dwh.products` table  
- `load_prices` – launches SQL code to load data into the `dwh.prices` table  
- `load_contracts` – launches SQL code to load data into the `dwh.contracts` table  
- `load_prices_periods` – launches SQL code to calculate the active periods of prices and fills the `dwh.price_periods` table  
- `load_contracts_enriched` – launches SQL code to calculate the periods of specific prices  
  related to contracts, corresponding energy consumption, and revenue calculation for the contract for a time period  

At this point, the pipeline execution is complete. User-level questions can be answered using SQL queries  
to the following tables, namely:

1) Consumption and Revenue attached to the attributes Creation Date and Product
```sql
SELECT createdat, productid, SUM(revenue) AS sum_revenue, SUM(consumption) AS sum_consumption
FROM dwh.contract_price_enriched
GROUP BY createdat, productid
ORDER BY createdat, productid;
```

2) How did the average revenue (base price + consumption * working price) develop between 01.10.2020 and 01.01.2021?
```sql
SELECT createdat, SUM(revenue) AS sum_revenue, SUM(consumption) AS sum_consumption
FROM dwh.contract_price_enriched
WHERE createdat BETWEEN '10.01.2020' AND '01.01.2021'
GROUP BY createdat
ORDER BY createdat;
```

3) How many contracts were “in delivery” on 01.01.2021?
```sql
SELECT COUNT(DISTINCT contract_id)
FROM dwh.contracts
WHERE status = 'indelivery'
  AND startdate <= '01.01.2021' AND COALESCE(enddate, '01.01.9999') >= '01.01.2021';
```

4) How many new contracts were loaded into the DWH on 01.12.2020?
```sql
SELECT *
FROM dwh.contracts
WHERE DATE_PART('month', snapshot_date - INTERVAL '1 month') = DATE_PART('month', createdat);
```

# Description of Table Models in the DWH

# Approach to Population

## dwh.products
- Deduplication on all business fields except source_table  
- If a new version arrives (based on hash) — the old version is closed, new one is created  
- is_current always indicates the latest open version  
- Duplicates differing only in source_table — merged (MAX(source_table))  

## dwh.prices
- Full duplicates removed, keeping MAX(source_table)  
- Only new records inserted (NOT EXISTS on price_id + modificationdate)  
- Older versions marked is_current = FALSE  
- Only one current version per price_id  

## dwh.contracts
- Contracts with abnormally high usage/usagenet (>15000) are removed  
- For each contract_id + modificationdate, the latest by source_table is selected  
- New version is inserted if the modificationdate is new  
- Old version is closed using LEAD(modificationdate) — no overlaps  
- is_valid = FALSE if status is cancelled or terminated  
- is_current indicates only the latest version  


# Important Assumptions

- Since the data provides no reason to think otherwise, we assume indelivery contracts  
  are those for which energy delivery has not yet started, and thus no charges are incurred.  
- We assume baseprice is paid annually. If the contract year has started and the client terminates the contract,  
  they are still obligated to pay the baseprice for that year.  
- For calculating consumption and revenue, we use usagenet by default as more trustworthy.  
  If not available, we use usage.  
- It has been observed that customers have the same consumption value for the entire period.  
  The only explanation is that in the raw data, usagenet represents the cumulative consumption for the contract  
  at the moment of extraction.  
- We assume that revenue is calculated based on the price valid during the contract period.  
  Since prices could change during a contract’s term,  
  we calculate revenue by multiplying the price valid in a given period by the energy consumed in that period.  
- As we have no data on actual consumption per price period, we assume uniform consumption  
  throughout the contract term. Thus, we apply a weighted consumption proportionate to the period’s length.  
- The need to calculate revenue by price periods explains the necessity of determining  
  price periods (which is performed within the load_prices_periods task).
