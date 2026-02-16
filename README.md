# Module 4 Homework: Analytics Engineering with dbt

## NOTE

Below is the exact markdown (MD) file from the original repository (data-engineering-zoomcamp) with **my answers on the bottom**. Please click [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/04-analytics-engineering/homework.md) to go to the actual original file.

In this homework, we'll use the dbt project in `04-analytics-engineering/taxi_rides_ny/` to transform NYC taxi data and answer questions by querying the models.

## Setup

1. Set up your dbt project following the [setup guide](../../../04-analytics-engineering/setup/)
2. Load the Green and Yellow taxi data for 2019-2020 into your warehouse
3. Run `dbt build --target prod` to create all models and run tests

> **Note:** By default, dbt uses the `dev` target. You must use `--target prod` to build the models in the production dataset, which is required for the homework queries below.

After a successful build, you should have models like `fct_trips`, `dim_zones`, and `fct_monthly_zone_revenue` in your warehouse.

---

### Question 1. dbt Lineage and Execution

Given a dbt project with the following structure:

```
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)
```

If you run `dbt run --select int_trips_unioned`, what models will be built?

- `stg_green_tripdata`, `stg_yellow_tripdata`, and `int_trips_unioned` (upstream dependencies)
- Any model with upstream and downstream dependencies to `int_trips_unioned`
- `int_trips_unioned` only
- `int_trips_unioned`, `int_trips`, and `fct_trips` (downstream dependencies)

#### Question 1 Answer

ANSWER:
- **`int_trips_unioned` only**

The model that will be built is just `int_trips_unioned`. If we wanted to do either upstream or downstream dependencies, we would need to add a `+` immedietly before or after the `int_trips_unioned` in the command.

For upstream dependencies to be included, we do:

```bash
dbt run --select +int_trips_unioned
```

For downstream dependencies to be included, we do:

```bash
dbt run --select int_trips_unioned+
```

To include both downstream and upstream dependencies, we do:

```bash
dbt run --select +int_trips_unioned+
```

---

### Question 2. dbt Tests

You've configured a generic test like this in your `schema.yml`:

```yaml
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
```

Your model `fct_trips` has been running successfully for months. A new value `6` now appears in the source data.

What happens when you run `dbt test --select fct_trips`?

- dbt will skip the test because the model didn't change
- dbt will fail the test, returning a non-zero exit code
- dbt will pass the test with a warning about the new value
- dbt will update the configuration to include the new value

#### Question 2 Answer

ANSWER:
- **dbt will fail the test, returning a non-zero exit code**

This is due to the fact that the `accepted_values` test only allows the values passed (1, 2, 3, 4, or 5), therefore, any row with `payment_type = 6` will make the test fail, making dbt return a non-zero exit code.


---

### Question 3. Counting Records in `fct_monthly_zone_revenue`

After running your dbt project, query the `fct_monthly_zone_revenue` model.

What is the count of records in the `fct_monthly_zone_revenue` model?

- 12,998
- 14,120
- 12,184
- 15,421

#### Question 3 Answer

ANSWER:
- **12,184**

I ran the following code in BigQuery to get this answer:

```sql
SELECT COUNT(*) FROM `module-4-dbt.dbt_prod.fct_monthly_zone_revenue`
```

---

### Question 4. Best Performing Zone for Green Taxis (2020)

Using the `fct_monthly_zone_revenue` table, find the pickup zone with the **highest total revenue** (`revenue_monthly_total_amount`) for **Green** taxi trips in 2020.

Which zone had the highest revenue?

- East Harlem North
- Morningside Heights
- East Harlem South
- Washington Heights South

#### Question 4 Answer

ANSWER:
- **East Harlem North**

I ran the following query in BigQuery to get my answer:

```sql
SELECT pickup_zone, SUM(revenue_monthly_total_amount) AS total_revenue_2020
FROM `module-4-dbt.dbt_prod.fct_monthly_zone_revenue` 
WHERE 
  service_type = 'Green' 
  AND 
  EXTRACT(YEAR FROM revenue_month) = 2020
GROUP BY pickup_zone
ORDER BY total_revenue_2020 DESC
LIMIT 1
```

---

### Question 5. Green Taxi Trip Counts (October 2019)

Using the `fct_monthly_zone_revenue` table, what is the **total number of trips** (`total_monthly_trips`) for Green taxis in October 2019?

- 500,234
- 350,891
- 384,624
- 421,509

#### Question 5 Answer

ANSWER:
- **384,624**

I ran the following query in BigQuery to get my answer:

```sql
SELECT SUM(total_monthly_trips)
FROM `module-4-dbt.dbt_prod.fct_monthly_zone_revenue` 
WHERE 
  service_type = 'Green' 
  AND 
  revenue_month = '2019-10-01'
```
---

### Question 6. Build a Staging Model for FHV Data

Create a staging model for the **For-Hire Vehicle (FHV)** trip data for 2019.

1. Load the [FHV trip data for 2019](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) into your data warehouse
2. Create a staging model `stg_fhv_tripdata` with these requirements:
   - Filter out records where `dispatching_base_num IS NULL`
   - Rename fields to match your project's naming conventions (e.g., `PUlocationID` → `pickup_location_id`)

What is the count of records in `stg_fhv_tripdata`?

- 42,084,899
- 43,244,693
- 22,998,722
- 44,112,187

#### Question 6 Answer

ANSWER:
- **43,244,693**

Firstly, I loaded the data into BigQuery using a python script, just as I had done before starting this homework for the green and yellow taxi data.

Then, I updated the sources.yml file in my dbt project to add the fhv_tripdata table as a source:

```yml
version: 2

sources:
  - name: raw_data
    description: "Raw data sources for NYC taxi rides"
    database: module-4-dbt # Project ID
    schema: nytaxi # Dataset name
    tables:
      - name: yellow_tripdata
        description: Raw yellow taxi trip data
      - name: green_tripdata
        description: Raw green taxi trip data
      - name: fhv_tripdata
        description: Raw fhv taxi trip data
```

Next, I made the ```stg_fhv_tripdata.sql``` file in my staging folder in my dbt project. In that, I wrote the following code to match with this questions criteria:

```sql
select dispatching_base_num,
    pickup_datetime,
    dropOff_datetime as dropoff_datetime,
    PUlocationID as pickup_location_id,
    DOlocationID as dropoff_location_id,
    SR_Flag as sr_flag,
    Affiliated_base_number as affiliated_base_number
from {{ source("raw_data", "fhv_tripdata")}}
where dispatching_base_num is not null
```

Finally, I then ran the following code in the dbt analysis folder (added the code and then clicked preview on dbt Cloud)

```sql
select count(*) from {{ref('stg_fhv_tripdata')}}
```

---