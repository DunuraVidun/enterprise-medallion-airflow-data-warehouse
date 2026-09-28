# ABC Hub Data Warehouse

## 1. Project Overview

ABC Hub is a PostgreSQL-based analytical data warehouse implemented
using a layered **Bronze, Silver, and Gold architecture**. The platform
ingests data from an OLTP PostgreSQL source, applies cleansing and
validation in the Silver layer, maintains historical dimension changes
using **SCD Type 2**, and produces analytical Gold fact tables.

**Apache Airflow 3.3.0** is used as the central orchestration platform.
Python is used for ETL control logic and database interaction, while SQL
files contain the primary extraction, cleansing, dimensional, and
fact-loading logic.

The implementation includes:

-   Initial/full Bronze ingestion
-   Incremental Bronze delta ingestion using a column-based watermark
-   Silver cleansing, validation, and deduplication
-   Derived Silver profile tables
-   SCD Type 2 Gold dimensions
-   Gold analytical fact tables
-   Cross-DAG dependency management
-   Transactional Gold loading
-   ETL auditing
-   Retry and timeout configuration
-   Re-runnable pipeline processing

------------------------------------------------------------------------

## 2. Architecture

The implemented processing flow is:

``` text
                         PostgreSQL OLTP
                               |
              +----------------+----------------+
              |                                 |
              v                                 v
     01_full_load_bronze              02_delta_load_bronze
              |                                 |
              +----------------+----------------+
                               |
                               v
                    Bronze Layer
                               |
                               v
                    03_silver_load_layer
                               |
                               v
                    Silver Layer
                               |
                  +------------+------------+
                  |            |            |
                  v            v            v
          Customer Profile Content Profile Inventory Profile
                  |            |            |
                  v            v            v
              04_gold_dim_load_layer
                  |
                  v
             Gold Dimensions
             (SCD Type 2)
                  |
                  v
             05_gold_fact_load_layer
                  |
                  v
             Gold Fact Tables
```

The Gold fact layer also waits directly for the Silver transactional
tables required for each fact and for the corresponding Gold dimension.

------------------------------------------------------------------------

## 3. Technology Stack

  -----------------------------------------------------------------------
  Technology / Component              Purpose
  ----------------------------------- -----------------------------------
  Apache Airflow 3.3.0                Workflow orchestration, scheduling,
                                      dependencies, retries, and
                                      execution monitoring

  Python 3.12.3                       ETL control flow, database
                                      operations, SQL execution, and
                                      audit handling

  PostgreSQL 18.4                     Relational source/target database
                                      platform

  SQL                                 Data extraction, cleansing,
                                      transformation, SCD Type 2
                                      processing, and fact generation

  PythonOperator                      Executes Python-based ETL functions

  ExternalTaskSensor                  Coordinates dependencies between
                                      Airflow DAGs

  PostgresHook                        Provides Airflow-to-PostgreSQL
                                      connectivity and transactional
                                      database access

  Airflow templating                  Supplies dynamic execution values
                                      such as `{{ data_interval_end }}`

  PostgreSQL audit schema             Stores ETL execution and
                                      record-count audit information
  -----------------------------------------------------------------------

### Environment

  Component                                Version / Environment
  ---------------------------------------- -----------------------
  Host Operating System                    Windows 11
  Airflow execution environment            WSL (Linux)
  Python                                   3.12.3
  Apache Airflow                           3.3.0
  PostgreSQL                               18.4
  PostgreSQL platform reported by server   `x86_64-windows`

> **Note:** The Airflow project is executed inside WSL, while the
> confirmed PostgreSQL 18.4 server reports the Windows platform.
> PostgreSQL connectivity depends on how the PostgreSQL server is
> configured and exposed to WSL.

------------------------------------------------------------------------

## 4. Project Structure

The main project structure is:

``` text
Mini_project-02/
├── dags/
│   ├── 01_full_load_bronze_layer.py
│   ├── 02_delta_load_bronze_layer.py
│   ├── 03_silver_load_layer.py
│   ├── 04_gold_dim_load_layer.py
│   └── 05_gold_fact_load_layer.py
│
├── sql/
│   ├── bronze/
│   │   ├── full_load/
│   │   └── delta_load/
│   │
│   ├── silver/
│   │   └── profile/
│   │
│   └── gold/
│       ├── dimension/
│       └── fact/
│
├── utils/
│   ├── database.py
│   ├── audit.py
│   └── file.py
│
├── .gitignore
└── .vscode/
```

The `.gitignore` excludes Python cache files, virtual environments,
Airflow logs/local metadata, environment files, and
operating-system-specific files.

------------------------------------------------------------------------

## 5. Prerequisites

The following environment is required or recommended:

1.  Windows 11 host operating system
2.  WSL with a Linux distribution
3.  Python 3.12.3
4.  Apache Airflow 3.3.0
5.  PostgreSQL 18.4 or a compatible PostgreSQL installation
6.  Airflow PostgreSQL provider support
7.  Access to the ABC Hub OLTP PostgreSQL database
8.  An OLAP PostgreSQL database for the warehouse

The project should be executed from the WSL environment where Apache
Airflow is installed.

------------------------------------------------------------------------

## 6. Python and Airflow Setup

### 6.1 Verify Python

From WSL:

``` bash
python3 --version
```

Expected environment:

``` text
Python 3.12.3
```

### 6.2 Verify Airflow

``` bash
airflow version
```

Expected:

``` text
3.3.0
```

### 6.3 Verify PostgreSQL connectivity

From the environment where PostgreSQL client tools are available:

``` bash
psql --version
```

The PostgreSQL server can also be verified using:

``` sql
SELECT version();
```

or:

``` sql
SHOW server_version;
```

------------------------------------------------------------------------

## 7. Dependencies

The project uses the following Python-level components directly in its
implementation:

-   Apache Airflow
-   Apache Airflow PostgreSQL provider
-   Python standard library modules such as `pathlib`, `datetime`,
    `logging`, and `sys`

The DAGs import Airflow components including:

``` text
DAG
PythonOperator
EmptyOperator
ExternalTaskSensor
TriggerRule
PostgresHook
```

The project does not contain a `requirements.txt` file in the supplied
project package. Therefore, the exact provider package version used with
Airflow is not pinned in the repository.

When recreating the environment, install Apache Airflow 3.3.0 and a
PostgreSQL provider version compatible with that Airflow release.

------------------------------------------------------------------------

## 8. Database Configuration

The DAGs use two Airflow PostgreSQL connection IDs:

``` text
abc_hub_oltp
abc_hub_olap
```

### Source connection

``` text
Connection ID: abc_hub_oltp
Purpose: ABC Hub OLTP source database
```

### Target connection

``` text
Connection ID: abc_hub_olap
Purpose: ABC Hub analytical warehouse database
```

The actual username, password, host, port, and database credentials are
intentionally not stored in the project source code.

Configure these connections through the Airflow connection management
interface or CLI before executing the DAGs.

Example CLI pattern:

``` bash
airflow connections add abc_hub_oltp \
    --conn-type postgres \
    --conn-host <host> \
    --conn-login <username> \
    --conn-password <password> \
    --conn-port 5432 \
    --conn-schema <oltp_database>
```

``` bash
airflow connections add abc_hub_olap \
    --conn-type postgres \
    --conn-host <host> \
    --conn-login <username> \
    --conn-password <password> \
    --conn-port 5432 \
    --conn-schema <olap_database>
```

Replace the placeholders with the actual environment-specific values.

------------------------------------------------------------------------
9. PostgreSQL Database Initialization

Before executing the Airflow DAGs, the required PostgreSQL database structures must be created and the source data must be available.

The project includes SQL scripts for initializing the OLTP source database, warehouse schemas, and audit infrastructure:

Script	                    Purpose
01_create_oltp_schema.sql	Creates the required OLTP source schema and tables
02_import_data.sql	        Imports the provided source data and handles the payment CSV data-type compatibility workaround
03_bronze_schema.sql	    Creates the Bronze-layer schema and tables
04_silver_schema.sql	    Creates the Silver-layer schema and tables
05_gold_dim.sql	            Creates Gold dimension tables
06_gold_fact.sql	        Creates Gold fact tables
audit_table.sql	            Creates the ETL audit schema and audit.etl_audit table

The scripts should be executed in the appropriate order so that the required source, warehouse, and audit structures exist before the Airflow pipelines are executed.

The resulting database structure contains the main warehouse layers:

ABC_Hub_oltp
└── OLTP source tables

ABC_Hub_olap
├── bronze
├── silver
├── gold
└── audit

The Airflow DAGs subsequently populate and transform these structures using the SQL processing scripts stored in the project.


## 10. Airflow DAGs

The implementation contains five operational DAGs.

### 10.1 `01_full_load_bronze`

Performs the initial/full ingestion from the OLTP source into the Bronze
layer.

The full-load DAG:

-   Reads SQL files from `sql/bronze/full_load/`
-   Extracts source records
-   Truncates target Bronze tables where configured
-   Loads the extracted records
-   Records execution statistics through the audit framework

This DAG is intended for initial population or controlled full
reprocessing.

------------------------------------------------------------------------

### 10.2 `02_delta_load_bronze`

Performs incremental Bronze ingestion.

The DAG dynamically obtains the latest watermark from the target Bronze
table using:

``` sql
SELECT COALESCE(
    GREATEST(
        MAX(created_at),
        MAX(updated_at)
    ),
    TIMESTAMP '1900-01-01'
)
FROM <target_table>;
```

The resulting watermark is substituted into the delta SQL template.

The extraction window is defined using:

``` sql
WHERE GREATEST(created_at, updated_at) > '{{ last_watermark }}'
AND GREATEST(created_at, updated_at) <= '{{ processing_date }}';
```

The processing boundary is supplied by Airflow through:

``` text
{{ data_interval_end }}
```

This allows the Bronze layer to process only records falling within the
relevant incremental window.

------------------------------------------------------------------------

### 10.3 `03_silver_load_layer`

Performs Silver-layer transformation.

The Silver processing:

-   Reads Bronze data
-   Cleans and standardizes values
-   Validates required attributes
-   Handles invalid records according to the implemented SQL logic
-   Deduplicates source records where required
-   Rebuilds Silver tables using truncate-and-reload processing
-   Produces derived profile tables for Gold dimensions

The DAG uses `ExternalTaskSensor` to coordinate execution with the
Bronze DAG.

The profile tasks include:

``` text
load_silver_customer_profile
load_silver_content_profile
load_silver_inventory_item_profile
```

------------------------------------------------------------------------

### 10.4 `04_gold_dim_load_layer`

Loads the Gold dimension tables using SCD Type 2 processing.

The implemented dimensions are:

``` text
gold.dim_customer
gold.dim_content
gold.dim_inventory_item
```

Each dimension branch waits for its corresponding Silver profile task.

SCD Type 2 processing:

1.  Identifies the current dimension version.
2.  Compares tracked attributes with the incoming Silver profile.
3.  Closes the current version when a genuine change is detected.
4.  Inserts a new historical version.
5.  Retains previous versions for historical analysis.
6.  Leaves the current version unchanged when no tracked attribute has
    changed.

Gold dimension loading is executed through PostgreSQL transactions.
Successful processing is committed, while exceptions trigger rollback.

------------------------------------------------------------------------

### 10.5 `05_gold_fact_load_layer`

Loads the analytical Gold fact tables.

The implemented facts are:

``` text
gold.fact_customer_daily_activity
gold.fact_content_monthly_performance
gold.fact_inventory_daily_utilization
```

The fact DAG waits for:

-   Required Silver transactional data
-   Corresponding Gold dimensions

The fact tables use a **full-refresh strategy**. Each fact SQL truncates
the target and rebuilds it using `INSERT ... SELECT`.

The loader:

1.  Calculates the expected fact-grain row count.
2.  Reads the fact SQL.
3.  Executes the full-refresh operation.
4.  Commits the transaction.
5.  Counts rows actually loaded.
6.  Calculates audit statistics.
7.  Rolls back if an exception occurs.

------------------------------------------------------------------------

## 10. Pipeline Execution Order

For a normal end-to-end execution, the logical processing order is:

``` text
01_full_load_bronze
        │
        ▼
02_delta_load_bronze
        │
        ▼
03_silver_load_layer
        │
        ▼
04_gold_dim_load_layer
        │
        ▼
05_gold_fact_load_layer
```

The project also contains explicit cross-DAG sensors. The Gold fact DAG
independently waits for the relevant Silver tasks and Gold dimension
tasks rather than relying only on the visual DAG sequence above.

### Important

The full Bronze load is intended for initial population or controlled
reprocessing. Normal daily operation uses the incremental Bronze delta
DAG followed by the downstream Silver and Gold DAGs.

------------------------------------------------------------------------

## 11. Incremental Loading and Watermarking

The Bronze incremental process uses a **column-based watermark** derived
from the current target state.

For each target table, the latest value of `created_at` or `updated_at`
is used as the previous watermark. If the target contains no records,
the fallback value is:

``` text
1900-01-01
```

The extraction then applies a lower and upper boundary:

``` text
previous watermark < source change timestamp <= processing boundary
```

This provides a bounded incremental extraction window and avoids
normally reprocessing records at or before the previously represented
watermark.

The watermark is calculated immediately before each table load, so the
mechanism is maintained at the table level without requiring a separate
checkpoint store.

------------------------------------------------------------------------

## 12. Silver Transformation Strategy

The Silver layer acts as the cleansing and validation layer between
Bronze and Gold.

Typical processing includes:

-   Trimming whitespace
-   Converting empty strings to NULL where applicable
-   Standardizing textual values
-   Normalizing email values
-   Validating mandatory attributes
-   Validating date values
-   Deduplicating records
-   Producing reusable profile datasets

Silver tables are rebuilt from the current Bronze state rather than
incrementally appended.

------------------------------------------------------------------------

## 13. SCD Type 2 Strategy

The following dimensions use SCD Type 2 processing:

``` text
dim_customer
dim_content
dim_inventory_item
```

The dimension model maintains:

``` text
surrogate key
business key
historical attributes
effective_from
effective_to
is_current
```

The interval representation is based on:

``` text
effective_from <= timestamp < effective_to
```

The initial versions use analytical historical anchor dates defined in
the dimension SQL, while subsequent versions use the incoming source
`updated_at` value as the version boundary.

Because the source data did not provide a reliable historical
business-change timestamp for every change, the implementation uses the
available `updated_at` processing/source timestamp for subsequent SCD
boundaries. This is an implementation assumption and should be
considered when interpreting historical effective dates.

------------------------------------------------------------------------

## 14. Fact Loading Strategy

The Gold fact tables are rebuilt using a full-refresh approach.

The general pattern is:

``` text
TRUNCATE target fact table
          ↓
INSERT ... SELECT
          ↓
COMMIT
```

This differs from an incremental upsert design.

The approach ensures that repeated successful executions do not
accumulate duplicate fact records because the previous target contents
are removed before reconstruction.

For the same Silver and Gold dimension input state, the resulting
fact-grain dataset is expected to remain consistent.

------------------------------------------------------------------------

## 15. Fact-to-SCD Historical Referencing

Gold facts use the surrogate keys of the corresponding SCD Type 2
dimensions.

The lookup determines the dimension version valid for the relevant
analytical period.

For example:

-   Customer daily activity uses the customer dimension version valid at
    the end of the activity day.
-   Content monthly performance uses the content dimension version valid
    at the end of the activity month.
-   Inventory daily utilization uses the inventory dimension version
    valid at the end of the activity day.

This allows historical facts to retain the dimensional attributes that
were valid for the corresponding analytical period.

------------------------------------------------------------------------

## 16. Idempotency and Re-execution

The pipeline uses different reprocessing strategies at different layers.

### Bronze

Incremental loading uses a target-derived watermark to identify records
that have not normally been represented in the target at or before the
previous watermark.

### Silver

Silver tables use truncate-and-reload processing from Bronze. Re-running
the same Silver transformation therefore rebuilds the table from the
current Bronze state rather than appending another copy.

### Gold dimensions

SCD Type 2 change detection compares incoming profile attributes with
the current dimension version. If no tracked attribute has changed, the
current record remains active and no unnecessary historical version is
created.

### Gold facts

Gold facts use full refresh. Re-running the same logical processing
removes the existing fact contents and reconstructs the fact table,
preventing duplicate accumulation.

Idempotency should be validated using both ETL audit records and
database-state comparisons such as row counts, fact-grain uniqueness,
and SCD version counts.

------------------------------------------------------------------------

## 17. ETL Auditing

The project contains an `audit.etl_audit` table with the following main
fields:

``` text
audit_id
batch_id
dag_id
run_id
start_time
end_time
status
source_record_count
processed_record_count
inserted_record_count
updated_record_count
rejected_record_count
```

Each DAG has a `start_audit` task and a `finish_audit` task.

The audit utility collects task results through Airflow XCom and updates
the corresponding audit record.

Audit information can be queried using:

``` sql
SELECT
    audit_id,
    batch_id,
    dag_id,
    run_id,
    start_time,
    end_time,
    status,
    source_record_count,
    processed_record_count,
    inserted_record_count,
    updated_record_count,
    rejected_record_count
FROM audit.etl_audit
ORDER BY start_time DESC;
```

For concise reporting, executions can be ranked per DAG and the latest
five records selected.

------------------------------------------------------------------------

## 18. Error Handling and Retries

The DAGs define common retry settings including:

``` text
retries = 1
retry_delay = 2 minutes
```

Gold loading additionally uses database transactions.

For transactional Gold processing:

``` text
Successful execution → COMMIT
Exception             → ROLLBACK
```

The implementation also uses execution timeouts for selected ETL tasks.

Python exceptions are logged and re-raised so that Airflow can recognise
the task failure and apply its configured retry behaviour.

------------------------------------------------------------------------

## 19. Cross-DAG Dependencies

`ExternalTaskSensor` is used to coordinate processing between the DAGs.

Examples include:

``` text
Silver → Bronze
Gold Dimensions → Silver profiles
Gold Facts → Silver transactional tasks
Gold Facts → Gold dimensions
```

Sensors are configured to recognise successful upstream tasks and to
fail when configured upstream failure states are encountered.

The sensors use reschedule mode so that waiting tasks do not
continuously occupy a worker slot while waiting for the upstream
execution.

------------------------------------------------------------------------

## 20. Scheduling

The implemented DAGs use daily scheduling:

``` text
schedule = "@daily"
```

The DAGs use:

``` text
start_date = 2026-01-01
catchup = False
```

Maximum active runs are restricted to one for the implemented DAGs to
reduce concurrent execution of the same pipeline stage.

------------------------------------------------------------------------

## 21. Configuration and SQL Management

SQL logic is stored separately from Python orchestration code.

The DAGs construct SQL file paths using the project directory structure
and load SQL through the shared utility:

``` text
utils/file.py
```

The `read_sql()` utility reads the SQL file and returns its contents for
execution.

This separation keeps orchestration logic in Python while keeping data
transformation and loading logic in SQL.

------------------------------------------------------------------------

## 22. Assumptions and Limitations

### Source timestamps

The source dataset does not provide a complete reliable historical
business-change timestamp for all records. The implementation therefore
uses the available `created_at` and `updated_at` values for incremental
processing and SCD version boundaries.

### SCD initial dates

Initial SCD Type 2 versions use analytical anchor dates defined by the
dimension-loading SQL. These anchors are used to allow historical
analytical records to match an appropriate initial dimension version.

### Bronze delta loading

The Bronze incremental mechanism is based on the latest timestamp
represented in the target table. It is therefore a target-state-derived
watermark rather than a separately persisted checkpoint.

### Gold facts

Gold facts are currently implemented as full refreshes rather than
incremental upserts. Consequently, audit insert/update counts for Gold
facts should be interpreted as full-refresh execution statistics rather
than incremental change statistics.

### Current-date fact generation

Some Gold fact date spines extend through `CURRENT_DATE`. Therefore, a
run performed on a later date can legitimately produce additional
current-period rows even when the historical source state has not
changed.

------------------------------------------------------------------------

## 23. Final Technology Decision

The completed implementation uses **Apache Airflow as the central
orchestration platform**, with Python and PostgreSQL SQL providing the
ETL processing capabilities. Apache NiFi was not included in the final
implementation.

Airflow is responsible for scheduling, task orchestration, cross-DAG
dependency management, retries, execution monitoring, and audit-task
coordination. PythonOperator is used to execute the Python ETL
functions, while ExternalTaskSensor coordinates dependencies between
processing layers. PostgresHook provides PostgreSQL connectivity and is
used for transactional Gold processing.

The data transformation logic is primarily implemented through SQL files
covering Bronze extraction, Silver cleansing and validation, SCD Type 2
dimension processing, and Gold fact construction.

The selected architecture is appropriate for the implemented workload
because the platform is primarily a scheduled, batch-oriented relational
data warehouse pipeline. The combination of Airflow, Python, and
PostgreSQL provides the required orchestration, transformation,
dependency, transaction, and auditing capabilities without introducing
an additional flow-based orchestration platform.

**Therefore, Apache Airflow was selected as the primary orchestration
platform for the ABC Hub data warehouse, complemented by Python for ETL
control logic and PostgreSQL SQL for data transformation and analytical
processing.**

------------------------------------------------------------------------

## 24. Troubleshooting

### Airflow cannot find the DAG

Confirm that the project `dags/` directory is included in the Airflow
DAGs folder/configuration.

Check:

``` bash
airflow dags list
```

The expected DAG IDs are:

``` text
01_full_load_bronze
02_delta_load_bronze
03_silver_load_layer
04_gold_dim_load_layer
05_gold_fact_load_layer
```

### PostgreSQL connection failure

Check:

1.  PostgreSQL is running.
2.  The Airflow connection ID is correct.
3.  Host and port are reachable from WSL.
4.  Username and password are correct.
5.  The target/source database exists.
6.  PostgreSQL authentication permits the connection.

### Sensor remains waiting

Check that:

1.  The upstream DAG has been triggered for the corresponding logical
    date.
2.  The expected upstream task completed successfully.
3.  The external DAG ID and task ID match the implementation.
4.  The upstream run and downstream run use compatible logical dates.

### Gold fact loading failure

Check the Airflow task logs and PostgreSQL state. Gold fact loading uses
a transaction and rolls back the database transaction when an exception
occurs.

------------------------------------------------------------------------

## 25. Recommended Execution Validation

After setup, validate the pipeline in stages rather than immediately
executing the complete workflow.

1.  Confirm Airflow starts successfully.
2.  Confirm both PostgreSQL Airflow connections work.
3.  Confirm the five DAGs appear in Airflow.
4.  Execute the initial Bronze full load.
5.  Verify Bronze row counts.
6.  Execute the Bronze delta load.
7.  Verify the watermark and extracted delta counts.
8.  Execute the Silver DAG.
9.  Verify Silver row counts and rejected records where applicable.
10. Execute the Gold dimension DAG.
11. Validate SCD Type 2 current/historical versions.
12. Execute the Gold fact DAG.
13. Validate fact row counts and grain.
14. Query `audit.etl_audit`.
15. Re-run an unchanged stage and compare database state to validate
    re-runnability and idempotent behaviour.

------------------------------------------------------------------------

## 26. Security

Database credentials should not be committed to source control.

The project `.gitignore` excludes:

``` text
.env
```

and other local/runtime files.

Use Airflow's connection management facilities for database credentials
rather than embedding passwords directly in DAGs or SQL files.
