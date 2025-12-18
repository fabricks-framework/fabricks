# Fabricks Job Options Documentation

> **Comprehensive guide to configuring Bronze, Silver, and Gold tier jobs in Fabricks**

This document provides detailed documentation for all job configuration options in Fabricks. Jobs are organized into three tiers: Bronze, Silver, and Gold, each with their own specific options and behaviors.

---

## 📑 Table of Contents

- [📋 Overview](#-overview)
- [🏗️ Job Tiers](#️-job-tiers)
- [⚙️ Common Options](#️-common-options)
- [🥉 Bronze Options](#-bronze-options)
- [🥈 Silver Options](#-silver-options)
- [🥇 Gold Options](#-gold-options)
- [📊 Table Options](#-table-options)
- [✅ Check Options](#-check-options)
- [⚡ Spark Options](#-spark-options)
- [🔗 Invoker Options](#-invoker-options)
- [🔌 Extender Options](#-extender-options)
- [📝 Complete Configuration Examples](#-complete-configuration-examples)
- [💡 Key Concepts](#-key-concepts)
- [🎯 Best Practices](#-best-practices)

---

## 📋 Overview

Fabricks uses a **tiered data processing architecture** with three layers:

| Tier | Purpose | Description |
|------|---------|-------------|
| **🥉 Bronze** | Raw Data Ingestion | Ingest raw data from external sources with minimal transformation |
| **🥈 Silver** | Data Cleaning & Validation | Clean, validate, deduplicate, and apply quality checks |
| **🥇 Gold** | Business Analytics | Create business-ready aggregated and transformed data |

Each tier has specific configuration options that control how data is processed, stored, and managed.

---

## 🏗️ Job Tiers

### Available Tiers

```python
TBronze = Literal["bronze"]  # Raw data ingestion layer
TSilver = Literal["silver"]  # Cleaned and validated data layer
TGold = Literal["gold"]      # Business-ready analytics layer
```

---

## ⚙️ Common Options

> These options are available across **all tiers** (Bronze, Silver, and Gold).

<table>
<tr>
<td width="200"><strong>Option</strong></td>
<td><strong>Description</strong></td>
</tr>

<tr>
<td valign="top">

### `type`

**Type:** `Optional[Literal["manual", "default"]]`
**Default:** `"default"`

</td>
<td>

Specifies the job execution type:
- `"default"` - Standard automated job processing
- `"manual"` - Manual intervention required, job skips automatic execution

</td>
</tr>

<tr>
<td valign="top">

### `parents`

**Type:** `Optional[List[str]]`
**Default:** `None`

</td>
<td>

List of parent job names that this job depends on. The job will only execute after all parent jobs have completed successfully.

**Example:**
```python
parents=["bronze__customers", "bronze__orders"]
```

</td>
</tr>

<tr>
<td valign="top">

### `filter_where`

**Type:** `Optional[str]`
**Default:** `None`

</td>
<td>

SQL WHERE clause to filter data during processing. Applied to the source data before any transformations.

**Example:**
```python
filter_where="status = 'active' AND created_date >= '2024-01-01'"
```

</td>
</tr>

<tr>
<td valign="top">

### `optimize`

**Type:** `Optional[bool]`
**Default:** `False`

</td>
<td>

When `True`, runs the OPTIMIZE command on the target table after data loading to improve query performance by compacting small files.

</td>
</tr>

<tr>
<td valign="top">

### `compute_statistics`

**Type:** `Optional[bool]`
**Default:** `False`

</td>
<td>

When `True`, computes table statistics after data loading to help the query optimizer make better decisions.

</td>
</tr>

<tr>
<td valign="top">

### `vacuum`

**Type:** `Optional[bool]`
**Default:** `False`

</td>
<td>

When `True`, runs the VACUUM command to remove old data files that are no longer referenced by the table (typically files older than the retention period).

</td>
</tr>

<tr>
<td valign="top">

### `no_drop`

**Type:** `Optional[bool]`
**Default:** `False`

</td>
<td>

When `True`, prevents the table from being dropped during job execution, even if the job configuration would normally trigger a drop operation.

</td>
</tr>

<tr>
<td valign="top">

### `timeout`

**Type:** `Optional[int]`
**Default:** `None`

</td>
<td>

Maximum execution time in seconds for the job. If the job exceeds this time, it will be terminated.

</td>
</tr>

</table>

---

## 🥉 Bronze Options

> **Bronze tier**: Raw data ingestion from external sources

Bronze tier is responsible for ingesting raw data from external sources with minimal transformation. It focuses on capturing data exactly as it arrives.

### 🔧 Bronze-Specific Options

<details open>
<summary><h4>📌 <code>mode</code> (Required)</h4></summary>

**Type:** `Literal["memory", "append", "register"]`

Defines how data is loaded into the bronze table:

| Mode | Description | Use Case |
|------|-------------|----------|
| `"memory"` | Load data into memory only, don't persist | Temporary or test data |
| `"append"` | Append new data without checking duplicates | Raw data ingestion |
| `"register"` | Register external table without moving data | External data sources |

</details>

<details open>
<summary><h4>📌 <code>uri</code> (Required)</h4></summary>

**Type:** `str`

URI or path to the source data. Supports multiple formats:

- **File path**: `"/mnt/data/customers/*.json"`
- **URL**: `"https://api.example.com/data"`
- **Cloud storage**: `"s3://bucket/path/to/data"`
- **Database connection string**

**Example:**
```python
uri="/mnt/raw/customers/2024/*.parquet"
```

</details>

<details open>
<summary><h4>📌 <code>parser</code> (Required)</h4></summary>

**Type:** `str`

Name of the parser to use for reading and transforming the source data. Parsers define how to interpret the source format.

**Example:**
```python
parser="json_customer_parser"
```

</details>

<details open>
<summary><h4>📌 <code>source</code> (Required)</h4></summary>

**Type:** `str`

Name or identifier of the source system providing the data.

**Example:**
```python
source="salesforce_api"
```

</details>

<details open>
<summary><h4>📌 <code>keys</code></h4></summary>

**Type:** `Optional[List[str]]` | **Default:** `None`

List of column names that uniquely identify a record. Used for deduplication and change tracking.

**Example:**
```python
keys=["customer_id"]
```

</details>

<details open>
<summary><h4>📌 <code>encrypted_columns</code></h4></summary>

**Type:** `Optional[List[str]]` | **Default:** `None`

List of column names containing encrypted data that should be decrypted during ingestion.

**Example:**
```python
encrypted_columns=["ssn", "credit_card"]
```

</details>

<details open>
<summary><h4>📌 <code>calculated_columns</code></h4></summary>

**Type:** `Optional[dict[str, str]]` | **Default:** `None`

Dictionary mapping new column names to SQL expressions for calculating derived values.

**Example:**
```python
calculated_columns={
    "full_name": "concat(first_name, ' ', last_name)",
    "age": "year(current_date()) - year(birth_date)"
}
```

</details>

<details open>
<summary><h4>📌 <code>operation</code></h4></summary>

**Type:** `Optional[Literal["upsert", "reload", "delete"]]` | **Default:** `None`

Specifies the operation type for CDC (Change Data Capture):

| Operation | Description |
|-----------|-------------|
| `"upsert"` | Insert new records or update existing ones |
| `"reload"` | Full reload of all data (marks timestamp for downstream) |
| `"delete"` | Mark records as deleted |

</details>

### 📦 Complete Bronze Configuration Example

<details>
<summary>Click to expand full Bronze configuration example</summary>

```python
BronzeOptions(
    # Required fields
    mode="append",
    uri="/mnt/data/customers/*.json",
    parser="customer_json_parser",
    source="crm_system",

    # Identification
    keys=["customer_id"],

    # Common options
    type="default",
    parents=["bronze__raw_sources"],
    filter_where="status IS NOT NULL",
    optimize=True,
    compute_statistics=True,
    vacuum=False,
    no_drop=False,
    timeout=3600,

    # Bronze-specific
    encrypted_columns=["ssn"],
    calculated_columns={"full_name": "concat(first_name, ' ', last_name)"},
    operation="upsert"
)
```

</details>

---

## 🥈 Silver Options

> **Silver tier**: Cleaned and validated data with quality checks and change tracking

Silver tier processes bronze data with quality checks, deduplication, and change data capture strategies.

### 🔧 Silver-Specific Options

<details open>
<summary><h4>📌 <code>mode</code> (Required)</h4></summary>

**Type:** `Literal["memory", "append", "latest", "update", "combine"]`

Defines how data is processed and loaded:

| Mode | Description | Use Case |
|------|-------------|----------|
| `"memory"` | Process in memory only without persisting | Testing transformations |
| `"append"` | Append all new data without checking existing | Accumulate all data |
| `"latest"` | Process only the most recent data | Latest snapshot processing |
| `"update"` | Incremental updates, only new/changed records | Efficient incremental loads |
| `"combine"` | Combine multiple sources into single table | Multi-source consolidation |

</details>

<details open>
<summary><h4>📌 <code>change_data_capture</code> (Required)</h4></summary>

**Type:** `Literal["nocdc", "scd1", "scd2"]`

Change Data Capture strategy for tracking changes:

| Strategy | Description | History Tracking |
|----------|-------------|------------------|
| `"nocdc"` | No change tracking, simple updates/inserts | ❌ None |
| `"scd1"` | Slowly Changing Dimension Type 1 - Overwrite | ⚠️ Current state only |
| `"scd2"` | Slowly Changing Dimension Type 2 - Versioning | ✅ Full history |

</details>

<details open>
<summary><h4>📌 <code>deduplicate</code></h4></summary>

**Type:** `Optional[bool]` | **Default:** `False`

When `True`, removes duplicate records based on keys and hash values. Keeps the most recent record for each key.

</details>

<details open>
<summary><h4>📌 <code>stream</code></h4></summary>

**Type:** `Optional[bool]` | **Default:** `False`

When `True`, processes data using Spark Structured Streaming for real-time data processing.

</details>

<details open>
<summary><h4>📌 <code>order_duplicate_by</code></h4></summary>

**Type:** `Optional[dict[str, str]]` | **Default:** `None`

Dictionary specifying columns and sort order for determining which duplicate record to keep.

**Example:**
```python
order_duplicate_by={
    "updated_at": "desc",
    "priority": "desc"
}
```

</details>

### 📦 Complete Silver Configuration Example

<details>
<summary>Click to expand full Silver configuration example</summary>

```python
SilverOptions(
    # Required fields
    mode="update",
    change_data_capture="scd2",

    # Common options
    type="default",
    parents=["bronze__customers"],
    filter_where="quality_score > 0.8",
    optimize=True,
    compute_statistics=True,
    vacuum=True,
    no_drop=False,
    timeout=7200,

    # Silver-specific
    deduplicate=True,
    stream=False,
    order_duplicate_by={"updated_at": "desc"}
)
```

</details>

---

## 🥇 Gold Options

> **Gold tier**: Business-ready analytics with aggregations and ML enrichment

Gold tier creates business-ready data with aggregations, transformations, and analytics-optimized structures.

### 🔧 Gold-Specific Options

#### mode (Required)
**Type:** `Literal["memory", "append", "complete", "update", "invoke"]`

Defines how data is processed and loaded:

- **`"memory"`**: Process in memory only
- **`"append"`**: Append new calculated results
- **`"complete"`**: Complete rebuild of the entire table
- **`"update"`**: Incremental updates to existing data
- **`"invoke"`**: Execute external notebook or process

#### change_data_capture (Required)
**Type:** `Literal["nocdc", "scd1", "scd2"]`

Same as Silver tier - defines the CDC strategy.

#### update_where
**Type:** `Optional[str]`
**Default:** `None`

SQL WHERE clause to filter which records should be updated. Only applicable in `"update"` mode.

**Example:**
```python
update_where="last_modified >= current_date() - INTERVAL 7 DAYS"
```

#### deduplicate
**Type:** `Optional[bool]`
**Default:** `False`

Remove duplicates based on keys and hash values.

#### rectify_as_upserts
**Type:** `Optional[bool]`
**Default:** `False`

When `True`, converts reload operations into individual upsert and delete operations. This ensures historical consistency by generating synthetic delete records for items that disappear between reloads.

**Use Case:** Handle scenarios where records are deleted between full reloads without explicit delete operations.

#### correct_valid_from
**Type:** `Optional[bool]`
**Default:** `False`

When `True` and using SCD2, sets the `__valid_from` timestamp of the earliest record to `'1900-01-01'` instead of the actual first timestamp.

**Use Case:** Standardize historical record start dates for reporting purposes.

#### persist_last_timestamp
**Type:** `Optional[bool]`
**Default:** `False`

When `True`, persists the maximum timestamp from the processed data to be used as a watermark for the next incremental run.

#### persist_last_updated_timestamp
**Type:** `Optional[bool]`
**Default:** `False`

When `True`, persists the maximum `__last_updated` timestamp for incremental processing tracking.

#### table
**Type:** `Optional[str]`
**Default:** `None`

Override the default target table name with a custom table name.

**Example:**
```python
table="custom_schema.custom_table_name"
```

#### notebook
**Type:** `Optional[bool]`
**Default:** `False`

When `True`, generates a notebook for this job that can be executed independently.

#### requirements
**Type:** `Optional[bool]`
**Default:** `False`

When `True`, generates a requirements file for dependencies needed by this job.

#### metadata
**Type:** `Optional[bool]`
**Default:** `False`

When `True`, adds or updates metadata tracking columns (`__metadata.inserted`, `__metadata.updated`).

#### last_updated
**Type:** `Optional[bool]`
**Default:** `False`

When `True`, adds or updates the `__last_updated` timestamp column.

### Complete Gold Configuration

```python
GoldOptions(
    type="default",
    mode="update",
    change_data_capture="scd2",
    update_where="updated_at > (SELECT max(last_run) FROM control_table)",
    parents=["silver__customers", "silver__orders"],
    optimize=True,
    compute_statistics=True,
    vacuum=True,
    no_drop=False,
    deduplicate=True,
    rectify_as_upserts=True,
    correct_valid_from=True,
    persist_last_timestamp=True,
    persist_last_updated_timestamp=True,
    table="analytics.customer_360",
    notebook=False,
    requirements=False,
    timeout=10800,
    metadata=True,
    last_updated=True
)
```

---

## Table Options

Table options control the physical table structure and optimization features.

### identity
**Type:** `Optional[bool]`
**Default:** `None`

When `True`, adds an auto-incrementing identity column to the table.

### liquid_clustering
**Type:** `Optional[bool]`
**Default:** `None`

When `True`, enables Databricks liquid clustering for improved query performance with automatic optimization.

### partition_by
**Type:** `Optional[List[str]]`
**Default:** `None`

List of columns to partition the table by. Partitioning physically organizes data for faster queries on partition columns.

**Example:**
```python
partition_by=["year", "month"]
```

**Use Case:** Date-based partitioning for time-series data.

### zorder_by
**Type:** `Optional[List[str]]`
**Default:** `None`

List of columns to Z-order (multi-dimensional clustering). Improves query performance for columns frequently used in filters.

**Example:**
```python
zorder_by=["customer_id", "product_id"]
```

### cluster_by
**Type:** `Optional[List[str]]`
**Default:** `None`

List of columns to cluster by. Alternative to liquid clustering for organizing data.

**Example:**
```python
cluster_by=["region", "category"]
```

### powerbi
**Type:** `Optional[bool]`
**Default:** `None`

When `True`, optimizes table settings for Power BI connectivity and performance.

### maximum_compatibility
**Type:** `Optional[bool]`
**Default:** `None`

When `True`, creates tables with maximum compatibility settings for older Spark/Delta versions.

### bloomfilter_by
**Type:** `Optional[List[str]]`
**Default:** `None`

List of columns to create bloom filters on for faster equality lookups.

**Example:**
```python
bloomfilter_by=["email", "phone_number"]
```

### constraints
**Type:** `Optional[dict[str, str]]`
**Default:** `None`

Dictionary mapping constraint names to SQL constraint expressions.

**Example:**
```python
constraints={
    "valid_email": "email LIKE '%@%.%'",
    "positive_amount": "amount > 0"
}
```

### properties
**Type:** `Optional[dict[str, str]]`
**Default:** `None`

Dictionary of custom table properties as key-value pairs.

**Example:**
```python
properties={
    "owner": "data_team",
    "data_classification": "confidential"
}
```

### comment
**Type:** `Optional[str]`
**Default:** `None`

Description or comment for the table.

**Example:**
```python
comment="Customer master data with full history tracking"
```

### calculated_columns
**Type:** `Optional[dict[str, str]]`
**Default:** `None`

Dictionary mapping column names to SQL expressions for computed/generated columns.

**Example:**
```python
calculated_columns={
    "full_address": "concat(street, ', ', city, ', ', state)",
    "is_premium": "CASE WHEN tier = 'premium' THEN true ELSE false END"
}
```

### masks
**Type:** `Optional[dict[str, str]]`
**Default:** `None`

Dictionary mapping column names to masking expressions for data privacy/security.

**Example:**
```python
masks={
    "ssn": "concat('***-**-', right(ssn, 4))",
    "credit_card": "concat('****-****-****-', right(credit_card, 4))"
}
```

### comments
**Type:** `Optional[dict[str, str]]`
**Default:** `None`

Dictionary mapping column names to their descriptions.

**Example:**
```python
comments={
    "customer_id": "Unique identifier for each customer",
    "lifetime_value": "Total revenue generated by customer"
}
```

### retention_days
**Type:** `Optional[int]`
**Default:** `None`

Number of days to retain old versions of data before they can be vacuumed.

**Example:**
```python
retention_days=90
```

### primary_key
**Type:** `Optional[dict[str, PrimaryKey]]`
**Default:** `None`

Dictionary defining primary key constraint with name and configuration.

**Example:**
```python
primary_key={
    "pk_customers": {
        "keys": ["customer_id"],
        "options": {"constraint": "not enforced"}
    }
}
```

### foreign_keys
**Type:** `Optional[dict[str, ForeignKey]]`
**Default:** `None`

Dictionary defining foreign key constraints with names and configurations.

**Example:**
```python
foreign_keys={
    "fk_orders_customer": {
        "keys": ["customer_id"],
        "reference": "customers",
        "options": {
            "foreign_key": "on delete no action",
            "constraint": "not enforced"
        }
    }
}
```

### Complete Table Options Example

```python
TableOptions(
    identity=False,
    liquid_clustering=True,
    partition_by=["year", "month"],
    zorder_by=["customer_id"],
    cluster_by=None,
    powerbi=True,
    maximum_compatibility=False,
    bloomfilter_by=["email"],
    constraints={
        "valid_email": "email LIKE '%@%.%'",
        "positive_balance": "balance >= 0"
    },
    properties={
        "owner": "analytics_team",
        "pii": "true"
    },
    comment="Customer dimension table with full SCD2 history",
    calculated_columns={
        "age": "year(current_date()) - year(birth_date)"
    },
    masks={
        "ssn": "concat('***-**-', right(ssn, 4))"
    },
    comments={
        "customer_id": "Primary key - unique customer identifier",
        "balance": "Current account balance"
    },
    retention_days=90,
    primary_key={
        "pk_customer": {
            "keys": ["customer_id"],
            "options": {"constraint": "not enforced"}
        }
    },
    foreign_keys={
        "fk_country": {
            "keys": ["country_code"],
            "reference": "dim_countries",
            "options": {
                "foreign_key": "on delete no action",
                "constraint": "not enforced"
            }
        }
    }
)
```

---

## Check Options

Data quality and validation checks that run before or after job execution.

### skip
**Type:** `Optional[bool]`
**Default:** `False`

When `True`, skips all data quality checks for this job.

### pre_run
**Type:** `Optional[bool]`
**Default:** `False`

When `True`, runs data quality checks before job execution. Job fails if checks don't pass.

### post_run
**Type:** `Optional[bool]`
**Default:** `False`

When `True`, runs data quality checks after job execution. Job fails if checks don't pass.

### min_rows
**Type:** `Optional[int]`
**Default:** `None`

Minimum number of rows expected in the result. Check fails if row count is below this threshold.

**Example:**
```python
min_rows=1000
```

### max_rows
**Type:** `Optional[int]`
**Default:** `None`

Maximum number of rows expected in the result. Check fails if row count exceeds this threshold.

**Example:**
```python
max_rows=1000000
```

### count_must_equal
**Type:** `Optional[str]`
**Default:** `None`

SQL expression or table name to compare row counts. Check fails if counts don't match.

**Example:**
```python
count_must_equal="bronze__source_table"
```

### Complete Check Options Example

```python
CheckOptions(
    skip=False,
    pre_run=True,
    post_run=True,
    min_rows=100,
    max_rows=10000000,
    count_must_equal="bronze__raw_customers"
)
```

---

## Spark Options

Spark configuration for the job execution.

### sql
**Type:** `Optional[dict[str, str]]`
**Default:** `None`

Dictionary of Spark SQL configuration parameters.

**Example:**
```python
sql={
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true"
}
```

### conf
**Type:** `Optional[dict[str, str]]`
**Default:** `None`

Dictionary of general Spark configuration parameters.

**Example:**
```python
conf={
    "spark.executor.memory": "8g",
    "spark.executor.cores": "4",
    "spark.dynamicAllocation.enabled": "true"
}
```

### Complete Spark Options Example

```python
SparkOptions(
    sql={
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.shuffle.partitions": "200",
        "spark.sql.autoBroadcastJoinThreshold": "10485760"
    },
    conf={
        "spark.executor.memory": "8g",
        "spark.executor.cores": "4",
        "spark.executor.instances": "10"
    }
)
```

---

## Invoker Options

Options for invoking external notebooks at different stages of job execution.

### Structure

```python
class _InvokeOptions(TypedDict):
    notebook: str           # Path to notebook to invoke
    timeout: int           # Timeout in seconds
    arguments: Optional[dict[str, str]]  # Arguments to pass to notebook
```

### pre_run
**Type:** `Optional[List[_InvokeOptions]]`
**Default:** `None`

List of notebooks to execute before the main job runs.

### run
**Type:** `Optional[List[_InvokeOptions]]`
**Default:** `None`

List of notebooks to execute as the main job (replaces default job logic).

### post_run
**Type:** `Optional[List[_InvokeOptions]]`
**Default:** `None`

List of notebooks to execute after the main job completes.

### Complete Invoker Options Example

```python
InvokerOptions(
    pre_run=[
        {
            "notebook": "/Notebooks/setup/validate_sources",
            "timeout": 300,
            "arguments": {"check_level": "strict"}
        }
    ],
    run=None,
    post_run=[
        {
            "notebook": "/Notebooks/post/send_notification",
            "timeout": 60,
            "arguments": {
                "recipients": "team@example.com",
                "status": "success"
            }
        },
        {
            "notebook": "/Notebooks/post/update_dashboard",
            "timeout": 120,
            "arguments": {"dashboard_id": "main"}
        }
    ]
)
```

---

## Extender Options

Options for extending job functionality with custom logic.

### extender (Required)
**Type:** `str`

Name or path of the extender class to use.

**Example:**
```python
extender="CustomDataProcessor"
```

### arguments
**Type:** `Optional[dict[str, str]]`
**Default:** `None`

Dictionary of arguments to pass to the extender.

**Example:**
```python
arguments={
    "transformation": "advanced",
    "output_format": "parquet"
}
```

### Complete Extender Options Example

```python
ExtenderOptions(
    extender="ml.FeatureEngineeringExtender",
    arguments={
        "feature_set": "customer_features_v2",
        "normalize": "true",
        "handle_missing": "impute"
    }
)
```

Multiple extenders can be chained:

```python
extender_options=[
    {
        "extender": "DataValidationExtender",
        "arguments": {"strict": "true"}
    },
    {
        "extender": "FeatureEngineeringExtender",
        "arguments": {"feature_set": "v2"}
    },
    {
        "extender": "MLScoringExtender",
        "arguments": {"model": "customer_churn_v3"}
    }
]
```

---

## Complete Configuration Examples

### Bronze Job: Ingest Customer Data

```python
JobConfBronze(
    job_id="bronze__crm_customers",
    topic="customers",
    item="raw_data",
    step="bronze",
    options=BronzeOptions(
        type="default",
        mode="append",
        uri="s3://data-lake/raw/crm/customers/*.json",
        parser="crm_customer_parser",
        source="salesforce_crm",
        keys=["customer_id"],
        parents=None,
        filter_where="status != 'test'",
        optimize=True,
        compute_statistics=True,
        vacuum=False,
        no_drop=False,
        encrypted_columns=["ssn", "credit_card"],
        calculated_columns={
            "full_name": "concat(first_name, ' ', last_name)",
            "account_age_days": "datediff(current_date(), created_date)"
        },
        operation="upsert",
        timeout=3600
    ),
    table_options=TableOptions(
        partition_by=["ingestion_date"],
        comment="Raw customer data from CRM system",
        retention_days=30
    ),
    check_options=CheckOptions(
        skip=False,
        post_run=True,
        min_rows=1
    ),
    spark_options=SparkOptions(
        conf={
            "spark.executor.memory": "4g",
            "spark.executor.cores": "2"
        }
    ),
    tags=["pii", "critical", "daily"],
    comment="Daily customer data ingestion from Salesforce CRM"
)
```

### Silver Job: Clean and Deduplicate Customers

```python
JobConfSilver(
    job_id="silver__customers",
    topic="customers",
    item="cleaned",
    step="silver",
    options=SilverOptions(
        type="default",
        mode="update",
        change_data_capture="scd2",
        parents=["bronze__crm_customers"],
        filter_where="email IS NOT NULL AND email LIKE '%@%.%'",
        optimize=True,
        compute_statistics=True,
        vacuum=True,
        no_drop=False,
        deduplicate=True,
        stream=False,
        order_duplicate_by={"updated_at": "desc", "source_priority": "asc"},
        timeout=7200
    ),
    table_options=TableOptions(
        liquid_clustering=True,
        cluster_by=["customer_id", "country_code"],
        bloomfilter_by=["email", "phone"],
        constraints={
            "valid_email": "email LIKE '%@%.%'",
            "valid_country": "country_code IN ('US', 'CA', 'UK', 'DE')"
        },
        comments={
            "customer_id": "Unique customer identifier",
            "email": "Primary contact email",
            "phone": "Primary phone number"
        },
        comment="Cleaned customer master data with SCD2 history",
        retention_days=90
    ),
    check_options=CheckOptions(
        pre_run=True,
        post_run=True,
        min_rows=1000,
        count_must_equal="bronze__crm_customers"
    ),
    spark_options=SparkOptions(
        sql={
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true"
        }
    ),
    invoker_options=InvokerOptions(
        post_run=[
            {
                "notebook": "/Quality/DataQualityReport",
                "timeout": 300,
                "arguments": {"table": "silver__customers"}
            }
        ]
    ),
    tags=["master_data", "scd2", "pii"],
    comment="Silver layer customer data with deduplication and validation"
)
```

### Gold Job: Customer 360 View

```python
JobConfGold(
    job_id="gold__customer_360",
    topic="analytics",
    item="customer_360",
    step="gold",
    options=GoldOptions(
        type="default",
        mode="update",
        change_data_capture="scd1",
        update_where="__last_updated >= current_date() - INTERVAL 7 DAYS",
        parents=["silver__customers", "silver__orders", "silver__interactions"],
        optimize=True,
        compute_statistics=True,
        vacuum=True,
        no_drop=False,
        deduplicate=True,
        rectify_as_upserts=True,
        correct_valid_from=False,
        persist_last_timestamp=True,
        persist_last_updated_timestamp=True,
        table="analytics.customer_360_view",
        notebook=False,
        requirements=False,
        timeout=10800,
        metadata=True,
        last_updated=True
    ),
    table_options=TableOptions(
        liquid_clustering=True,
        cluster_by=["customer_segment", "region"],
        zorder_by=["customer_id"],
        powerbi=True,
        bloomfilter_by=["customer_id"],
        calculated_columns={
            "customer_lifetime_value": "total_revenue - total_costs",
            "churn_risk_category": """
                CASE
                    WHEN churn_score > 0.8 THEN 'High'
                    WHEN churn_score > 0.5 THEN 'Medium'
                    ELSE 'Low'
                END
            """
        },
        properties={
            "owner": "analytics_team",
            "refresh_frequency": "daily",
            "data_classification": "internal"
        },
        comment="Complete 360-degree view of customer data for analytics and reporting",
        retention_days=180
    ),
    check_options=CheckOptions(
        post_run=True,
        min_rows=100,
        max_rows=100000000
    ),
    spark_options=SparkOptions(
        sql={
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.shuffle.partitions": "400"
        },
        conf={
            "spark.executor.memory": "16g",
            "spark.executor.cores": "8",
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.maxExecutors": "50"
        }
    ),
    invoker_options=InvokerOptions(
        post_run=[
            {
                "notebook": "/Analytics/RefreshDashboards",
                "timeout": 600,
                "arguments": {
                    "dashboard_list": "customer_360,executive_summary"
                }
            },
            {
                "notebook": "/Notifications/SendSuccessEmail",
                "timeout": 60,
                "arguments": {
                    "recipients": "analytics-team@company.com",
                    "job": "customer_360"
                }
            }
        ]
    ),
    extender_options=[
        {
            "extender": "MLScoringExtender",
            "arguments": {
                "model": "customer_churn_model_v2",
                "score_column": "churn_score"
            }
        },
        {
            "extender": "SegmentationExtender",
            "arguments": {
                "algorithm": "kmeans",
                "n_segments": "5"
            }
        }
    ],
    tags=["analytics", "customer", "ml_enriched", "daily"],
    comment="Gold layer customer 360 view with ML scoring and segmentation"
)
```

---

## 💡 Key Concepts

### 🔄 Change Data Capture (CDC) Strategies

Understanding CDC strategies is crucial for choosing the right data tracking approach:

<table>
<tr>
<th width="150">Strategy</th>
<th>Description</th>
<th>History</th>
<th>Use Case</th>
</tr>

<tr>
<td><strong>nocdc</strong></td>
<td>No change tracking. Simple insert/update/delete operations without history.</td>
<td align="center">❌</td>
<td>Lookup tables, configuration data, or data where history isn't needed</td>
</tr>

<tr>
<td><strong>scd1</strong><br/><em>(Type 1)</em></td>
<td>Overwrites existing records with new values. Maintains only current state with flags for deleted records.</td>
<td align="center">⚠️ Current</td>
<td>Master data where only current values matter (current address, current status)</td>
</tr>

<tr>
<td><strong>scd2</strong><br/><em>(Type 2)</em></td>
<td>Maintains complete history with versioning. Each change creates a new version with validity dates.</td>
<td align="center">✅ Full</td>
<td>Data requiring full audit trail (customer history, product changes over time)</td>
</tr>

</table>

### 🎭 Processing Modes by Tier

<table>
<tr>
<th width="120">Tier</th>
<th width="120">Mode</th>
<th>Description</th>
</tr>

<tr>
<td rowspan="3"><strong>🥉 Bronze</strong></td>
<td><code>memory</code></td>
<td>Testing and temporary data</td>
</tr>
<tr>
<td><code>append</code></td>
<td>Raw data ingestion without deduplication</td>
</tr>
<tr>
<td><code>register</code></td>
<td>External table registration</td>
</tr>

<tr>
<td rowspan="5"><strong>🥈 Silver</strong></td>
<td><code>memory</code></td>
<td>Testing transformations</td>
</tr>
<tr>
<td><code>append</code></td>
<td>Accumulate all data</td>
</tr>
<tr>
<td><code>latest</code></td>
<td>Process only latest snapshot</td>
</tr>
<tr>
<td><code>update</code></td>
<td>Incremental processing</td>
</tr>
<tr>
<td><code>combine</code></td>
<td>Merge multiple sources</td>
</tr>

<tr>
<td rowspan="5"><strong>🥇 Gold</strong></td>
<td><code>memory</code></td>
<td>Testing analytics</td>
</tr>
<tr>
<td><code>append</code></td>
<td>Accumulate metrics/aggregations</td>
</tr>
<tr>
<td><code>complete</code></td>
<td>Full rebuild</td>
</tr>
<tr>
<td><code>update</code></td>
<td>Incremental updates</td>
</tr>
<tr>
<td><code>invoke</code></td>
<td>Custom notebook execution</td>
</tr>

</table>

### 🔗 Parent Dependencies

Jobs can depend on other jobs via the `parents` option. The dependency system ensures:

✅ **Correct execution order** - Parents complete before children run
✅ **No circular dependencies** - System validates dependency graph
✅ **Parallel execution** - Independent jobs run concurrently
✅ **Automatic retry** - Failed dependencies trigger retries

**Example:**
```python
# This job waits for both bronze jobs to complete
parents=["bronze__customers", "bronze__orders"]
```

### ✅ Data Quality Checks

Quality checks can run at different stages:

| Stage | Description | Failure Behavior |
|-------|-------------|------------------|
| **pre_run** | Validate input before processing | Job fails before execution |
| **post_run** | Validate output after processing | Job fails after execution |

**Check Types:**
- ✅ **Row counts** - Validate min/max row thresholds
- ✅ **Data equality** - Compare counts with other tables
- ✅ **Custom assertions** - SQL constraints and validations

---

## Best Practices

### 1. Bronze Layer
- Use `mode="append"` for raw data ingestion
- Set `operation="upsert"` or `"reload"` for CDC tracking
- Enable `optimize=True` for large datasets
- Use `encrypted_columns` for sensitive data
- Keep retention short (`retention_days=30`)

### 2. Silver Layer
- Always use CDC strategy (`scd1` or `scd2`)
- Enable `deduplicate=True` to ensure data quality
- Use `mode="update"` for incremental processing
- Set up data quality checks with `check_options`
- Use longer retention (`retention_days=90`)

### 3. Gold Layer
- Choose CDC based on business needs
- Enable `rectify_as_upserts=True` for historical consistency
- Use `persist_last_timestamp=True` for incremental loads
- Enable `metadata=True` and `last_updated=True` for tracking
- Optimize for BI tools with `powerbi=True`
- Use extenders for ML scoring and enrichment

### 4. Performance
- Use `liquid_clustering=True` for modern workloads
- Set `partition_by` for time-based queries
- Use `zorder_by` for high-cardinality filter columns
- Enable `bloomfilter_by` for equality lookups
- Configure appropriate Spark resources via `spark_options`

### 5. Data Governance
- Add comprehensive `comments` for documentation
- Use `constraints` for data validation
- Set `properties` for metadata tracking
- Use `masks` for PII protection
- Tag jobs appropriately with `tags`

---

## Related Documentation

- [CDC Templates Documentation](../cdc/templates/README.md)
- [Parser Options](../parsers/README.md)
- [Metastore Table Documentation](../../metastore/README.md)
