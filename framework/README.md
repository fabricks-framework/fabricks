# 🧱 Fabricks: Simplifying **Databricks** Data Pipelines

**Fabricks** (**F**ramework for **Databricks**) is a Python framework designed to streamline the creation of lakehouses in **Databricks**. It offers a standardized approach to defining and managing data processing workflows, making it easier to build and maintain robust data pipelines.

## 🌟 Key Features

- 📄 **YAML Configuration**: Easy-to-modify workflow definitions
- 🔍 **SQL-Based Business Logic**: Familiar and powerful data processing
- 🔄 **Version Control**: Track changes and roll back when needed
- 🔌 **Seamless Data Source Integration**: Effortlessly add new sources
- 📊 **Change Data Capture (CDC)**: Track and handle data changes over time
- 🔧 **Flexible Schema Management**: Drop and create tables as needed

## 🚀 Getting Started

### 📦 Installation

1. Navigate to your **Databricks** workspace
2. Select your target cluster
3. Click on the `Libraries` tab
4. Choose `Install New`
5. Select `PyPI` as the library source
6. Enter `fabricks` in the package text box
7. Click `Install`

Once installed, import **Fabricks** in your notebooks or scripts:

```python
import fabricks
```

## 🏗️ Project Configuration

### 🔧 Runtime Configuration

Define your **Fabricks** runtime in a YAML file. It's used to define your steps, timeouts, spark options and mostly every basic thing.
The yaml file's location should be configured in your pyproject.toml:

```toml
[tool.fabricks]
runtime = "."
notebooks = "notebooks" # Copy those from fabricks/api/notebooks in this repo
config_file = "fabricks/conf.fabricks.yml"
```

Here's a basic version:

```yaml
name: MyFabricksProject
options:
  secret_scope: my_secret_scope
  timeouts:
    step: 3600
    job: 3600
    pre_run: 3600
    post_run: 3600
  workers: 4
path_options:
  storage: /mnt/data
spark_options:
  sql:
    option1: value1
    option2: value2
    spark.sql.parquet.compression.codec: zstd # ZSTD is just the best one

# Pipeline Stages
bronze:
  - name: bronze
    path_options:
      runtime: src/steps/bronze
      storage: abfss://bronze@youraccount.blob.core.windows.net
    options:
      option1: value1

silver:
  - name: silver
    path_options:
      runtime: src/steps/silver
      storage: abfss://silver@youraccount.blob.core.windows.net
    options:
      option1: value1

gold:
  - name: transf # we want some additional step between silver and gold to do some fancy stuff
    path_options:
      runtime: src/steps/transf
      storage: abfss://transf@youraccount.blob.core.windows.net
    options:
      option1: value1
  - name: gold
    path_options:
      runtime: src/steps/gold
      storage: abfss://gold@youraccount.blob.core.windows.net
    options:
      option1: value1
  - name: powerbi # We want to rename some stuff here, maybe.
    path_options:
      runtime: src/steps/powerbi
      storage: abfss://powerbi@youraccount.blob.core.windows.net
    options:
      option1: value1
```

A more advanced config can be found in the [tests](tests/integration/runtime/fabricks/conf.5589296195699698.yml)

## 🥉 Bronze Step

The initial stage for raw data ingestion.

You usually either want to gather data from existing parquet/json files, 
or directly create a delta file using a pre-run, which we'll show below:

```yaml
- job:
    step: bronze
    topic: sales_data
    item: daily_transactions
    tags: [raw, sales]
    options:
      mode: append
      uri: abfss://fabricks@$datahub/raw/sales # files are saved from some other system here, as parquet
      parser: parquet # we use AutoLoader in Background in such a case
      keys: [transaction_id]
      source: pos_system
- job:
    step: bronze
    topic: apidata
    item: http_api
    tags: [raw]
    options:
      mode: register
      uri: abfss://fabricks@$datahub/raw/http_api # It's a delta file located here
      keys: [transaction_id]
    
    invoker_options: # we could directly write to this delta file using an invoker
      pre_run:
        - notebook: bronze/invokers/http_call
          arguments:
            url: https://shouldideploy.today # just some arbitrary args
```

## 🥈 Silver Step

The intermediate stage for data processing. It will handle scd1/scd2

The resulting table will contain fields according to your scd config.

For scd1, there will be `__is_current` and `__is_deleted`, for scd2 you'll also get `__valid_from` and `__valid_to`.
In addition, a view named `TABLENAME__current` will be created for convenience.

```yaml
- job:
    step: silver
    topic: sales_analytics
    item: daily_summary
    tags: [processed, sales]
    options:
      mode: update
      change_data_capture: scd1
      parents: [bronze.daily_transactions]
      extender: sales_extender
      check_options:
        max_rows: 1000000
```

## 🥇 Gold Step Type

The final stage for data consumption. Usually just sql. There can also be multiple gold steps,
so you could create a transf step, a gold step, a power bi step, all of which are of type "gold".

```yaml
- job:
    step: gold
    topic: sales_reports
    item: monthly_summary
    tags: [report, sales]
    options:
      mode: complete
      change_data_capture: scd2
```

## 📚 Use Cases / Features

### Checks

There are a number of check_options you can use:

```yaml
  
    check_options: 
      min_rows: 10 # Table must have at least 10 rows
      max_rows: 10000 # Table must not have more than 10000 rows
      count_must_equal: fabricks.dummy # Must have exactly the same number of rows as another table, makes sense if you just have joins to add columns and want to avoid duplicates 
      post_run: true # A custom script to be run after the load
      pre_run: true # A custom script to be run before the load
```

For Pre and Post Run, you have to provide a `table_name.pre_run.sql` / `table_name.post_run.sql` script which returns at least these two columns:

- `__message`, an error message that will show up in the logs
- `__action`, which can be either `'fail'` or `'warning'` 

All other columns are ignored. If a table is of type `memory` (meaning it's a view only), then
the logs will contain the error, but nothing else happens. Otherwise the data is restored to the previous version
on failure.


### SCD2 in gold Steps

Fabricks provides an easy way to create an scd2 table containing `__valid_from` and `__valid_to` out of 
a base select that requires three important fields:

- `__key`: A Unique Key, can be of any data type
- `__timestamp`: The date where your record was either changed or deleted
- `__operation`: Must be either `'upsert'` or `'delete'` 

A common pattern is to aggregate a (silver) scd2 table like this:

```sql
with
    -- ask yourself, what you want to aggregate on and which columns should now be part of your new key
    newkey as (select only_offer_id as __key, * except(__key) from offer_and_lines_table d),
    deletes as (
        select only_offer_id as __key, max(__valid_to) + interval 1 second as deleted_date -- we simply delete on last deleted thing. that's a simplification that usually is good enough. 
        -- alternatively, you could delete if you cannot join on a new __valid_from (meaning there is no record afterwards anymore)
        from offer_and_lines_table
        group by only_offer_id
        having max(`__valid_to`) < '9999-12-31'
    ),
    dates as (
        
        select __key, __valid_from as __timestamp, 'upsert' as __operation -- each source valid from is an update
        from newkey
        union
        select __key, deleted_date as __timestamp, 'delete' as __operation 
        from deletes
    )
    select
        d.__key,      
        d.__timestamp,
        d.__operation,
        sum(
            if(
                d.__operation = 'delete' and d.__timestamp = r.__valid_to,
                0,
                sales
            )
        ) as sales,
        sum(
            if(
                d.__operation = 'delete' and d.__timestamp = r.__valid_to,
                0,
                sales_gross
            )
        ) as sales_gross
    from dates d
    left join newkey r on d.__timestamp between r.__valid_from and r.__valid_to and d.only_offer_id = r.only_offer_id
    group by d.only_offer_id, __timestamp, __operation
    
```



## 📄 License

This project is licensed under the MIT License.
