# Fabricks

Fabricks is a Python framework developed to help create a lake house in Databricks. It simplifies the process of building and maintaining data pipelines by providing a standardized approach to defining and managing data processing workflows.

Though Fabricks is currently really meant to be run on Databricks, the code using Fabricks is really portable - you'll almost exclusively
write SQL-Select Code - no need to manually write DDL/DML/Merge queries. Later on we might add support for other platforms as well, eg. DuckDB or Open Source Spark.



## Features

- YAML configuration files: Fabricks uses YAML files for configuration, making it easy to define and modify workflows without requiring significant changes to the code.
- SQL files for business logic: Business logic is defined in SQL files, providing a familiar and powerful tool for data processing.
- Version control: Fabricks supports version control, ensuring that changes are tracked and can be rolled back if necessary.
- Seamless integration of new data sources: Fabricks can easily integrate new data sources into existing workflows.
- Change Data Capture: Fabricks supports Change Data Capture, allowing it to track and handle changes in the data over time.
- Drop and create: Fabricks can drop and create tables as needed, providing flexibility in managing the data schema.

## Getting Started

To get started with Fabricks, you'll need to install it and set up your first project. Here's a basic guide on how to do that:

### Installation

To install Fabricks, you need to install the library on your Databricks cluster. Follow the steps below:

1. Navigate to your Databricks workspace.
2. Select the cluster where you want to install the library.
3. Click on the `Libraries` tab.
4. Click on `Install New`.
5. Choose `PyPI` from the library source dropdown.
6. Enter `fabricks` in the package text box.
7. Click `Install`.

After the library is installed, you can import it in your notebooks or scripts using `import fabricks`.

### Setting Up Your First Project

# Fabricks Runtime Configuration

The Fabricks runtime configuration is defined in a YAML file. This file specifies the settings for the Fabricks runtime, including options for the runtime environment, path options, Spark options, and the configuration for different stages of the data pipeline (bronze, silver, gold, etc.).

## Configuration Options

- `name`: The name of the configuration.
- `options`: General options for the runtime. This includes:
  - `secret_scope`: The name of the secret scope in Databricks.
  - `timeout`: The timeout for the runtime in seconds.
  - `workers`: The number of workers for the runtime.
- `path_options`: Options for the storage path. This includes:
  - `storage`: The storage path for the data.
- `spark_options`: Options for Spark. This includes:
  - `sql`: SQL options for Spark.

## Data Pipeline Stages

The configuration file defines the settings for different stages of the data pipeline:

- `bronze`: The initial stage of the data pipeline. This includes:
  - `name`: The name of the stage.
  - `path_options`: Options for the storage path.
  - `options`: Options for the stage.
- `silver`: The intermediate stage of the data pipeline. This includes:
  - `name`: The name of the stage.
  - `path_options`: Options for the storage path.
  - `options`: Options for the stage.
- `gold`: The final stage of the data pipeline. This includes:
  - `name`: The name of the stage.
  - `path_options`: Options for the storage path.
  - `options`: Options for the stage.

## Other Configurations

- `powerbi`: Configuration for PowerBI integration.
- `databases`: Configuration for the databases.
- `credentials`: Credentials for accessing different resources.
- `variables`: Variables used in the configuration.

Please note that this is a basic documentation based on the provided YAML file. The actual configuration options may vary depending on the specific requirements of your project.

# Bronze Step Configuration

The "bronze" step in Fabricks is the initial stage of the data pipeline. It is responsible for ingesting raw data and storing it in a "bronze" table for further processing. The configuration for the "bronze" step is defined in a YAML file. Each job in the "bronze" step has the following configuration options:

- `step`: The step in the data pipeline. For these jobs, it is always "bronze".
- `topic`: The topic of the job. This is usually the name of the data source.
- `item`: The item of the job. This is usually the name of the specific data item being processed.
- `tags`: Tags for the job. These can be used to categorize or filter jobs.
- `options`: Options for the job. This includes:
  - `mode`: The mode of the job. This can be "append", "memory", or "register".
  - `uri`: The URI of the data source. This is usually an Azure Blob File System (ABFS) URI.
  - `parser`: The parser to use for the data. This can be "monarch", "parquet", etc.
  - `keys`: The keys for the data. These are the columns that uniquely identify each row in the data.
  - `source`: The source of the data. This is usually the same as the topic.
  - `extender`: The extender for the data. This is used to extend the data with additional columns or transformations.
  - `encrypted_columns`: The columns in the data that are encrypted. These columns will be decrypted during the "bronze" step.
  - `calculated_columns`: The columns in the data that are calculated. These columns will be calculated during the "bronze" step.

Here's an example of a "bronze" step job:

```yaml
- job:
    step: bronze
    topic: king
    item: scd1
    tags: [test]
    options:
      mode: append
      uri: abfss://fabricks@$datahub/raw/king
      parser: monarch
      keys: [id]
      source: king
```

# Silver Step Configuration

The "silver" step in Fabricks is the intermediate stage of the data pipeline. It is responsible for processing the raw data ingested in the "bronze" step and storing it in a "silver" table for further processing. The configuration for the "silver" step is defined in a YAML file. Each job in the "silver" step has the following configuration options:

- `step`: The step in the data pipeline. For these jobs, it is always "silver".
- `topic`: The topic of the job. This is usually the name of the data source.
- `item`: The item of the job. This is usually the name of the specific data item being processed.
- `tags`: Tags for the job. These can be used to categorize or filter jobs.
- `options`: Options for the job. This includes:
  - `mode`: The mode of the job. This can be "update", "memory", "latest", "append", "combine", etc.
  - `change_data_capture`: The type of Change Data Capture (CDC) to use. This can be "scd1", "scd2", "nocdc", etc.
  - `parents`: The parent jobs that this job depends on. These are usually "bronze" step jobs.
  - `extender`: The extender for the data. This is used to extend the data with additional columns or transformations.
  - `order_duplicate_by`: The order to use when removing duplicates. This can be "asc" or "desc".
  - `check_options`: Options for checking the data. This includes:
    - `max_rows`: The maximum number of rows to check.
  - `stream`: Whether to stream the data. This can be "true" or "false".

Here's an example of a "silver" step job:

```yaml
- job:
    step: silver
    topic: king_and_queen
    item: scd1
    tags: [test]
    options:
      mode: update
      change_data_capture: scd1
      parents: [bronze.queen_scd1, bronze.king_scd1]
```
# Gold Step Configuration

The "gold" step in Fabricks is the final stage of the data pipeline. It is responsible for processing the data from the "silver" step and storing it in a "gold" table for consumption. The configuration for the "gold" step is defined in a YAML file. Each job in the "gold" step has the following configuration options:

- `step`: The step in the data pipeline. For these jobs, it is always "gold".
- `topic`: The topic of the job. This is usually the name of the data source.
- `item`: The item of the job. This is usually the name of the specific data item being processed.
- `tags`: Tags for the job. These can be used to categorize or filter jobs.
- `options`: Options for the job. This includes:
  - `mode`: The mode of the job. This can be "complete", "memory", etc.
  - `change_data_capture`: The type of Change Data Capture (CDC) to use. This can be "scd1", "scd2", "nocdc", etc.

Here's an example of a "gold" step job:

```yaml
- job:
    step: gold
    topic: scd2
    item: complete
    tags: [test]
    options:
      change_data_capture: scd2
      mode: complete
````

## Usage

// Instructions on how to use the framework go here


## License

This project is licensed under the terms of the MIT license.
