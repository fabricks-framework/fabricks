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

Define your **Fabricks** runtime in a YAML file. Here's a basic structure:

```yaml
name: MyFabricksProject
options:
  secret_scope: my_secret_scope
  timeout: 3600
  workers: 4
path_options:
  storage: /mnt/data
spark_options:
  sql:
    option1: value1
    option2: value2

# Pipeline Stages
bronze:
  name: Bronze Stage
  path_options:
    storage: /mnt/bronze
  options:
    option1: value1

silver:
  name: Silver Stage
  path_options:
    storage: /mnt/silver
  options:
    option1: value1

gold:
  name: Gold Stage
  path_options:
    storage: /mnt/gold
  options:
    option1: value1
```

## 🥉 Bronze Step

The initial stage for raw data ingestion:

```yaml
- job:
    step: bronze
    topic: sales_data
    item: daily_transactions
    tags: [raw, sales]
    options:
      mode: append
      uri: abfss://fabricks@$datahub/raw/sales
      parser: parquet
      keys: [transaction_id]
      source: pos_system
```

## 🥈 Silver Step

The intermediate stage for data processing:

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

## 🥇 Gold Step

The final stage for data consumption:

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

## 📚 Usage

// Detailed usage instructions to be added here

## 📄 License

This project is licensed under the MIT License.

---

For more information, visit [**Fabricks** Documentation](https://fabricks.readthedocs.io) 📚
