# Welcome to Fabricks 🏗️🧱
## The framework for Databricks 

[![PyPI version](https://badge.fury.io/py/fabricks.svg)](https://pypi.org/project/fabricks/)

Fabricks is a Python framework developed to help create a Lakehouse in **Databricks**. It simplifies the process of building and maintaining data pipelines by providing a standardized approach to defining and managing data processing workflows. Fabricks is battle-proven, used in production environments running thousands of jobs.

Fabricks currently supports Azure **Databricks** and uses Azure Blob Storage, Azure Table Storage, and Azure Queue Storage. AWS and Google Cloud are not supported.

Although Fabricks is primarily designed to run on **Databricks**, the code using Fabricks is highly portable. You'll predominantly write SQL-Select code, eliminating the need to manually write DDL/DML/Merge queries. In the future, we may add support for other platforms such as DuckDB or Open Source Spark.

## Use Cases 🛠️
- Data Ingestion using Python Notebooks, Jupyter-style
- ETL using SQL-queries (should cover 99% of cases) or Notebooks
- Data Distribution using Python Notebooks

No need for magic here. It's all your Data Lakehouse/Data Warehouse code in one place. Simple and great! ✨ You don't need expensive Delta Live Tables, ETL Tools, or DBT. It's basically just writing SQL Queries and letting Fabricks do the magic 🧙‍♂️. 

### Release Notes

For the latest releases and detailed changelogs, please visit the [Fabricks Releases page on GitHub](https://github.com/fabricks-framework/fabricks/releases).

As of Fabricks 4.1, development is agentic: start with `AGENTS.md` and use
the locally installed skills it references.

### Runtime Requirements

[✔] `Fabricks 4.0.0` was successfully tested on Databricks Runtime `16 LTS`.

[✔] `Fabricks 4.1.*` was successfully tested on Databricks Runtime `17 LTS` with the following dependencies:

[❌] `Fabricks 4.1.*` was not successfully tested on Databricks Runtime `18 LTS` for streaming workloads on `USER_ISOLATION` compute.

```yaml
dependencies = [
    "Jinja2==3.1.6",
    "PyYAML==6.0.2",
    "azure-data-tables==12.7.0",
    "azure-identity==1.20.0",
    "azure-storage-blob==12.23.0",
    "azure-storage-queue==12.15.0",
    "databricks-sdk==0.49.0",
    "ipython==8.30.0",
    "mermaid-magic==0.1.4",
    "pandas==2.2.3",
    "pydantic==2.10.6",
    "pydantic-settings==2.14.1",
    "python-dotenv==1.2.2",
    "sparkdantic==2.8.0",
    "sqlglot==30.8.0",
    "tenacity==9.0.0",
    "tomli==2.0.1",
    "tqdm==4.67.3",
]
```

> [!WARNING]
> The `sqlglot[c]` extra (C-based parser with Cython optimizations) cannot be used with Fabricks.

## Related Projects 🔗
- We use [odbc2deltalake](https://github.com/bmsuisse/odbc2deltalake) for extensive SQL Server data ingestion in a pre_run notebook. 🔌🏊‍♂️
