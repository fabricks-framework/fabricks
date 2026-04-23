# Welcome to Fabricks 🏗️🧱
## Framework for Databricks 

[![PyPI version](https://badge.fury.io/py/fabricks.svg)](https://pypi.org/project/fabricks/)

Fabricks is a Python framework developed to help create a Lakehouse in **Databricks**. It simplifies the process of building and maintaining data pipelines by providing a standardized approach to defining and managing data processing workflows. Fabricks is battle-proven, used in production environments running thousands of jobs. 💪🚀

Currently, Fabricks is based on Azure **Databricks** and runs on Azure, utilizing Azure Blob Storage, Azure Table Storage, and Azure Queue Storage. Porting it to AWS or Google Cloud should not be a significant challenge. ☁️🔄

Although Fabricks is primarily designed to run on **Databricks**, the code using Fabricks is highly portable. You'll predominantly write SQL-Select code, eliminating the need to manually write DDL/DML/Merge queries. In the future, we may add support for other platforms such as DuckDB or Open Source Spark. 🐍📊

## Use Cases 🛠️
- Data Ingestion using Python Notebooks, Jupyter-style
- ETL using SQL-queries (should cover 99% of cases) or Notebooks
- Data Distribution using Python Notebooks

No need for magic here. It's all your Data Lakehouse/Data Warehouse code in one place. Simple and great! ✨ You don't need expensive Delta Live Tables, ETL Tools, or DBT. It's basically just writing SQL Queries and letting Fabricks do the magic 🧙‍♂️. 

## About this repo 🕵️‍♂️
We're just getting started with open-sourcing Fabricks! There are many areas where we want to improve:
- Implement testing in GitHub Actions 🧪👨‍💻
- Decouple Spark dependencies where possible ⚡🔓
- Migrate YAML parsing to Pydantic 📄🔄
- Enhance documentation with more examples and best practices 📚💡
- Develop a comprehensive getting started guide 🚀📘
- Create a contribution guide for the open-source community 🤝🌐

## More Information ℹ️
See [Fabricks Documentation](https://fabricks-framework.github.io/fabricks/)

### Release Notes

For the latest releases and detailed changelogs, please visit the [Fabricks Releases page on GitHub](https://github.com/fabricks-framework/fabricks/releases).

### Runtime Requirements

[✔] `Fabricks 4.0.0` was successfully tested on Databricks Runtime `16.4 LTS`.

[✔] `Fabricks 4.0.19` was successfully tested on Databricks Runtime `17.3 LTS` with the following dependencies:

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
    "pydantic-settings==2.14.0",
    "python-dotenv==1.2.2",
    "sparkdantic==2.8.0",
    "sqlglot==30.6.0",
    "tenacity==9.0.0",
    "tomli==2.0.1",
    "tqdm==4.67.3",
]
```

> [!WARNING]
> The `sqlglot[c]` extra (C-based parser with Cython optimizations) cannot be used with Fabricks.

## Related Projects 🔗
- We use [odbc2deltalake](https://github.com/bmsuisse/odbc2deltalake) for extensive SQL Server data ingestion in a pre_run notebook. 🔌🏊‍♂️
