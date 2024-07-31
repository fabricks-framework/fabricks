# Welcome to Fabricks Framework!

[![PyPI version](https://badge.fury.io/py/fabricks.svg)](https://pypi.org/project/fabricks/)

Fabricks is a Python framework developed to help create a lake house in Databricks. It simplifies the process of building and maintaining data pipelines by providing a standardized approach to defining and managing data processing workflows.

Though Fabricks is currently really meant to be run on Databricks, the code using Fabricks is really portable - you'll almost exclusively
write SQL-Select Code - no need to manually write DDL/DML/Merge queries. Later on we might add support for other platforms as well, eg. DuckDB or Open Source Spark.

## Use Cases

- Data Ingestion using Python Notebooks, Jupyter-style
- ETL using SQL-queries (should cover 99%) or Notebooks
- Data Distribution using Python Notebooks

No need for magic in here. It's all your Data Lakehouse/Data Warehouse code in one place. Simple and great!

## Basic Wording

- Runtime: The Folder/Repo where you're Lakehouse lives.
- Step: A Layer in the ETL, which should follow some ordering. Commonly used is the medaillon architecture with `bronze`/`silver`/`gold`, but often you'll need some more, eg. we also have `distribution` or `transfer` in our ETL config, it's up to you.
- Job: Often equal to a table, a thing to be executed during a `Schedule`.
- Schedule/Load: Executes a number of jobs, meaning it loads data into tables, and maybe even distributes them

You'll find more documentation in the [Framework Readme](framework/README.md), and even more is to come.

For some basic examples, we currently have to refer to our tests:

- [Jobs Config](framework/tests/runtime/gold/gold/invoke/config.invoke.yml)
  - pre_run is for Notebooks to be run before a job, we use it for ingestion
  - post_run is for Notebooks to be run after a job, we use it for distribution
- [Just some SQL Query for a Job](framework/tests/runtime/gold/gold/fact/dependency.sql)

## About this repo

We're just getting started with open-sourcing Fabricks! There are lot's of areas where we want to improve:

- Testing in Github Pipelines
- Decouple Spark where possible
- Move Yaml Parsing to pydantic

## More infos

See [Framework Readme](framework/README.md)

## Related Projects

- We use [odbc2deltalake](https://github.com/bmsuisse/odbc2deltalake) for lot's of sql server data ingestion in a pre_run notebook
