# Welcome to Fabricks Framework!

Fabricks is a Python framework developed to help create a lake house in Databricks. It simplifies the process of building and maintaining data pipelines by providing a standardized approach to defining and managing data processing workflows.

Though Fabricks is currently really meant to be run on Databricks, the code using Fabricks is really portable - you'll almost exclusively
write SQL-Select Code - no need to manually write DDL/DML/Merge queries. Later on we might add support for other platforms as well, eg. DuckDB or Open Source Spark.

## About this repo

We're just getting started with open-sourcing Fabricks! There are lot's of areas where we want to improve:

- Testing in Github Pipelines
- Decouple Spark where possible
- Move Yaml Parsing to pydantic

## More infos

See [Framework Readme](framework/README.md)