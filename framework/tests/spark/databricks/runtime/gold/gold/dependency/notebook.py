# Databricks notebook source
from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession

# COMMAND ----------

source = SparkSession.builder.getOrCreate().table("gold.dim_time")
source.createOrReplaceGlobalTempView("gold_dependency_notebook")
dbutils.notebook.exit("gold_dependency_notebook")  # type: ignore
