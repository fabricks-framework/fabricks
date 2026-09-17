# Databricks notebook source
from databricks.sdk.runtime import dbutils, spark

from fabricks.metastore.view import View

# COMMAND ----------

# Invoker._invoke_notebook always passes schema_only when called from
# Gold.get_data() -- dbutils.notebook.run() needs a matching widget declared
# here, unlike pre_run/post_run/timeout notebooks which never receive it.
dbutils.widgets.text("schema_only", "false")

# COMMAND ----------

df = spark.sql("select * from gold.dim_time")
uuid = View.create_or_replace(df)

# COMMAND ----------

dbutils.notebook.exit(uuid)  # type: ignore
