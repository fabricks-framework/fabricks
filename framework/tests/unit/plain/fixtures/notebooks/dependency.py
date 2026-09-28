# Databricks notebook source
# MAGIC %md
# MAGIC Databricks source-format mirror of dependency.ipynb, for the ".py sibling exists ->
# MAGIC skip static parse" precedence fixture (see test_notebook_dependencies_wiring.py). This
# MAGIC file is never itself statically parsed -- Databricks source-format cell markers
# MAGIC (`# COMMAND ----------`, `# MAGIC`) are intentionally out of scope for this parser (see
# MAGIC https://github.com/fabricks-framework/fabricks/issues/103#issuecomment-2999181785); a
# MAGIC ".py" notebook always falls back to execution-based dependency resolution.

# COMMAND ----------

spark.sql("select * from gold.dim_time")

# COMMAND ----------

query = "select * from silver.customer_scd2__current"
spark.sql(query)

# COMMAND ----------

spark.table("gold.fact_sales")

# COMMAND ----------

spark.read.table("silver.orders")
spark.read.format("delta").table("silver.orders_v2")

# COMMAND ----------

spark.readStream.table("bronze.events")
spark.readStream.format("delta").table("bronze.events_v2")

# COMMAND ----------

from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "gold.dim_customer")

# COMMAND ----------

target_table = "silver.product"
dt2 = DeltaTable.forName(spark, target_table)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.dim_region

# COMMAND ----------

# df9 = spark.sql("select * from gold.should_not_appear")

# COMMAND ----------

# df10_old = spark.sql("select * from gold.old_table")  -- dead, must not appear
df10 = spark.sql("select * from gold.new_table")

# COMMAND ----------

# MAGIC %pip install some-package

# COMMAND ----------

# MAGIC %sh echo hello

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

spark.sql("""
insert into gold.summary select * from silver.staging;
merge into gold.summary_v2 using silver.staging_v2 s on true when matched then update set *;
""")

# COMMAND ----------

df13 = spark.sql("select * from identifier({tbl})", tbl="gold.dim_time")

# COMMAND ----------

df14 = spark.sql("with cte as (select 1 as x) select * from cte")

# COMMAND ----------

# OUT OF SCOPE: f-string / dynamic SQL construction
table_name = "gold.dynamic_target"
df15 = spark.sql(f"select * from {table_name}")

# COMMAND ----------


# OUT OF SCOPE: helper-function-returned table name
def get_table_name():
    return "gold.helper_table"


df16 = spark.table(get_table_name())

# COMMAND ----------

df17 = spark.read.load("/mnt/raw/some_file.parquet", format="parquet")

# COMMAND ----------

df18 = spark.sql("select * from external_system.raw_feed")
