# Databricks notebook source
print("Test")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "TEST"

# COMMAND ----------

# MAGIC %md
# MAGIC # Test

# COMMAND ----------

# MAGIC %run ./Includes/Setup
# MAGIC

# COMMAND ----------

print(full_name)

# COMMAND ----------

# MAGIC %fs ls databricks-datasets

# COMMAND ----------

dbutils.help()
dbutils.fs.help()
display(dbutils.fs.ls("databricks-datasets"))
