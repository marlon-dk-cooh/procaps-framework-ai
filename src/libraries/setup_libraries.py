# Databricks notebook source
dbutils.fs.mkdirs("dbfs:/FileStore/libs")

# COMMAND ----------

dbutils.fs.cp(
    "file:/Workspace/Users/iadataknow@auteco.com/bundles/indexing/files/src/libraries/adls_medallion_uploader.py",
    "dbfs:/FileStore/libs/adls_medallion_uploader.py",
)
