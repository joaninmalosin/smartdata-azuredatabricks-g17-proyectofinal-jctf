# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
dbutils.widgets.removeAll()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text("container_raw", "raw")
dbutils.widgets.text("container_metastore", "metastore")
dbutils.widgets.text("catalog", "catalog_dev")
dbutils.widgets.text("schema_bronze", "bronze")
dbutils.widgets.text("schema_silver", "silver")
dbutils.widgets.text("schema_golden", "golden")
dbutils.widgets.text("storageName", "adlsproyectofinaljctfd01")


# COMMAND ----------

container_raw = dbutils.widgets.get("container_raw")
catalog = dbutils.widgets.get("catalog")
schema_bronze = dbutils.widgets.get("schema_bronze")
schema_silver = dbutils.widgets.get("schema_silver")
schema_golden = dbutils.widgets.get("schema_golden")
storageName = dbutils.widgets.get("storageName")


spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_bronze}")
spark.sql(f"USE SCHEMA {schema_bronze}")

ruta = f"abfss://{container_raw}@{storageName}.dfs.core.windows.net/IngestProyectofinalSmartDataG17/vuelos.csv"

# COMMAND ----------

vuelo_schema = StructType(
                    fields=[
                        StructField("vuelo_id", StringType(), False),
                        StructField("aerolinea", StringType(), True),
                        StructField("origen", StringType(), True),
                        StructField("destino_codigo",StringType(), True),
                        StructField("destino", StringType(), True),
                        StructField("pais_destino", StringType(), True),
                        StructField("fecha_salida", TimestampType(), True),
                        StructField("hora_salida", StringType(), True),
                        StructField("fecha_llegada", TimestampType(), True),
                        StructField("hora_llegada", StringType(), True),
                        StructField("precio", DoubleType(), True),
                        StructField("equipaje", StringType(), True),
                        StructField("estatus", StringType(), True)
                    ]
                )

# COMMAND ----------

df_vuelo_read = spark.read.option('header', True)\
                    .schema(vuelo_schema)\
                    .csv(ruta)

# COMMAND ----------

df_vuelo_final = df_vuelo_read.select(
                    "vuelo_id",
                    "aerolinea",
                    "origen",
                    "destino_codigo",
                    "destino",
                    "pais_destino",
                    "fecha_salida",
                    "hora_salida",
                    "fecha_llegada",
                    "hora_llegada",
                    "precio",
                    "equipaje",
                    "estatus"
                )

# COMMAND ----------

df_vuelo_final.write.mode("overwrite").insertInto(f"{catalog}.{schema_bronze}.vuelos")