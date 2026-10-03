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
container_metastore = dbutils.widgets.get("container_metastore")
catalog = dbutils.widgets.get("catalog")
schema_bronze = dbutils.widgets.get("schema_bronze")
schema_silver = dbutils.widgets.get("schema_silver")
schema_golden = dbutils.widgets.get("schema_golden")
storageName = dbutils.widgets.get("storageName")


ruta = f"abfss://{container_raw}@{storageName}.dfs.core.windows.net/IngestProyectofinalSmartDataG17/hoteles.csv"

# COMMAND ----------

hotel_schema = StructType(
                    fields = [
                        StructField("hotel_id", StringType(), False),
                        StructField("nombre_hotel", StringType(), True),
                        StructField("destino", StringType(), True),
                        StructField("destino_codigo",StringType(), True),
                        StructField("pais", StringType(), True),
                        StructField("categoria_estrellas", IntegerType(), True),
                        StructField("regimen", StringType(), True),
                        StructField("habitaciones", IntegerType(), True),
                        StructField("precio_noche", DoubleType(), True),
                        StructField("calificacion", DoubleType(), True)
                    ]
                )

# COMMAND ----------

df_hotel_read = spark.read.option('header', True)\
                .schema(hotel_schema)\
                .csv(ruta)

# COMMAND ----------

df_hotel_final = df_hotel_read.select(
                    "hotel_id",
                    "nombre_hotel",
                    "destino",
                    "destino_codigo",
                    "pais",
                    "categoria_estrellas",
                    "regimen",
                    "habitaciones",
                    "precio_noche",
                    "calificacion"
                )

# COMMAND ----------

df_hotel_final.write.mode("overwrite").insertInto(f"{catalog}.{schema_bronze}.hoteles")