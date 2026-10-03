# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text("container", "raw")
dbutils.widgets.text("catalogo", "catalog_dev")
dbutils.widgets.text("esquema", "bronze")
dbutils.widgets.text("storageName", "adlsproyectofinaljctfd01")

# COMMAND ----------

container = dbutils.widgets.get("container")
catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")
storageName = dbutils.widgets.get("storageName")

ruta = f"abfss://{container}@{storageName}.dfs.core.windows.net/IngestProyectofinalSmartDataG17/vuelos.csv"

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

df_vuelo_final.write.mode("overwrite").insertInto(f"{catalogo}.{esquema}.vuelos")