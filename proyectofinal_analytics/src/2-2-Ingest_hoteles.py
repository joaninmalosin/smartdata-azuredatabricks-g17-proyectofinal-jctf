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

ruta = f"abfss://{container}@{storageName}.dfs.core.windows.net/IngestProyectofinalSmartDataG17/hoteles.csv"

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

df_hotel_final.write.mode("overwrite").insertInto(f"{catalogo}.{esquema}.hoteles")