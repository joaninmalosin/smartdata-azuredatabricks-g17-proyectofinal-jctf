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

ruta = f"abfss://{container}@{storageName}.dfs.core.windows.net/IngestProyectofinalSmartDataG17/reservas.csv"

# COMMAND ----------

reserva_schema = StructType(
                    fields=[
                        StructField("id_reserva", StringType(), False),
                        StructField("id_cliente", StringType(), True),
                        StructField("id_vuelo", StringType(), True),
                        StructField("id_hotel",StringType(), True),
                        StructField("fecha_reserva",TimestampType(), True),
                        StructField("fecha_salida", TimestampType(), True),
                        StructField("fecha_regreso", TimestampType(), True),
                        StructField("destino", StringType(), True),
                        StructField("tipo_reserva", StringType(), True),
                        StructField("num_personas",IntegerType(), True),
                        StructField("descuento_pct", IntegerType(), True),
                        StructField("monto_total",DoubleType(), True),
                        StructField("moneda", StringType(), True),
                        StructField("estado_reserva", StringType(), True)
                    ]
                )


# COMMAND ----------

df_reserva_read = spark.read.option('header', True)\
                    .schema(reserva_schema)\
                    .csv(ruta)

# COMMAND ----------

df_reserva_final = df_reserva_read.select(
                        "id_reserva",
                        "id_cliente",
                        "id_vuelo",
                        "id_hotel",
                        "fecha_reserva",
                        "fecha_salida",
                        "fecha_regreso",
                        "destino",
                        "tipo_reserva",
                        "num_personas",
                        "descuento_pct",
                        "monto_total",
                        "moneda",
                        "estado_reserva"
                    )

# COMMAND ----------

df_reserva_final.write.mode("overwrite").insertInto(f"{catalogo}.{esquema}.reservas")