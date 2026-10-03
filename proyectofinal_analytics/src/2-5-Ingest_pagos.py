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

ruta = f"abfss://{container}@{storageName}.dfs.core.windows.net/IngestProyectofinalSmartDataG17/pagos.csv"

# COMMAND ----------

pago_schema = StructType(
                fields=[
                    StructField("id_pago", StringType(), False),
                    StructField("id_reserva", StringType(), True),
                    StructField("id_cliente", StringType(), True),
                    StructField("fecha_pago",TimestampType(), True),
                    StructField("monto_pago", DoubleType(), True),
                    StructField("moneda", StringType(), True),
                    StructField("metodo_pago", StringType(), True),
                    StructField("tipo_pago", StringType(), True),
                    StructField("estado_pago", StringType(), True),
                    StructField("referencia_pago", StringType(), True)
                ]
            )


# COMMAND ----------

df_pago_read = spark.read.option('header', True)\
                .schema(pago_schema)\
                .csv(ruta)

# COMMAND ----------

df_pago_final = df_pago_read.select(
                    "id_pago",
                    "id_reserva",
                    "id_cliente",
                    "fecha_pago",
                    "monto_pago",
                    "moneda",
                    "metodo_pago",
                    "tipo_pago",
                    "estado_pago",
                    "referencia_pago"
                )

# COMMAND ----------

df_pago_final.write.mode("overwrite").insertInto(f"{catalogo}.{esquema}.pagos")