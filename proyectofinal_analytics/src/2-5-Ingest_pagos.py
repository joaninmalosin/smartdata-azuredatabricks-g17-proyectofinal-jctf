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
container_raw = dbutils.widgets.get("container_raw")
catalog = dbutils.widgets.get("catalog")
schema_bronze = dbutils.widgets.get("schema_bronze")
schema_silver = dbutils.widgets.get("schema_silver")
schema_golden = dbutils.widgets.get("schema_golden")
storageName = dbutils.widgets.get("storageName")


spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_bronze}")
spark.sql(f"USE SCHEMA {schema_bronze}")

ruta = f"abfss://{container_raw}@{storageName}.dfs.core.windows.net/IngestProyectofinalSmartDataG17/pagos.csv"

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

df_pago_final.write.mode("overwrite").insertInto(f"{catalog}.{schema_bronze}.pagos")