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

dbutils.widgets.text("container", "raw")
dbutils.widgets.text("catalog", "catalog_dev")
dbutils.widgets.text("schema_bronze", "bronze")
dbutils.widgets.text("schema_silver", "silver")
dbutils.widgets.text("schema_golden", "golden")
dbutils.widgets.text("storageName", "adlsproyectofinaljctfd01")

# COMMAND ----------


container = dbutils.widgets.get("container")
catalog = dbutils.widgets.get("catalog")
schema_bronze = dbutils.widgets.get("schema_bronze")
schema_silver = dbutils.widgets.get("schema_silver")
schema_golden = dbutils.widgets.get("schema_golden")
storageName = dbutils.widgets.get("storageName")


spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_bronze}")
spark.sql(f"USE SCHEMA {schema_bronze}")

ruta = f"abfss://{container}@{storageName}.dfs.core.windows.net/IngestProyectofinalSmartDataG17/clientes.csv"

# COMMAND ----------


cliente_schema = StructType (
                    fields=[
                        StructField("cliente_id", StringType(), False),
                        StructField("nombre", StringType(), True),
                        StructField("apellido", StringType(), True),
                        StructField("edad",IntegerType(), True),
                        StructField("ciudad", StringType(), True),
                        StructField("estado", StringType(), True),
                        StructField("pais", StringType(), True),
                        StructField("email", StringType(), True),
                        StructField("segmento_cliente", StringType(), True),
                        StructField("fecha_registro", TimestampType(), True)
                    ]
                )


# COMMAND ----------

df_cliente_read = spark.read.option('header', True)\
                    .schema(cliente_schema)\
                    .csv(ruta)

# COMMAND ----------

df_cliente_final = df_cliente_read.select(
    "cliente_id",
    "nombre",
    "apellido",
    "edad",
    "ciudad",
    "estado",
    "pais",
    "email",
    "segmento_cliente",
    "fecha_registro"
)

# COMMAND ----------

df_cliente_final.write.mode("overwrite").insertInto(f"{catalog}.{schema_bronze}.clientes")