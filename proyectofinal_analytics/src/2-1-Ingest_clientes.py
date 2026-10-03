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

df_cliente_final.write.mode("overwrite").insertInto(f"{catalogo}.{esquema}.clientes")