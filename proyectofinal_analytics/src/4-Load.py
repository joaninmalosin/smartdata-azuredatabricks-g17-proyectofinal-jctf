# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

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
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_golden}")
spark.sql(f"USE SCHEMA {schema_silver}")

spark.sql(f"USE SCHEMA {schema_golden}")


# COMMAND ----------

df_reservas = spark.table(f"{catalog}.{schema_silver}.reservas_enriquecidas")
#df_reservas.display()

# COMMAND ----------

df_clientes = (df_reservas
    .filter(df_reservas["cliente_id"].isNotNull())
    .select(
        "cliente_id",
        "nombre_completo",
        "edad",
        "ciudad_cliente",
        "estado_cliente",
        "pais_cliente",
        "email",
        "segmento_cliente"
    )
    .distinct()
)
#df_clientes.display()

# COMMAND ----------

df_clientes.write.mode("overwrite").insertInto(f"{catalog}.{schema_golden}.dim_cliente")

# COMMAND ----------

df_destino = (df_reservas
              .filter(df_reservas["destino_codigo"].isNotNull())
              .select("destino_codigo", "destino")
              .distinct())
#df_destino.display()

# COMMAND ----------

df_destino.write.mode("overwrite").insertInto(f"{catalog}.{schema_golden}.dim_destino")

# COMMAND ----------

df_hotel = (df_reservas
                .filter(df_reservas["destino_codigo"].isNotNull())
                .select(
                    "hotel_id",
                    "nombre_hotel",
                    "destino_codigo",
                    "destino",
                    "categoria_estrellas",
                    "regimen",
                    "precio_noche",
                    "calificacion_hotel",
                    "nivel_calificacion_hotel"
                )
              .distinct())
#df_hotel.display()
"""
df_hotel = (df_hotel_silver
    .filter(df_hotel_silver["hotel_id"].isNotNull())
    .select(
        "hotel_id",
        "nombre_hotel",
        "destino_codigo",
        "destino",
        "categoria_estrellas",
        "regimen",
        "precio_noche",
         col("calificacion").alias("calificacion_hotel"),
         col("nivel_calificacion").alias("nivel_calificacion_hotel")
    )
    .distinct()
)
df_hotel.display()
"""


# COMMAND ----------

df_hotel.write.mode("overwrite").insertInto(f"{catalog}.{schema_golden}.dim_hotel")

# COMMAND ----------

dt_vuelos = (
    df_reservas.filter ( "vuelo_id IS NOT NULL" )
        .select(
            "vuelo_id",
            "aerolinea",
            "origen",
            "destino_codigo",
            "fecha_salida_vuelo",
            "precio_vuelo",
            "equipaje",
            "duracion_minutos",
            "ruta",
            "jornada"
        ).distinct()
    )


# COMMAND ----------

dt_vuelos.write.mode("overwrite").insertInto(f"{catalog}.{schema_golden}.dim_vuelo")

# COMMAND ----------

limites = ( df_reservas.filter ( "fecha_reserva IS NOT NULL AND fecha_regreso IS NOT NULL" )
                .select(
                    F.min("fecha_reserva").cast("date").alias("min_fecha"),
                    F.max("fecha_regreso").cast("date").alias("max_fecha")
                )
                .collect()[0]
)
min_fecha = limites["min_fecha"]
max_fecha = limites["max_fecha"]
print(min_fecha)
print(max_fecha)

# 2. Generar la secuencia de fechas (Equivalente al CTE 'fechas')
# Usamos lit() para pasar las variables locales como columnas de tipo fecha
df_fechas = spark.range(1).select(
    F.explode(
        F.sequence(F.lit(min_fecha), F.lit(max_fecha), F.expr("INTERVAL 1 DAY"))
    ).alias("fecha")
)

# 3. Construir la dimensión de tiempo con las transformaciones requeridas
df_dim_fecha = df_fechas.select(
    F.date_format("fecha", "yyyyMMdd").cast("int").alias("fecha_key"),
    F.col("fecha"),
    F.year("fecha").alias("anio"),
    F.quarter("fecha").alias("trimestre"),
    F.month("fecha").alias("mes"),
    F.date_format("fecha", "MMMM").alias("nombre_mes"),
    F.weekofyear("fecha").alias("semana"),
    F.dayofmonth("fecha").alias("dia"),
    F.dayofweek("fecha").alias("dia_semana"),
    F.date_format("fecha", "EEEE").alias("nombre_dia"),
    F.when(F.dayofweek("fecha").isin(1, 7), True).otherwise(False).alias("es_fin_de_semana")
)
df_dim_fecha.display()


# COMMAND ----------

df_dim_fecha.write.mode("overwrite").insertInto(f"{catalog}.{schema_golden}.dim_fecha")

# COMMAND ----------

df_fact_reservas = df_reservas.select(
    F.col("id_reserva"),
    F.date_format(F.col("fecha_reserva"), "yyyyMMdd").cast(IntegerType()).alias("fecha_reserva_key"),
    F.date_format(F.col("fecha_salida"), "yyyyMMdd").cast(IntegerType()).alias("fecha_salida_key"),
    F.col("cliente_id"),
    F.col("vuelo_id"),
    F.col("hotel_id"),
    F.col("destino_codigo"),
    F.col("tipo_reserva"),
    F.col("num_personas"),
    F.col("descuento_pct"),
    F.col("monto_total"),
    F.col("monto_por_persona"),
    F.col("moneda"),
    F.col("estado_reserva"),
    F.col("numero_noches"),
    F.col("precio_vuelo"),
    F.col("precio_noche"),
    F.col("duracion_minutos"),
    F.col("calificacion_hotel"),
    F.col("monto_pago"),
    F.col("estado_pago")
)
df_fact_reservas.display()
"""
  id_reserva          string,
    fecha_reserva_key   integer,
    fecha_salida_key    integer,
    cliente_id          string,
    vuelo_id            string,
    hotel_id            string,
    destino_codigo      string,
    tipo_reserva        string,
    num_personas        integer,
    descuento_pct       decimal(5,2),
    monto_total         decimal(18,2),
    monto_por_persona   decimal(18,2),
    moneda              string,
    estado_reserva      string,
    numero_noches       integer,
    precio_vuelo        decimal(18,2),
    precio_noche        decimal(18,2),
    duracion_minutos    integer,
    calificacion_hotel  decimal(3,1),
    monto_pago          decimal(18,2),
    estado_pago         string
    """

# COMMAND ----------

df_fact_reservas.write.mode("overwrite").insertInto(f"{catalog}.{schema_golden}.fact_reservas")

# COMMAND ----------

df_kpi_ventas_mensuales = (
    df_reservas.groupBy(
        F.year("fecha_reserva").alias("anio"),
        F.month("fecha_reserva").alias("mes"),
        F.date_format("fecha_reserva", "MMMM").alias("nombre_mes")
    )
    .agg(
        F.count_distinct("id_reserva").alias("total_reservas"),
        F.sum("num_personas").alias("total_viajeros"),
        F.sum("monto_total").alias("ventas_totales"),
        F.avg("monto_total").alias("ticket_promedio"),
        F.avg("monto_por_persona").alias("gasto_promedio_persona"),
        F.sum("monto_pago").alias("pagos_recibidos")
    )
)
#df_kpi_ventas_mensuales.display()

# COMMAND ----------

df_kpi_ventas_mensuales.write.mode("overwrite").insertInto(f"{catalog}.{schema_golden}.kpi_ventas_mensuales")

# COMMAND ----------

df_kpi_destinos = df_reservas.groupBy("destino_codigo", "destino").agg(
    F.count_distinct("id_reserva").alias("total_reservas"),
    F.sum("num_personas").alias("total_viajeros"),
    F.sum("monto_total").alias("ventas_totales"),
    F.avg("monto_total").alias("ticket_promedio"),
    F.avg("monto_por_persona").alias("gasto_promedio_persona"),
    F.avg("numero_noches").alias("promedio_noches"),
    F.avg("descuento_pct").alias("descuento_promedio")
)
#df_kpi_destinos.display()

# COMMAND ----------

df_kpi_destinos.write.mode("overwrite").insertInto(f"{catalog}.{schema_golden}.kpi_destinos")

# COMMAND ----------

df_kpi_clientes = df_reservas.groupBy("cliente_id").agg(
    F.max("nombre_completo").alias("nombre_completo"),
    F.max("segmento_cliente").alias("segmento_cliente"),
    F.max("ciudad_cliente").alias("ciudad_cliente"),
    F.count_distinct("id_reserva").alias("total_reservas"),
    F.sum("num_personas").alias("total_viajeros"),
    F.sum("monto_total").alias("gasto_total"),
    F.avg("monto_total").alias("ticket_promedio"),
    F.min("fecha_reserva").alias("primera_reserva"),
    F.max("fecha_reserva").alias("ultima_reserva")
)
#df_kpi_clientes.display()
 

# COMMAND ----------

df_kpi_clientes.write.mode("overwrite").insertInto(f"{catalog}.{schema_golden}.kpi_clientes")

# COMMAND ----------

df_kpi_hoteles = (
    df_reservas
        .filter(
            F.col("hotel_id").isNotNull()
        )
        .groupBy("hotel_id")
        .agg(
            F.max("nombre_hotel").alias("nombre_hotel"),
            F.max("destino").alias("destino"),
            F.max("categoria_estrellas").alias("categoria_estrellas"),
            F.max("regimen").alias("regimen"),
            F.count_distinct("id_reserva").alias("total_reservas"),
            F.sum("num_personas").alias("total_huespedes"),
            F.sum("monto_total").alias("ventas_totales"),
            F.avg("precio_noche").alias("precio_promedio_noche"),
            F.avg("calificacion_hotel").alias("calificacion_promedio"),
            F.avg("numero_noches").alias("promedio_noches")
        )
)
#df_kpi_hoteles.display()


# COMMAND ----------

df_kpi_hoteles.write.mode("overwrite").insertInto(f"{catalog}.{schema_golden}.kpi_hoteles")

# COMMAND ----------

df_kpi_vuelos = (
    df_reservas .filter(
        F.col("vuelo_id").isNotNull()
    )
    .groupBy("aerolinea", "ruta", "jornada")
    .agg(
        F.count_distinct("id_reserva").alias("reservas"),
        F.sum("num_personas").alias("pasajeros"),
        F.avg("precio_vuelo").alias("precio_promedio"),
        F.avg("duracion_minutos").alias("duracion_promedio"),
        F.avg("monto_total").alias("ticket_promedio")
    )
)
#df_kpi_vuelos.display() 

# COMMAND ----------

df_kpi_vuelos.write.mode("overwrite").insertInto(f"{catalog}.{schema_golden}.kpi_vuelos")