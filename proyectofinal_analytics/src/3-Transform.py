# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalogo", "catalog_dev")
dbutils.widgets.text("esquema_bronze", "bronze")
dbutils.widgets.text("esquema_silver", "silver")

# COMMAND ----------


catalogo = dbutils.widgets.get("catalogo")
esquema_bronze = dbutils.widgets.get("esquema_bronze")
esquema_silver = dbutils.widgets.get("esquema_silver")

# COMMAND ----------

df_clientes = spark.table(f"{catalogo}.{esquema_bronze}.clientes")
     

# COMMAND ----------

# Definimos los caracteres a sustituir
CON_ACENTO = "áéíóúüñÁÉÍÓÚÜÑ"
SIN_ACENTO = "aeiouuNAEIOUUN"
# 2. Aplicar las transformaciones de limpieza y lógica de negocio
df_clientes_silver = df_clientes.select(
    F.trim(F.col("cliente_id")).alias("cliente_id"),
    # Concatenación para el nombre completo
    F.concat( F.initcap(F.trim(F.col("nombre"))), F.lit(" "), F.initcap(F.trim(F.col("apellido"))) ).alias("nombre_completo"),
    # Validación de edad
    F.when(F.col("edad").between(0, 120), F.col("edad")).otherwise(None).alias("edad"),
    F.initcap(F.trim(F.col("ciudad"))).alias("ciudad"),
    F.initcap(F.trim(F.col("estado"))).alias("estado"),
    F.initcap(F.trim(F.col("pais"))).alias("pais"),
    #F.lower(F.trim(F.col("email"))).alias("email"),
    F.lower(F.translate(F.trim(F.col("email")), CON_ACENTO, SIN_ACENTO)).alias("email"),
    F.initcap(F.trim(F.col("segmento_cliente"))).alias("segmento_cliente"),
    F.col("fecha_registro"),
    # Lógica de calidad del registro (CASE WHEN)
    F.when
        (
         F.col("cliente_id").isNull(), "cliente_id isNull"
        )
        .when(F.col("email").isNull(), "email isNull")
        .when(~F.lower(F.translate(F.trim(F.col("email")), CON_ACENTO, SIN_ACENTO)).rlike("^[\w\.-]+@[\w\.-]+\.\w+$"), "REVISAR email")
        .when(~F.col("edad").between(0, 120), "ERROR edad")
        .otherwise("OK")
    .alias("calidad_registro")
)

# COMMAND ----------

df_clientes_silver.write.mode("overwrite").insertInto(f"{catalogo}.{esquema_silver}.clientes")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

df_hoteles = spark.table(f"{catalogo}.{esquema_bronze}.hoteles")
#df_hoteles.display()


# COMMAND ----------

df_hoteles_silver = df_hoteles.select(
    F.trim(F.col("hotel_id")).alias("hotel_id"),
    F.trim(F.col("nombre_hotel")).alias("nombre_hotel"),
    F.initcap(F.trim(F.col("destino"))).alias("destino"),
    F.upper(F.trim(F.col("destino_codigo"))).alias("destino_codigo"),
    F.initcap(F.trim(F.col("pais"))).alias("pais"),
    F.when(
        F.col("categoria_estrellas").between(1, 5), F.col("categoria_estrellas")
    )    .otherwise(None) .alias("categoria_estrellas"),
    F.initcap(F.trim(F.col("regimen"))).alias("regimen"),
    F.when(F.col("habitaciones") >= 0, F.col("habitaciones")) .otherwise(None) .alias("habitaciones"),
    F.when(F.col("precio_noche") >= 0, F.col("precio_noche")) .otherwise(None) .alias("precio_noche"),
    F.when(F.col("calificacion").between(0, 5), F.col("calificacion")) .otherwise(None) .alias("calificacion"),
    F.when(F.col("calificacion") >= 4.5, "Excelente")
        .when(F.col("calificacion") >= 4.0, "Muy buena")
        .when(F.col("calificacion") >= 3.0, "Buena")
        .when(F.col("calificacion").isNotNull(), "Regular")
        .otherwise("Sin calificación")
        .alias("nivel_calificacion")
)
#df_hoteles_silver.display()

# COMMAND ----------

df_hoteles_silver.write.mode("overwrite").insertInto(f"{catalogo}.{esquema_silver}.hoteles")

# COMMAND ----------

df_vuelos = spark.table(f"{catalogo}.{esquema_bronze}.vuelos")

# COMMAND ----------

df_vuelos_silver = df_vuelos.select(
    F.trim(F.col("vuelo_id")).alias("vuelo_id"),
    F.initcap(F.trim(F.col("aerolinea"))).alias("aerolinea"),
    F.upper(F.trim(F.col("origen"))).alias("origen"),
    F.upper(F.trim(F.col("destino_codigo"))).alias("destino_codigo"),
    F.initcap(F.trim(F.col("destino"))).alias("destino"),
    F.initcap(F.trim(F.col("pais_destino"))).alias("pais_destino"),
    F.col("fecha_salida"),
    F.trim(col("hora_salida")).alias("hora_salida"),
    F.col("fecha_llegada"),
    F.trim(F.col("hora_llegada")).alias("hora_llegada"),
    F.col("precio"),
    F.initcap(F.trim(F.col("equipaje"))).alias("equipaje"),
    F.initcap(F.trim(F.col("estatus"))).alias("estatus"),
    F.concat(
        F.upper(F.trim(F.col("origen"))),
        F.lit("-"),
        F.upper(F.trim(F.col("destino_codigo")))
    ).alias("ruta"),
    F.round(
        (F.unix_timestamp(F.col("fecha_llegada")) - 
         F.unix_timestamp(F.col("fecha_salida"))) / F.lit(60)
    ).alias("duracion_minutos"),
    F.when(F.col("precio") > 0, F.lit("Con precio"))
     .otherwise(F.lit("Sin precio"))
     .alias("calidad_precio"),
     F.when(F.hour(F.col("hora_salida")).between(6, 11), "Diurno")
     .when(F.hour(F.col("hora_salida")).between(12, 18), "Vespertino")
     .otherwise("Nocturno").alias("jornada")
)
 

# COMMAND ----------

df_vuelos_silver.write.mode("overwrite").insertInto(f"{catalogo}.{esquema_silver}.vuelos")

# COMMAND ----------

df_reservas = spark.table(f"{catalogo}.{esquema_bronze}.reservas")


# COMMAND ----------

df_reservas_silver = df_reservas.select(
    F.trim(F.col("id_reserva")).alias("id_reserva"),
    F.trim(F.col("id_cliente")).alias("id_cliente"),
    F.trim(F.col("id_vuelo")).alias("id_vuelo"),
    F.trim(F.col("id_hotel")).alias("id_hotel"),
    F.col("fecha_reserva"),
    F.col("fecha_salida"),
    F.col("fecha_regreso"),
    F.initcap(F.trim(F.col("destino"))).alias("destino"),
    F.initcap(F.trim(F.col("tipo_reserva"))).alias("tipo_reserva"),
    
    # CASE WHEN num_personas > 0 ...
    F.when(F.col("num_personas") > 0, F.col("num_personas"))
     .otherwise(F.lit(None))
     .alias("num_personas"),
    
    # CASE WHEN descuento_pct BETWEEN 0 AND 100 ...
    F.when(F.col("descuento_pct").between(0, 100), F.col("descuento_pct"))
     .otherwise(F.lit(0))
     .alias("descuento_pct"),
    
    # CASE WHEN monto_total >= 0 ...
    F.when(F.col("monto_total") >= 0, F.col("monto_total"))
     .otherwise(F.lit(None))
     .alias("monto_total"),
    
    F.upper(F.trim(F.col("moneda"))).alias("moneda"),
    F.initcap(F.trim(F.col("estado_reserva"))).alias("estado_reserva"),
    
    # DATEDIFF con TO_DATE
    F.datediff(
        F.to_date(F.col("fecha_regreso")), 
        F.to_date(F.col("fecha_salida"))
    ).alias("numero_noches"),
    
    # CASE WHEN num_personas > 0 THEN ROUND(monto_total / num_personas, 2) ...
    F.when(F.col("num_personas") > 0, F.round(F.col("monto_total") / F.col("num_personas"), 2))
     .otherwise(F.lit(None))
     .alias("monto_por_persona"),
    
    # CASE WHEN fecha_regreso >= fecha_salida ...
    F.when(F.col("fecha_regreso") >= F.col("fecha_salida"), F.lit("OK"))
     .otherwise(F.lit("ERROR"))
     .alias("validacion_fechas")
)

# COMMAND ----------

df_reservas_silver.write.mode("overwrite").insertInto(f"{catalogo}.{esquema_silver}.reservas")

# COMMAND ----------

df_pagos = spark.table(f"{catalogo}.{esquema_bronze}.pagos")

# COMMAND ----------

df_pagos_silver = df_pagos.select(
    F.trim(F.col("id_pago")).alias("id_pago"),
    F.trim(F.col("id_reserva")).alias("id_reserva"),
    F.trim(F.col("id_cliente")).alias("id_cliente"),
    F.col("fecha_pago"),
    
    # CASE WHEN monto_pago >= 0 THEN monto_pago ELSE NULL END
    F.when(F.col("monto_pago") >= 0, F.col("monto_pago"))
     .otherwise(None)
     .alias("monto_pago"),
    
    F.upper(F.trim(F.col("moneda"))).alias("moneda"),
    F.initcap(F.trim(F.col("metodo_pago"))).alias("metodo_pago"),
    F.initcap(F.trim(F.col("tipo_pago"))).alias("tipo_pago"),
    F.initcap(F.trim(F.col("estado_pago"))).alias("estado_pago"),
    F.trim(F.col("referencia_pago")).alias("referencia_pago"),
    
    # CASE WHEN monto_pago > 0 THEN 'OK' ELSE 'REVISAR' END
    F.when(F.col("monto_pago") > 0, "OK")
     .otherwise("REVISAR")
     .alias("calidad_pago")
)

# COMMAND ----------

df_pagos_silver.write.mode("overwrite").insertInto(f"{catalogo}.{esquema_silver}.pagos")

# COMMAND ----------

#df_reservas_enriquecidas = spark.table(f"{catalogo}.{esquema_silver}.reservas_enriquecidas")

# COMMAND ----------

# 1. Cargar las tablas origen desde el catálogo
r = spark.read.table(f"{catalogo}.{esquema_silver}.reservas")
c = spark.read.table(f"{catalogo}.{esquema_silver}.clientes")
v = spark.read.table(f"{catalogo}.{esquema_silver}.vuelos")
h = spark.read.table(f"{catalogo}.{esquema_silver}.hoteles")
p = spark.read.table(f"{catalogo}.{esquema_silver}.pagos")

# 2. Realizar los LEFT JOINs secuenciales
df_join = r \
    .join(c, r["id_cliente"] == c["cliente_id"], "left") \
    .join(v, r["id_vuelo"] == v["vuelo_id"], "left") \
    .join(h, r["id_hotel"] == h["hotel_id"], "left") \
    .join(p, r["id_reserva"] == p["id_reserva"], "left")

# 3. Seleccionar y renombrar las columnas requeridas
df_silver_reservas_enrr = df_join.select(
    # RESERVA
    r["id_reserva"],
    r["fecha_reserva"],
    r["fecha_salida"],
    r["fecha_regreso"],
    r["destino"],
    r["tipo_reserva"],
    r["num_personas"],
    r["numero_noches"],
    r["descuento_pct"],
    r["monto_total"],
    r["moneda"],
    r["estado_reserva"],
    r["monto_por_persona"],

    # CLIENTE
    c["cliente_id"],
    c["nombre_completo"],
    c["edad"],
    c["ciudad"].alias("ciudad_cliente"),
    c["estado"].alias("estado_cliente"),
    c["pais"].alias("pais_cliente"),
    c["email"],
    c["segmento_cliente"],

    # VUELO
    v["vuelo_id"],
    v["aerolinea"],
    v["origen"],
    v["destino_codigo"],
    v["fecha_salida"].alias("fecha_salida_vuelo"),
    v["precio"].alias("precio_vuelo"),
    v["equipaje"],
    v["duracion_minutos"],
    v["ruta"],
    v["jornada"],

    # HOTEL
    h["hotel_id"],
    h["nombre_hotel"],
    h["categoria_estrellas"],
    h["regimen"],
    h["precio_noche"],
    h["calificacion"].alias("calificacion_hotel"),
    h["nivel_calificacion"].alias("nivel_calificacion_hotel"),

    # PAGO
    p["id_pago"],
    p["fecha_pago"],
    p["monto_pago"],
    p["metodo_pago"],
    p["tipo_pago"],
    p["estado_pago"]
)
#df_silver_reservas_enrr.display()


# COMMAND ----------

df_silver_reservas_enrr.write.mode("overwrite").insertInto(f"{catalogo}.{esquema_silver}.reservas_enriquecidas")