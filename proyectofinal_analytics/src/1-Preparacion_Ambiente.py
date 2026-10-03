# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC #Preparacion del ambiente

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC create widget text storageName default "adlsproyectofinaljctfd01";

# COMMAND ----------

storageName = dbutils.widgets.get("storageName");

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-metastore`
# MAGIC URL 'abfss://metastore@${storageName}.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL `credential`)
# MAGIC COMMENT 'Ubicación externa para las tablas metastore del Data Lake';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-raw`
# MAGIC URL 'abfss://raw@${storageName}.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL `credential`)
# MAGIC COMMENT 'Ubicación externa para las tablas raw del Data Lake';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-bronze`
# MAGIC URL 'abfss://bronze@${storageName}.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL `credential`)
# MAGIC COMMENT 'Ubicación externa para las tablas bronze del Data Lake';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-silver`
# MAGIC URL 'abfss://silver@${storageName}.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL `credential`)
# MAGIC COMMENT 'Ubicación externa para las tablas silver del Data Lake';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-golden`
# MAGIC URL 'abfss://golden@${storageName}.dfs.core.windows.net/'
# MAGIC WITH (STORAGE CREDENTIAL `credential`)
# MAGIC COMMENT 'Ubicación externa para las tablas golden del Data Lake';

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP CATALOG IF EXISTS catalog_dev CASCADE;

# COMMAND ----------

spark.sql(f"""
CREATE CATALOG IF NOT EXISTS `catalog_dev`
MANAGED LOCATION 'abfss://metastore@{storageName}.dfs.core.windows.net/'
COMMENT 'Catalogo para la arquitectura medallion del ambiente de dev'
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS raw CASCADE;
# MAGIC DROP SCHEMA IF EXISTS bronze CASCADE;
# MAGIC DROP SCHEMA IF EXISTS silver CASCADE;
# MAGIC DROP SCHEMA IF EXISTS golden CASCADE;

# COMMAND ----------

dbutils.fs.rm(f"abfss://bronze@{storageName}.dfs.core.windows.net/",True)
dbutils.fs.rm(f"abfss://silver@{storageName}.dfs.core.windows.net/",True)
dbutils.fs.rm(f"abfss://golden@{storageName}.dfs.core.windows.net/",True)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS raw;
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS golden;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Tablas Bronze

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS bronze.clientes (
    cliente_id string,
    nombre string,
    apellido string,
    edad int,
    ciudad string,
    estado string,
    pais string,
    email string,
    segmento_cliente string,
    fecha_registro timestamp
)
USING DELTA
LOCATION "abfss://bronze@{storageName}.dfs.core.windows.net/clientes"
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS bronze.hoteles (
    hotel_id string,
    nombre_hotel string,
    destino string,
    destino_codigo string,
    pais string,
    categoria_estrellas int,
    regimen string,
    habitaciones int,
    precio_noche double,
    calificacion double
)
USING DELTA
LOCATION "abfss://bronze@{storageName}.dfs.core.windows.net/hoteles"
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS bronze.vuelos (
    vuelo_id string,
    aerolinea string,
    origen string,
    destino_codigo string,
    destino string,
    pais_destino string,
    fecha_salida timestamp,
    hora_salida string,
    fecha_llegada timestamp,
    hora_llegada string,
    precio double,
    equipaje string,
    estatus string
)
USING DELTA
LOCATION "abfss://bronze@{storageName}.dfs.core.windows.net/vuelos"
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS bronze.reservas (
    id_reserva string,
    id_cliente string,
    id_vuelo string,
    id_hotel string,
    fecha_reserva timestamp,
    fecha_salida timestamp,
    fecha_regreso timestamp,
    destino string,
    tipo_reserva string,
    num_personas int,
    descuento_pct int,
    monto_total double,
    moneda string,
    estado_reserva string
)
USING DELTA
LOCATION "abfss://bronze@{storageName}.dfs.core.windows.net/reservas"
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS bronze.pagos (
    id_pago string,
    id_reserva string,
    id_cliente string,
    fecha_pago timestamp,
    monto_pago double,
    moneda string,
    metodo_pago string,
    tipo_pago string,
    estado_pago string,
    referencia_pago string
)
USING DELTA
LOCATION "abfss://bronze@{storageName}.dfs.core.windows.net/pagos"
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Tablas Silver

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS silver.clientes (
    cliente_id string,
    nombre_completo string,
    edad int,
    ciudad string,
    estado string,
    pais string,
    email string,
    segmento_cliente string,
    fecha_registro timestamp,
    calidad_registro string
)
USING DELTA
LOCATION "abfss://silver@{storageName}.dfs.core.windows.net/clientes"
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS silver.hoteles (
    hotel_id string,
    nombre_hotel string,
    destino string,
    destino_codigo string,
    pais string,
    categoria_estrellas int,
    regimen string,
    habitaciones int,
    precio_noche double,
    calificacion double,
    nivel_calificacion string
)
USING DELTA
LOCATION "abfss://silver@{storageName}.dfs.core.windows.net/hoteles"
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS silver.vuelos (
    vuelo_id string,
    aerolinea string,
    origen string,
    destino_codigo string,
    destino string,
    pais_destino string,
    fecha_salida timestamp,
    hora_salida string,
    fecha_llegada timestamp,
    hora_llegada string,
    precio double,
    equipaje string,
    estatus string,
    ruta string,
    duracion_minutos int,
    calidad_precio string,
    jornada string
)
USING DELTA
LOCATION "abfss://silver@{storageName}.dfs.core.windows.net/vuelos"
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS silver.reservas (
    id_reserva string,
    id_cliente string,
    id_vuelo string,
    id_hotel string,
    fecha_reserva timestamp,
    fecha_salida timestamp,
    fecha_regreso timestamp,
    destino string,
    tipo_reserva string,
    num_personas int,
    descuento_pct int,
    monto_total double,
    moneda string,
    estado_reserva string,
    numero_noches int,
    monto_por_persona double,
    validacion_fechas string
)
USING DELTA
LOCATION "abfss://silver@{storageName}.dfs.core.windows.net/reservas"
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS silver.pagos (
    id_pago string,
    id_reserva string,
    id_cliente string,
    fecha_pago timestamp,
    monto_pago double,
    moneda string,
    metodo_pago string,
    tipo_pago string,
    estado_pago string,
    referencia_pago string,
    calidad_pago string
)
USING DELTA
LOCATION "abfss://silver@{storageName}.dfs.core.windows.net/pagos"
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS silver.reservas_enriquecidas (
    id_reserva string,
    fecha_reserva timestamp,
    fecha_salida timestamp,
    fecha_regreso timestamp,
    destino string,
    tipo_reserva string,
    num_personas int,
    numero_noches int,
    descuento_pct int,
    monto_total double,
    moneda string,
    estado_reserva string,
    monto_por_persona double,
    ---CLIENTE
    cliente_id string,
    nombre_completo string,
    edad int,
    ciudad_cliente string,
    estado_cliente string,
    pais_cliente string,
    email string,
    segmento_cliente string,
    ---VUELOS
    vuelo_id string,
    aerolinea string,
    origen string,
    destino_codigo string,
    fecha_salida_vuelo timestamp,
    precio_vuelo double,
    equipaje string,
    duracion_minutos int,
    ruta string,
    jornada string,
    ---HOTELES
    hotel_id string,
    nombre_hotel string,
    categoria_estrellas int,
    regimen string,
    precio_noche double,
    calificacion_hotel double,
    nivel_calificacion_hotel string,
    ---PAGOS
    id_pago string,
    fecha_pago timestamp,
    monto_pago double,
    metodo_pago string,
    tipo_pago string,
    estado_pago string
)
USING DELTA
LOCATION "abfss://silver@{storageName}.dfs.core.windows.net/reservas_enriquecidas"
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Tablas Golden

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS golden.dim_cliente (
    cliente_id string,
    nombre_completo string,
    edad int,
    ciudad_cliente string,
    estado_cliente string,
    pais_cliente string,
    email string,
    segmento_cliente string
)
USING DELTA
LOCATION "abfss://golden@{storageName}.dfs.core.windows.net/dim_cliente"
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS golden.dim_destino (
    destino_codigo string,
	destino string
)
USING DELTA
LOCATION "abfss://golden@{storageName}.dfs.core.windows.net/dim_destino"
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS golden.dim_hotel (
    hotel_id string,
    nombre_hotel string,
    destino_codigo string,
    destino string,
    categoria_estrellas int,
    regimen string,
    precio_noche double,
    calificacion_hotel double,
    nivel_calificacion_hotel string
)
USING DELTA
LOCATION "abfss://golden@{storageName}.dfs.core.windows.net/dim_hotel"
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS golden.dim_vuelo (
    vuelo_id string,
    aerolinea string,
    origen string,
    destino_codigo string,
    fecha_salida_vuelo timestamp,
    precio_vuelo double,
    equipaje string,
    duracion_minutos int,
    ruta string,
    jornada string
)
USING DELTA
LOCATION "abfss://golden@{storageName}.dfs.core.windows.net/dim_vuelo"
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS golden.dim_fecha (
    fecha_key int,
	fecha timestamp,
	anio int,
	trimestre int,
	mes int,
	nombre_mes string,
	semana int,
	dia int,
	dia_semana int,
	nombre_dia string,
	es_fin_de_semana boolean
)
USING DELTA
LOCATION "abfss://golden@{storageName}.dfs.core.windows.net/dim_fecha"
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS golden.fact_reservas (
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
)
USING DELTA
LOCATION "abfss://golden@{storageName}.dfs.core.windows.net/fact_reservas"
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS golden.kpi_ventas_mensuales (
    anio int,
    mes int,
    nombre_mes string,
    total_reservas int,
    total_viajeros int,
    ventas_totales double,
    ticket_promedio double,
    gasto_promedio_persona double,
    pagos_recibidos double
)
USING DELTA
LOCATION "abfss://golden@{storageName}.dfs.core.windows.net/kpi_ventas_mensuales"
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS golden.kpi_destinos (
    destino_codigo string,
    destino string,
    total_reservas int,
    total_viajeros int,
    ventas_totales double,
    ticket_promedio double,
    gasto_promedio_persona double,
    promedio_noches double,
    descuento_promedio double
)
USING DELTA
LOCATION "abfss://golden@{storageName}.dfs.core.windows.net/kpi_destinos"
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS golden.kpi_clientes (
    cliente_id string,
    nombre_completo string,
    segmento_cliente string,
    ciudad_cliente string,
    total_reservas int,
    total_viajeros int,
    gasto_total double,
    ticket_promedio double,
    primera_reserva timestamp,
    ultima_reserva timestamp
)
USING DELTA
LOCATION "abfss://golden@{storageName}.dfs.core.windows.net/kpi_clientes"
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS golden.kpi_hoteles (
    hotel_id string,
    nombre_hotel string,
    destino string,
    categoria_estrellas int,
    regimen string,
    total_reservas int,
    total_huespedes int,
    ventas_totales double,
    precio_promedio_noche double,
    calificacion_promedio double,
    promedio_noches double
)
USING DELTA
LOCATION "abfss://golden@{storageName}.dfs.core.windows.net/kpi_hoteles"
""")