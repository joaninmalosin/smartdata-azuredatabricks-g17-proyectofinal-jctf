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

storageName = dbutils.widgets.get("storageName")

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

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS catalog_dev
# MAGIC MANAGED LOCATION 'abfss://metastore@${storageName}.dfs.core.windows.net/'
# MAGIC COMMENT 'Catalogo para la arquitectura medallion del ambiente de dev';

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS catalog_dev.raw;
# MAGIC DROP SCHEMA IF EXISTS catalog_dev.bronze;
# MAGIC DROP SCHEMA IF EXISTS catalog_dev.silver;
# MAGIC DROP SCHEMA IF EXISTS catalog_dev.golden;

# COMMAND ----------

dbutils.fs.rm(f"abfss://bronze@{storageName}.dfs.core.windows.net/",True)
dbutils.fs.rm(f"abfss://silver@{storageName}.dfs.core.windows.net/",True)
dbutils.fs.rm(f"abfss://golden@{storageName}.dfs.core.windows.net/",True)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS catalog_dev.raw;
# MAGIC CREATE SCHEMA IF NOT EXISTS catalog_dev.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS catalog_dev.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS catalog_dev.golden;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Tablas Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS catalog_dev.bronze.clientes (
# MAGIC     cliente_id string,
# MAGIC     nombre string,
# MAGIC     apellido string,
# MAGIC     edad int,
# MAGIC     ciudad string,
# MAGIC     estado string,
# MAGIC     pais string,
# MAGIC     email string,
# MAGIC     segmento_cliente string,
# MAGIC     fecha_registro timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://bronze@${storageName}.dfs.core.windows.net/clientes"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS catalog_dev.bronze.hoteles (
# MAGIC     hotel_id string,
# MAGIC     nombre_hotel string,
# MAGIC     destino string,
# MAGIC     destino_codigo string,
# MAGIC     pais string,
# MAGIC     categoria_estrellas int,
# MAGIC     regimen string,
# MAGIC     habitaciones int,
# MAGIC     precio_noche double,
# MAGIC     calificacion double
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://bronze@${storageName}.dfs.core.windows.net/hoteles"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS catalog_dev.bronze.vuelos (
# MAGIC     vuelo_id string,
# MAGIC     aerolinea string,
# MAGIC     origen string,
# MAGIC     destino_codigo string,
# MAGIC     destino string,
# MAGIC     pais_destino string,
# MAGIC     fecha_salida timestamp,
# MAGIC     hora_salida string,
# MAGIC     fecha_llegada timestamp,
# MAGIC     hora_llegada string,
# MAGIC     precio double,
# MAGIC     equipaje string,
# MAGIC     estatus string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://bronze@${storageName}.dfs.core.windows.net/vuelos"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS catalog_dev.bronze.reservas (
# MAGIC     id_reserva string,
# MAGIC     id_cliente string,
# MAGIC     id_vuelo string,
# MAGIC     id_hotel string,
# MAGIC     fecha_reserva timestamp,
# MAGIC     fecha_salida timestamp,
# MAGIC     fecha_regreso timestamp,
# MAGIC     destino string,
# MAGIC     tipo_reserva string,
# MAGIC     num_personas int,
# MAGIC     descuento_pct int,
# MAGIC     monto_total double,
# MAGIC     moneda string,
# MAGIC     estado_reserva string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://bronze@${storageName}.dfs.core.windows.net/reservas"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS catalog_dev.bronze.pagos (
# MAGIC     id_pago string,
# MAGIC     id_reserva string,
# MAGIC     id_cliente string,
# MAGIC     fecha_pago timestamp,
# MAGIC     monto_pago double,
# MAGIC     moneda string,
# MAGIC     metodo_pago string,
# MAGIC     tipo_pago string,
# MAGIC     estado_pago string,
# MAGIC     referencia_pago string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://bronze@${storageName}.dfs.core.windows.net/pagos"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Tablas Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS catalog_dev.silver.clientes (
# MAGIC     cliente_id string,
# MAGIC     nombre_completo string,
# MAGIC     edad int,
# MAGIC     ciudad string,
# MAGIC     estado string,
# MAGIC     pais string,
# MAGIC     email string,
# MAGIC     segmento_cliente string,
# MAGIC     fecha_registro timestamp,
# MAGIC     calidad_registro string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@${storageName}.dfs.core.windows.net/clientes"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS catalog_dev.silver.hoteles (
# MAGIC     hotel_id string,
# MAGIC     nombre_hotel string,
# MAGIC     destino string,
# MAGIC     destino_codigo string,
# MAGIC     pais string,
# MAGIC     categoria_estrellas int,
# MAGIC     regimen string,
# MAGIC     habitaciones int,
# MAGIC     precio_noche double,
# MAGIC     calificacion double,
# MAGIC     nivel_calificacion string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@${storageName}.dfs.core.windows.net/hoteles"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS catalog_dev.silver.vuelos (
# MAGIC     vuelo_id string,
# MAGIC     aerolinea string,
# MAGIC     origen string,
# MAGIC     destino_codigo string,
# MAGIC     destino string,
# MAGIC     pais_destino string,
# MAGIC     fecha_salida timestamp,
# MAGIC     hora_salida string,
# MAGIC     fecha_llegada timestamp,
# MAGIC     hora_llegada string,
# MAGIC     precio double,
# MAGIC     equipaje string,
# MAGIC     estatus string,
# MAGIC     ruta string,
# MAGIC     duracion_minutos int,
# MAGIC     calidad_precio string,
# MAGIC     jornada string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@${storageName}.dfs.core.windows.net/vuelos"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS catalog_dev.silver.reservas (
# MAGIC     id_reserva string,
# MAGIC     id_cliente string,
# MAGIC     id_vuelo string,
# MAGIC     id_hotel string,
# MAGIC     fecha_reserva timestamp,
# MAGIC     fecha_salida timestamp,
# MAGIC     fecha_regreso timestamp,
# MAGIC     destino string,
# MAGIC     tipo_reserva string,
# MAGIC     num_personas int,
# MAGIC     descuento_pct int,
# MAGIC     monto_total double,
# MAGIC     moneda string,
# MAGIC     estado_reserva string,
# MAGIC     numero_noches int,
# MAGIC     monto_por_persona double,
# MAGIC     validacion_fechas string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@${storageName}.dfs.core.windows.net/reservas"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS catalog_dev.silver.pagos (
# MAGIC     id_pago string,
# MAGIC     id_reserva string,
# MAGIC     id_cliente string,
# MAGIC     fecha_pago timestamp,
# MAGIC     monto_pago double,
# MAGIC     moneda string,
# MAGIC     metodo_pago string,
# MAGIC     tipo_pago string,
# MAGIC     estado_pago string,
# MAGIC     referencia_pago string,
# MAGIC     calidad_pago string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@${storageName}.dfs.core.windows.net/pagos"

# COMMAND ----------

# MAGIC %sql
# MAGIC --- DROP TABLE IF EXISTS catalog_dev.silver.reservas_enriquecidas
# MAGIC CREATE TABLE IF NOT EXISTS catalog_dev.silver.reservas_enriquecidas (
# MAGIC     id_reserva string,
# MAGIC     fecha_reserva timestamp,
# MAGIC     fecha_salida timestamp,
# MAGIC     fecha_regreso timestamp,
# MAGIC     destino string,
# MAGIC     tipo_reserva string,
# MAGIC     num_personas int,
# MAGIC     numero_noches int,
# MAGIC     descuento_pct int,
# MAGIC     monto_total double,
# MAGIC     moneda string,
# MAGIC     estado_reserva string,
# MAGIC     monto_por_persona double,
# MAGIC     ---CLIENTE
# MAGIC     cliente_id string,
# MAGIC     nombre_completo string,
# MAGIC     edad int,
# MAGIC     ciudad_cliente string,
# MAGIC     estado_cliente string,
# MAGIC     pais_cliente string,
# MAGIC     email string,
# MAGIC     segmento_cliente string,
# MAGIC     ---VUELOS
# MAGIC     vuelo_id string,
# MAGIC     aerolinea string,
# MAGIC     origen string,
# MAGIC     destino_codigo string,
# MAGIC     fecha_salida_vuelo timestamp,
# MAGIC     precio_vuelo double,
# MAGIC     equipaje string,
# MAGIC     duracion_minutos int,
# MAGIC     ruta string,
# MAGIC     jornada string,
# MAGIC     ---HOTELES
# MAGIC     hotel_id string,
# MAGIC     nombre_hotel string,
# MAGIC     categoria_estrellas int,
# MAGIC     regimen string,
# MAGIC     precio_noche double,
# MAGIC     calificacion_hotel double,
# MAGIC     nivel_calificacion_hotel string,
# MAGIC     ---PAGOS
# MAGIC     id_pago string,
# MAGIC     fecha_pago timestamp,
# MAGIC     monto_pago double,
# MAGIC     metodo_pago string,
# MAGIC     tipo_pago string,
# MAGIC     estado_pago string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@${storageName}.dfs.core.windows.net/reservas_enriquecidas"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Tablas Golden

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS catalog_dev.golden.dim_cliente (
# MAGIC     cliente_id string,
# MAGIC     nombre_completo string,
# MAGIC     edad int,
# MAGIC     ciudad_cliente string,
# MAGIC     estado_cliente string,
# MAGIC     pais_cliente string,
# MAGIC     email string,
# MAGIC     segmento_cliente string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://golden@${storageName}.dfs.core.windows.net/dim_cliente"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS catalog_dev.golden.dim_destino (
# MAGIC 	destino_codigo string,
# MAGIC 	destino string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://golden@${storageName}.dfs.core.windows.net/dim_destino"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS catalog_dev.golden.dim_hotel (
# MAGIC 	hotel_id string,
# MAGIC     nombre_hotel string,
# MAGIC     destino_codigo string,
# MAGIC     destino string,
# MAGIC     categoria_estrellas int,
# MAGIC     regimen string,
# MAGIC     precio_noche double,
# MAGIC     calificacion_hotel double,
# MAGIC     nivel_calificacion_hotel string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://golden@${storageName}.dfs.core.windows.net/dim_hotel"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS catalog_dev.golden.dim_vuelo(
# MAGIC 	vuelo_id string,
# MAGIC     aerolinea string,
# MAGIC     origen string,
# MAGIC     destino_codigo string,
# MAGIC     fecha_salida_vuelo timestamp,
# MAGIC     precio_vuelo double,
# MAGIC     equipaje string,
# MAGIC     duracion_minutos int,
# MAGIC     ruta string,
# MAGIC     jornada string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://golden@${storageName}.dfs.core.windows.net/dim_vuelo"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS catalog_dev.golden.dim_fecha(
# MAGIC 	fecha_key int,
# MAGIC 	fecha timestamp,
# MAGIC 	anio int,
# MAGIC 	trimestre int,
# MAGIC 	mes int,
# MAGIC 	nombre_mes string,
# MAGIC 	semana int,
# MAGIC 	dia int,
# MAGIC 	dia_semana int,
# MAGIC 	nombre_dia string,
# MAGIC 	es_fin_de_semana boolean
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://golden@${storageName}.dfs.core.windows.net/dim_fecha"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_dev.golden.fact_reservas (
# MAGIC     id_reserva          string,
# MAGIC     fecha_reserva_key   integer,
# MAGIC     fecha_salida_key    integer,
# MAGIC     cliente_id          string,
# MAGIC     vuelo_id            string,
# MAGIC     hotel_id            string,
# MAGIC     destino_codigo      string,
# MAGIC     tipo_reserva        string,
# MAGIC     num_personas        integer,
# MAGIC     descuento_pct       decimal(5,2),
# MAGIC     monto_total         decimal(18,2),
# MAGIC     monto_por_persona   decimal(18,2),
# MAGIC     moneda              string,
# MAGIC     estado_reserva      string,
# MAGIC     numero_noches       integer,
# MAGIC     precio_vuelo        decimal(18,2),
# MAGIC     precio_noche        decimal(18,2),
# MAGIC     duracion_minutos    integer,
# MAGIC     calificacion_hotel  decimal(3,1),
# MAGIC     monto_pago          decimal(18,2),
# MAGIC     estado_pago         string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://golden@${storageName}.dfs.core.windows.net/fact_reservas"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_dev.golden.kpi_ventas_mensuales (
# MAGIC     anio int,
# MAGIC     mes int,
# MAGIC     nombre_mes string,
# MAGIC     total_reservas int,
# MAGIC     total_viajeros int,
# MAGIC     ventas_totales double,
# MAGIC     ticket_promedio double,
# MAGIC     gasto_promedio_persona double,
# MAGIC     pagos_recibidos double
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://golden@${storageName}.dfs.core.windows.net/kpi_ventas_mensuales"
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_dev.golden.kpi_destinos (
# MAGIC     destino_codigo string,
# MAGIC     destino string,
# MAGIC     total_reservas int,
# MAGIC     total_viajeros int,
# MAGIC     ventas_totales double,
# MAGIC     ticket_promedio double,
# MAGIC     gasto_promedio_persona double,
# MAGIC     promedio_noches double,
# MAGIC     descuento_promedio double
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://golden@${storageName}.dfs.core.windows.net/kpi_destinos"
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_dev.golden.kpi_clientes (
# MAGIC     cliente_id string,
# MAGIC     nombre_completo string,
# MAGIC     segmento_cliente string,
# MAGIC     ciudad_cliente string,
# MAGIC     total_reservas int,
# MAGIC     total_viajeros int,
# MAGIC     gasto_total double,
# MAGIC     ticket_promedio double,
# MAGIC     primera_reserva timestamp,
# MAGIC     ultima_reserva timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://golden@${storageName}.dfs.core.windows.net/kpi_clientes"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_dev.golden.kpi_hoteles (
# MAGIC     hotel_id string,
# MAGIC     nombre_hotel string,
# MAGIC     destino string,
# MAGIC     categoria_estrellas int,
# MAGIC     regimen string,
# MAGIC     total_reservas int,
# MAGIC     total_huespedes int,
# MAGIC     ventas_totales double,
# MAGIC     precio_promedio_noche double,
# MAGIC     calificacion_promedio double,
# MAGIC     promedio_noches double
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://golden@${storageName}.dfs.core.windows.net/kpi_hoteles"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE catalog_dev.golden.kpi_vuelos (
# MAGIC     aerolinea string,
# MAGIC     ruta string,
# MAGIC     jornada string,
# MAGIC     reservas int,
# MAGIC     pasajeros int,
# MAGIC     precio_promedio double,
# MAGIC     duracion_promedio double,
# MAGIC     ticket_promedio double
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://golden@${storageName}.dfs.core.windows.net/kpi_vuelos"
# MAGIC