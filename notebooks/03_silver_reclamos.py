# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =========================
# LECTURA DE BRONZE
# =========================
df_bronze = spark.table("bronze_reclamos")

# =========================
# TIPADO Y LIMPIEZA INICIAL
# =========================
df_silver = (
    df_bronze
    .withColumn("id_reclamo", F.col("id_reclamo").cast("int"))
    .withColumn("fecha_reclamo", F.to_date(F.col("fecha_reclamo")))
    .withColumn("cliente_id", F.col("cliente_id").cast("string"))
    .withColumn("canal", F.trim(F.col("canal").cast("string")))
    .withColumn("producto", F.trim(F.col("producto").cast("string")))
    .withColumn("motivo", F.trim(F.col("motivo").cast("string")))
    .withColumn("estado", F.trim(F.col("estado").cast("string")))
    .withColumn("tiempo_resolucion_horas", F.col("tiempo_resolucion_horas").cast("double"))
    .withColumn("monto_reembolso", F.col("monto_reembolso").cast("double"))
    .withColumn("nps", F.col("nps").cast("int"))
)

# =========================
# ELIMINAR REGISTROS SIN LLAVE
# =========================
df_silver = df_silver.filter(F.col("id_reclamo").isNotNull())

# =========================
# ELIMINAR DUPLICADOS
# CONSERVA EL MÁS RECIENTE
# =========================
window_spec = Window.partitionBy("id_reclamo").orderBy(F.col("fecha_carga").desc())

df_silver = (
    df_silver
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# =========================
# MANEJO DE NULOS CATEGÓRICOS
# =========================
df_silver = (
    df_silver
    .fillna({"cliente_id": "No informado"})
    .fillna({"canal": "No informado"})
    .fillna({"producto": "No informado"})
    .fillna({"motivo": "No informado"})
    .fillna({"estado": "No informado"})
)

# =========================
# MANEJO DE NULOS NUMÉRICOS
# =========================
df_silver = (
    df_silver
    .fillna({"tiempo_resolucion_horas": 0.0})
    .fillna({"monto_reembolso": 0.0})
)

# =========================
# VALIDAR RANGO DE NPS
# =========================
df_silver = df_silver.filter((F.col("nps") >= 0) & (F.col("nps") <= 10))

# =========================
# CLASIFICACIÓN NPS
# =========================
df_silver = (
    df_silver
    .withColumn(
        "clasificacion_nps",
        F.when(F.col("nps") >= 9, "Promotor")
         .when((F.col("nps") >= 7) & (F.col("nps") <= 8), "Neutro")
         .otherwise("Detractor")
    )
)

# =========================
# VARIABLES DERIVADAS DE FECHA
# =========================
df_silver = (
    df_silver
    .withColumn("anio", F.year("fecha_reclamo"))
    .withColumn("mes", F.month("fecha_reclamo"))
    .withColumn("dia", F.dayofmonth("fecha_reclamo"))
    .withColumn("trimestre", F.quarter("fecha_reclamo"))
    .withColumn("nombre_mes", F.date_format("fecha_reclamo", "MMMM"))
    .withColumn("fecha_proceso_silver", F.current_timestamp())
)

# =========================
# ELIMINAR TABLA ANTERIOR
# PARA EVITAR CONFLICTOS DE ESQUEMA
# =========================
spark.sql("DROP TABLE IF EXISTS silver_reclamos")

# =========================
# GUARDAR TABLA SILVER
# =========================
df_silver.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable("silver_reclamos")

# =========================
# VALIDACIÓN FINAL
# =========================
display(df_silver.limit(20))
print(f"Registros cargados en silver_reclamos: {df_silver.count()}")
df_silver.printSchema()
