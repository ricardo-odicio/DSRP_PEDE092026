# Databricks notebook source
from pyspark.sql import functions as F

df_silver = spark.table("silver_reclamos")

# =========================
# DIM_FECHA
# =========================
dim_fecha = (
    df_silver
    .select("fecha_reclamo", "anio", "mes", "nombre_mes", "trimestre", "dia")
    .dropDuplicates()
    .withColumn("fecha_key", F.date_format("fecha_reclamo", "yyyyMMdd").cast("int"))
    .select(
        "fecha_key",
        F.col("fecha_reclamo").alias("fecha"),
        "dia",
        "mes",
        "nombre_mes",
        "trimestre",
        "anio"
    )
)

dim_fecha.write.mode("overwrite").format("delta").saveAsTable("dim_fecha")

# =========================
# DIM_CLIENTE
# =========================
dim_cliente = (
    df_silver
    .select("cliente_id")
    .dropDuplicates()
    .withColumn("cliente_key", F.monotonically_increasing_id() + 1)
    .select("cliente_key", "cliente_id")
)

dim_cliente.write.mode("overwrite").format("delta").saveAsTable("dim_cliente")

# =========================
# DIM_CANAL
# =========================
dim_canal = (
    df_silver
    .select("canal")
    .dropDuplicates()
    .withColumn("canal_key", F.monotonically_increasing_id() + 1)
    .select("canal_key", "canal")
)

dim_canal.write.mode("overwrite").format("delta").saveAsTable("dim_canal")

# =========================
# DIM_PRODUCTO
# =========================
dim_producto = (
    df_silver
    .select("producto")
    .dropDuplicates()
    .withColumn("producto_key", F.monotonically_increasing_id() + 1)
    .select("producto_key", "producto")
)

dim_producto.write.mode("overwrite").format("delta").saveAsTable("dim_producto")

print("Dimensiones creadas correctamente.")

display(spark.table("dim_fecha").limit(10))
display(spark.table("dim_cliente").limit(10))
display(spark.table("dim_canal").limit(10))
display(spark.table("dim_producto").limit(10))
