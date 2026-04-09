# Databricks notebook source
from pyspark.sql import functions as F

df_silver = spark.table("silver_reclamos")
dim_fecha = spark.table("dim_fecha")
dim_cliente = spark.table("dim_cliente")
dim_canal = spark.table("dim_canal")
dim_producto = spark.table("dim_producto")

fact_reclamos = (
    df_silver.alias("s")
    .join(
        dim_fecha.alias("f"),
        F.col("s.fecha_reclamo") == F.col("f.fecha"),
        "left"
    )
    .join(
        dim_cliente.alias("c"),
        F.col("s.cliente_id") == F.col("c.cliente_id"),
        "left"
    )
    .join(
        dim_canal.alias("ca"),
        F.col("s.canal") == F.col("ca.canal"),
        "left"
    )
    .join(
        dim_producto.alias("p"),
        F.col("s.producto") == F.col("p.producto"),
        "left"
    )
    .select(
        F.col("s.id_reclamo"),
        F.col("f.fecha_key"),
        F.col("c.cliente_key"),
        F.col("ca.canal_key"),
        F.col("p.producto_key"),
        F.lit(1).alias("cantidad_reclamos"),
        F.col("s.tiempo_resolucion_horas"),
        F.col("s.monto_reembolso"),
        F.col("s.nps"),
        F.col("s.clasificacion_nps"),
        F.col("s.estado"),
        F.col("s.motivo"),
        F.current_timestamp().alias("fecha_proceso_gold")
    )
)

fact_reclamos.write.mode("overwrite").format("delta").saveAsTable("fact_reclamos")

display(fact_reclamos.limit(20))
print(f"Registros cargados en fact_reclamos: {fact_reclamos.count()}")
