from pyspark.sql import functions as F

df = spark.table("silver_reclamos")

fact = df.select(
    "id_reclamo",
    "cliente_id",
    "canal",
    "producto",
    "fecha_reclamo",
    F.lit(1).alias("cantidad_reclamos"),
    "tiempo_resolucion_horas",
    "monto_reembolso",
    "nps"
)

fact.write.mode("overwrite").saveAsTable("fact_reclamos")