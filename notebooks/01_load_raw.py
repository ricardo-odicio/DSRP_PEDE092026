from pyspark.sql import functions as F

df_raw = spark.table("workspace.default.dataset_reclamos_bigdata_1000")

# Normalizar nombres (opcional pero recomendable)
for col_name in df_raw.columns:
    new_name = col_name.strip().lower().replace(" ", "_")
    df_raw = df_raw.withColumnRenamed(col_name, new_name)

# Agregar metadata
df_raw = df_raw.withColumn("fecha_carga", F.current_timestamp())

# Guardar como raw
df_raw.write.mode("overwrite").format("delta").saveAsTable("raw_reclamos")

display(df_raw)
