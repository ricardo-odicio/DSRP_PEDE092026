from pyspark.sql import functions as F

df_raw = spark.table("raw_reclamos")

df_bronze = df_raw.withColumn("fecha_proceso_bronze", F.current_timestamp())

df_bronze.write.mode("overwrite").format("delta").saveAsTable("bronze_reclamos")