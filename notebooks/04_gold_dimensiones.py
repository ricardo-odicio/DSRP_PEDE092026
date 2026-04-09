from pyspark.sql import functions as F

df = spark.table("silver_reclamos")

dim_cliente = df.select("cliente_id").dropDuplicates()
dim_cliente.write.mode("overwrite").saveAsTable("dim_cliente")

dim_canal = df.select("canal").dropDuplicates()
dim_canal.write.mode("overwrite").saveAsTable("dim_canal")

dim_producto = df.select("producto").dropDuplicates()
dim_producto.write.mode("overwrite").saveAsTable("dim_producto")

dim_fecha = df.select("fecha_reclamo").dropDuplicates()
dim_fecha.write.mode("overwrite").saveAsTable("dim_fecha")