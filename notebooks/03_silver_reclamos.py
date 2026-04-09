from pyspark.sql import functions as F
from pyspark.sql.window import Window

df = spark.table("bronze_reclamos")

df = (
    df
    .withColumn("id_reclamo", F.col("id_reclamo").cast("int"))
    .withColumn("fecha_reclamo", F.to_date("fecha_reclamo"))
    .withColumn("nps", F.col("nps").cast("int"))
)

df = df.filter(F.col("id_reclamo").isNotNull())

window = Window.partitionBy("id_reclamo").orderBy(F.col("fecha_carga").desc())

df = (
    df.withColumn("row", F.row_number().over(window))
      .filter("row = 1")
      .drop("row")
)

df = df.withColumn(
    "clasificacion_nps",
    F.when(F.col("nps") >= 9, "Promotor")
     .when((F.col("nps") >= 7), "Neutro")
     .otherwise("Detractor")
)

df.write.mode("overwrite").format("delta").saveAsTable("silver_reclamos")