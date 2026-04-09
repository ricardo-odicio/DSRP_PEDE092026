from pyspark.sql import functions as F

file_path = "/FileStore/tables/dataset_reclamos_bigdata_1000.csv"

df_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("encoding", "UTF-8")
    .csv(file_path)
)

for col_name in df_raw.columns:
    new_name = (
        col_name.strip()
        .lower()
        .replace(" ", "_")
    )
    df_raw = df_raw.withColumnRenamed(col_name, new_name)

df_raw = (
    df_raw
    .withColumn("fecha_carga", F.current_timestamp())
)

df_raw.write.mode("overwrite").format("delta").saveAsTable("raw_reclamos")