from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = SparkSession \
  .builder \
  .appName("unal streaming") \
  .master("local[*]") \
  .getOrCreate()

spark

streaming_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", "12345") \
    .load()

# funcion para realizar transformaciones
def process_word_count(streaming_df):
    # lee y aplica transformacion
    words_df = streaming_df.selectExpr("explode(split(value, ' ')) as word")

    # Arealiza proceso de agregación
    agg_words_df = words_df \
        .groupBy("word") \
        .agg(count("word").alias("count"))
    
    # imprimir esquema
    agg_words_df.printSchema()
    return agg_words_df


# lee y transforma
agg_words_df = process_word_count(streaming_df)

# escritura en consola
writing_df = agg_words_df.writeStream \
    .format("console") \
    .outputMode("update") \
    .start()

# de este modo, se ejecuta 
writing_df.awaitTermination()