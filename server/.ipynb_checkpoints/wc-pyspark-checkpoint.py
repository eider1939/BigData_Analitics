
## Identico ------>>>

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

## <<<------


##
## Los datos se leen desde un flujo de entrada en vez de un archivo
## en disco. Para ello, se crea un DataFrame que representa las líneas
## de texto de entrada, las cuales son leídas desde una conexión a
## localhost:9999. El flujo de datos puede considerarse como un DataFrame
## infinito, donde los nuevos datos se van adicionando al final.
##
df = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()


## Identico ------>>>

words = df.select(
   explode(
       split(df.value, " ")
   ).alias("word")
)

wordCounts = words.groupBy("word").count()

## <<<------


##
## Crea un stream de salida a la consola, en la que se van
## escribiendo los resultados a medida que se van ingresando
## datos.
##
query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
