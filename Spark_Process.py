import os
import findspark
from pyspark.sql import SparkSession
import json

def init_spark():
    findspark.init('/opt/spark')  # Ruta donde está instalado Spark
    try:
        spark = SparkSession.builder \
            .appName("MedallionArchitecture") \
            .config("spark.master", "local") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
            .getOrCreate()
        print("SparkSession iniciada.")
        # Convertir el diccionario a formato JSON antes de leerlo con Spark

        return spark
    except Exception as e:
        log_error("Error al iniciar SparkSession", e)
        raise

def create_bronze(df_eventos_json):
    try:
        bronze_path_hdfs = 'hdfs://localhost:9000/user/spark/bronze_layer'
        
        # Verificar si el directorio ya existe
        if not os.path.exists(bronze_path_hdfs):
            os.makedirs(bronze_path_hdfs)
            print("Crear ruta bronze")
        
        # Comprobar si ya hay archivos en el directorio para decidir el modo de escritura
        if os.listdir(bronze_path_hdfs):
            mode = "append"
        else:
            mode = "overwrite"
        
        df_eventos_json.write.mode("append").parquet(bronze_path_hdfs)
        print(f"Datos escritos en la capa Bronze en formato Parquet en HDFS (modo: append).")
        return bronze_path_hdfs
    except Exception as e:
        log_error("Error al crear capa Bronze", e)
        raise

def init_hive():
    try:
        spark = SparkSession.builder \
            .appName("Spark SQL Hive Integration") \
            .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
            .enableHiveSupport() \
            .getOrCreate()
        print("HiveSession iniciada.")
        return spark
    except Exception as e:
        log_error("Error al iniciar HiveSession", e)
        raise


def process_json_data(spark,hive,json_data):
    try:
        json_string = json.dumps(json_data)
        df_eventos_json = spark.read.json(spark.sparkContext.parallelize([json_string]))
        bronze_path_hdfs = create_bronze(df_eventos_json)
        #create_table_bronze(hive, bronze_path_hdfs)
    except Exception as e:
        log_error("Error en el procesamiento de datos JSON", e)
        raise

def log_error(message, exception):
    with open('spark.log', 'a') as log_file:
        log_file.write(f"{message}: {str(exception)}\n")
    print(f"Error: {message}. Detalles en spark.log.")

# Lógica principal para ejecutar el procesamiento
def main_spark(spark,hive,json_data):
    try:
        process_json_data(spark,hive,json_data)
    except Exception as e:
        print(f"Error general: {str(e)}")
