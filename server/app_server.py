from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType, TimestampType
import datetime
import os

app = Flask(__name__)



# Antes de crear la sesión Spark, establece la configuración
spark = SparkSession.builder \
    .appName("FlaskSparkApp") \
    .config("spark.master", "local") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .getOrCreate()

# Schema definition for the data
schema = StructType([
    StructField("Latitud", FloatType(), True),
    StructField("Longitud", FloatType(), True),
    StructField("Fecha", TimestampType(), True),
    StructField("ID_Cliente", IntegerType(), True),
    StructField("ID_Empleado", IntegerType(), True),
    StructField("Cantidad_de_Productos", IntegerType(), True)
])

# Variables to store the number of received data and the last data
data_count = 0
last_data = {}

@app.route('/endpoint', methods=['POST'])
def receive_data():
    global data_count, last_data
    data = request.json
    last_data = data
    data_count += 1
    print(f"Datos recibidos: {data}")
    try:
        # Prepare the data for Spark
        print(data['Date'])
        #data['Fecha'] = datetime.datetime.strptime(data['Date'], '%Y-%m-%d %H:%M:%S')
        data_df = spark.createDataFrame([data], schema=schema)

        # Save the data in Parquet format
        data_df.write.mode("append").parquet("data.parquet")

        
        return jsonify({"status": "success"}), 200
    except Exception as e:
        print(f"Error al recibir los datos: {e}")
        return jsonify({"status": "error", "message": str(e)}), 400

@app.route('/')
def index():
    return f"""
    <html>
    <head><title>Estado del Servidor</title></head>
    <body>
        <h1>Datos Recibidos</h1>
        <p>Cantidad de datos recibidos: {data_count}</p>
        <p>Último dato recibido: {last_data}</p>
    </body>
    </html>
    """

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002, debug=True)
