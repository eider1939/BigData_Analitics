from flask import Flask, request, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Iniciar sesión de Spark
spark = SparkSession.builder \
    .appName("FlaskSparkApp") \
    .getOrCreate()

# Definir el esquema para los datos
schema = StructType([
    StructField("word", StringType(), True),
    StructField("count", IntegerType(), True)
])

# Variables globales para mantener el conteo de datos y el último dato recibido
data_count = 0
last_data = {}

# Iniciar la aplicación Flask
app = Flask(__name__)

@app.route('/endpoint', methods=['POST'])
def receive_data():
    global data_count, last_data
    data = request.json
    last_data = data
    data_count += 1
    print(f"Datos recibidos: {data}")

    try:
        # Crear un DataFrame de Spark a partir de los datos recibidos
        data_df = spark.createDataFrame([data], schema=schema)

        # Realizar operaciones con los datos recibidos (opcional)
        # Por ejemplo, guardar en un archivo, base de datos, etc.

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
    app.run(host='0.0.0.0', port=5005, debug=True)