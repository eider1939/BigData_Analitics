import socket
import json
from pyspark.sql import SparkSession
from Spark_Process import main_spark, init_spark,init_hive
# Variables para almacenar el número de datos recibidos y el último dato
data_count = 0
last_data = {}

def receive_data(data):
    global data_count, last_data
    data_count += 1
    last_data = json.loads(data)
    print(f"Datos recibidos: {last_data}")

def create_server(host='0.0.0.0', port=5002):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f"Servidor escuchando en {host}:{port}")
    spark=init_spark()
    hive=init_hive()
    
    while True:
        client_socket, addr = server_socket.accept()
        print(f"Conexión establecida con {addr}")
        
        data = client_socket.recv(1024).decode('utf-8')
        if data:
            receive_data(data)
            client_socket.sendall(json.dumps({"status": "success"}).encode('utf-8'))
            
            # Procesar los datos en Spark
            main_spark(spark,hive,last_data)
        
        client_socket.close()

if __name__ == '__main__':
    create_server()
