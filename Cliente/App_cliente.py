import socket
import json
import threading
import time
import numpy as np
from src.radom_json import radom_json_crudo

# Variables globales
sending_data = False
send_count = 0
current_data = {}
server_host = 'localhost'
server_port = 5002

# Ruta al archivo .json que contiene las rutas
def file_json_send():
    path_json_rutas = 'D:\\MAESTRIA\\Analitica_&_Big_data\\Trabajo_Final1\\Cliente\\Scripts\\rutas.json'

    with open(path_json_rutas, 'r') as file:
        data = json.load(file)

    path_medellin = data['path_medellin']
    path_customers = data['path_customers']
    path_employees = data['path_employees']

    datos_json = radom_json_crudo(path_medellin, path_customers, path_employees)

    # Convertir valores int64 a int
    for key, value in datos_json.items():
        if isinstance(value, np.int64):
            datos_json[key] = int(value)

    return datos_json

def generate_data():
    global sending_data, send_count, current_data
    while sending_data:
        current_data = file_json_send()
        send_count += 1
        print(f"Enviando datos {send_count}: {current_data}")

        # Enviar datos al servidor remoto usando sockets
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
                client_socket.connect((server_host, server_port))
                client_socket.sendall(json.dumps(current_data).encode('utf-8'))
                response = client_socket.recv(1024).decode('utf-8')
                print(f"Respuesta del servidor: {response}")
        except Exception as e:
            print(f"Error enviando datos al servidor remoto: {e}")

        time.sleep(1)

def start_sending():
    global sending_data
    if not sending_data:
        sending_data = True
        threading.Thread(target=generate_data).start()
        print("Inicio del envío de datos")

def stop_sending():
    global sending_data
    sending_data = False
    print("Parada del envío de datos")

def main():
    while True:
        command = input("Ingresa 'start' para iniciar, 'stop' para detener, 'status' para ver el estado, o 'exit' para salir: ").strip().lower()
        if command == 'start':
            start_sending()
        elif command == 'stop':
            stop_sending()
        elif command == 'status':
            print(f"Datos enviados: {send_count}")
            print(f"Últimos datos enviados: {current_data}")
        elif command == 'exit':
            stop_sending()
            break
        else:
            print("Comando no reconocido.")

if __name__ == '__main__':
    main()
