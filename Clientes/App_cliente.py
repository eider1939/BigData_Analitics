from flask import Flask, jsonify, request, render_template
import threading
import time
import json
import requests
import numpy as np
from src.radom_json import radom_json_crudo

app = Flask(__name__)

sending_data = False
send_count = 0
current_data = {}
#remote_server_url = 'http://localhost:5002/endpoint'  # URL del servidor remoto
remote_server_url ='http://localhost:5002/endpoint'
# Ruta al archivo .json que contiene las rutas
def file_json_send():
    path_json_rutas = 'Scripts/rutas.json'

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
        print(f"Sending data {send_count}: {current_data}")

        # Enviar datos al servidor remoto
        try:
            response = requests.post(remote_server_url, json=current_data)
            response.raise_for_status()  # Lanza una excepción para códigos de estado 4xx/5xx
            print(f"Datos enviados al servidor remoto: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Error enviando datos al servidor remoto: {e}")

        time.sleep(1)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/start', methods=['POST'])
def start():
    global sending_data
    if not sending_data:
        sending_data = True
        threading.Thread(target=generate_data).start()
    return jsonify({"status": "started"})

@app.route('/stop', methods=['POST'])
def stop():
    global sending_data
    sending_data = False
    return jsonify({"status": "stopped"})

@app.route('/status', methods=['GET'])
def status():
    return jsonify({"send_count": send_count, "data": current_data})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
