from flask import Flask, request, jsonify

app = Flask(__name__)

# Variables para almacenar el número de datos recibidos y el último dato
data_count = 0
last_data = {}

@app.route('/endpoint', methods=['POST'])
def receive_data():
    global data_count, last_data
    data = request.json
    data_count += 1
    last_data = data
    print(f"Datos recibidos: {data}")
    return jsonify({"status": "success"}), 200

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

