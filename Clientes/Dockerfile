# Usar una imagen base de Python
FROM python:3.9

# Establecer el directorio de trabajo
WORKDIR /app

# Copiar el archivo requirements.txt en el contenedor
COPY requirements.txt .

# Instalar las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el resto de tu aplicación al contenedor
COPY . .

# Exponer el puerto que utilizará la aplicación
EXPOSE 5000

# Comando por defecto (esto se puede sobrescribir en el comando `docker run`)
CMD ["python"]
