import uuid
import random

def generar_datos_aleatorios():
    # Generar un número aleatorio entre 0 y 50 para quantity_products
    quantity_products = random.randint(0, 50)

    # Generar un order_id aleatorio usando uuid
    order_id = str(uuid.uuid4())

    return quantity_products,order_id

#print(generar_datos_aleatorios())
