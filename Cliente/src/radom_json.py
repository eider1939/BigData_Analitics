from src.generation_json.poligono_med import *
from src.generation_json.customer import *
from src.generation_json.employees import *
from src.generation_json.data_radom import *
from datetime import datetime

def radom_json_crudo(path_medellin,path_customers,path_employees):
    #-------------###----------------
    gdf_postal=read_medellin(path_medellin)
    polygon=gdf_postal.loc[0, 'geometry']
    random_points = generate_random_points(polygon, 1)

    #-------------###----------------
    df_clientes=read_customer(file_path_clientes=path_customers)
    ramdom_customer=generate_random_customer(df_clientes)
    #-------------###----------------
    df_employees=read_employees(file_path_employees=path_employees)
    random_employees=generate_random_employees(df_employees)


    quantity_products,order_id=generar_datos_aleatorios()
    fecha=datetime.now()
        # Crear el diccionario con los datos generados aleatoriamente
    datos_aleatorios = {
        "latitude": random_points[0].y,
        "longitude": random_points[0].x,
        "Date":fecha.strftime("%Y-%m-%d %H:%M:%S"),
        "customer_id": ramdom_customer,
        "employee_id": random_employees,
        "quantity_products": quantity_products,
        "order_id": order_id
    }

    return datos_aleatorios

