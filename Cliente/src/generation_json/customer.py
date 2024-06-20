import pandas as pd

def read_customer(file_path_clientes):

    # Leer el archivo Parquet
    df_clientes = pd.read_parquet(file_path_clientes)

    # Mostrar las primeras filas del DataFrame
    return df_clientes


def generate_random_customer(df_clientes):
    # Seleccionar un cliente aleatorio
    random_customer = df_clientes.sample()

    return random_customer['customer_id'].iloc[0]
