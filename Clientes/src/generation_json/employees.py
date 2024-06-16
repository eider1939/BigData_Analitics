import pandas as pd

def read_employees(file_path_employees):

    # Leer el archivo Parquet
    df_employees = pd.read_parquet(file_path_employees)

    # Mostrar las primeras filas del DataFrame
    return df_employees


def generate_random_employees(df_employees):
    # Seleccionar un cliente aleatorio
    random_employees = df_employees.sample()

    return random_employees['employee_id'].iloc[0]