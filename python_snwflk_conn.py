import snowflake.connector
import pandas as pd
import csv

# Conectar a Snowflake
sf_conn = snowflake.connector.connect(
    user='herlbeng',
    password='BLHA9327_sno',
    account='uyhiano-ru59067',
    warehouse='COMPUTE_WH',
    database='BASETOTAL',
    schema='NBA'
)
sf_cursor = sf_conn.cursor()


# Leer el archivo CSV en un DataFrame
df = pd.read_csv("C:/Users/hermann.benda/DATA\BASETOTAL/game.csv")

# Definir una función que mapea los tipos de datos de pandas a tipos de datos de Snowflake
def map_dtype_to_snowflake(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "STRING"

# 1. Crear la consulta SQL para crear la tabla automáticamente
table_name = 'game'
columns = []
for col_name, dtype in df.dtypes.items():
    snowflake_type = map_dtype_to_snowflake(dtype)
    columns.append(f'"{col_name}" {snowflake_type}')

create_table_sql = f'CREATE OR REPLACE TABLE {table_name} ({", ".join(columns)});'

# Ejecutar la consulta para crear la tabla en Snowflake
sf_cursor.execute(create_table_sql)

# Verificar que la tabla fue creada
print(f"Tabla '{table_name}' creada con éxito en Snowflake.")

# 2. Poner el archivo CSV en un stage
# Subir el archivo data.csv desde el equipo local al stage @~/staged_files
sf_cursor.execute("PUT file://C:/Users/hermann.benda/DATA/BASETOTAL/game.csv @~/staged_files auto_compress=true")

# 3. Copiar los datos del CSV en la tabla MI_TABLA
sf_cursor.execute("""
    COPY INTO NBA.GAME 
    FROM @~/staged_files/game.csv.gz 
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1)
""")

# Cerrar el cursor y la conexión
sf_cursor.close()
sf_conn.close()


