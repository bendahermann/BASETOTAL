import psycopg2
import snowflake.connector
import csv

# Conectar a PostgreSQL local
pg_conn = psycopg2.connect(
    host="localhost",  # O IP de tu laptop si es remota
    database="nba",
    user="snowflake",
    password="snowflake",
    port="5432"  # Puerto de PostgreSQL
)
pg_cursor = pg_conn.cursor()

# Extraer datos de PostgreSQL
pg_cursor.execute("SELECT * FROM public.game")
rows = pg_cursor.fetchall()

# Escribir los datos en un archivo CSV temporal
with open("data.csv", "w", newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow([desc[0] for desc in pg_cursor.description])  # Escribir encabezados
    writer.writerows(rows)  # Escribir filas de datos

pg_cursor.close()
pg_conn.close()