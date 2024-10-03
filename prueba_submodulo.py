import pandas as pd
from DBConector.utils import format_date

# Cargar el archivo CSV como DataFrame
ruta_csv = 'C:/Users/hermann.benda/DATA/BASETOTAL/game.csv'
df = pd.read_csv(ruta_csv)

# Definir la columna a formatear y el formato de fecha
cols = ['game_date']  
date_format = '%Y-%m-%d %H:%M:%S'  

# Aplicar la función format_date desde el submódulo
df = format_date(df, cols, date_format)

# Mostrar las primeras filas del DataFrame
print(df.head())
