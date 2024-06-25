import pandas as pd

archivo_csv = "./observatorio-de-obras-urbanas.csv"
df = pd.read_csv(archivo_csv, sep=";")