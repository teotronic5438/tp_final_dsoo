import pandas as pd # type: ignore

archivo_csv = "./observatorio-de-obras-urbanas.csv"
df = pd.read_csv(archivo_csv, sep=";")
print("Lucas Nahuel Rey")