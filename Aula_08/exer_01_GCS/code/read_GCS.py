import pandas as pd
from google.cloud import storage

df = pd.read_csv("gs://engenhariadados-exemplo/raw/price.csv")

# dropna(): remove as linhas com valores faltantes
df.dropna(subset=["space"], inplace=True)
# fillna(): Preencher os valores faltantes com o valor médio de cada coluna
tax_avarage = df.tax.mean()
df.tax = df.tax.fillna(tax_avarage)

bucket = storage.Client().bucket("engenhariadados-exemplo")
blob = bucket.blob("curated/price.csv")
blob.upload_from_string(df.to_csv(index=False), "text/csv")
