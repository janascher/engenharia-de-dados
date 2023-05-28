import pandas as pd

gcs_uri = "gs://engenhariadados-exemplo/curated/price.csv"
df = pd.read_csv(gcs_uri)
# Usado a biblioteca pandas-gbq para integração com o BigQuery
df.to_gbq("aula_08.price", if_exists="replace")
