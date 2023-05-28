import pandas as pd
from google.cloud import storage

price_data = "../data/raw/price_data.csv"
df = pd.read_csv(price_data)
bucket = storage.Client().bucket("engenhariadados-exemplo")
blob = bucket.blob("raw/price.csv")
blob.upload_from_string(df.to_csv(index=False), "text/csv")
