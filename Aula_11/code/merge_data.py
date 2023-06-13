from pyspark.sql import DataFrame, SparkSession

spark = (
    SparkSession.builder.config('spark.driver.host', 'localhost')
    .appName('SparkSQL')
    .getOrCreate()
)

# Caminho dos arquivos limpos
cleaned_chicago_house_price = "Aula_11/data/processed/cleaned_chicago_house_price"
cleaned_chicago_built = "Aula_11/data/processed/cleaned_chicago_built"

# Leitura dos arquivos CSV limpos
df_chicago_house_price = spark.read.csv(
    cleaned_chicago_house_price, header=True, inferSchema=True)
df_chicago_built = spark.read.csv(
    cleaned_chicago_built, header=True, inferSchema=True)

# Merge dos DataFrames
df_merged = df_chicago_house_price.join(df_chicago_built, on='price', how='inner')
print(df_merged)


# df_merged = pd.merge(df_chicago_house_price, df_chicago_built, on='Price')
# print(df_merged)
"""
     Price  Bedroom   Space  Room   Lot     Tax  Bathroom  Garage  Condition  YearBuilt Location
0     53.0      2.0   967.0   5.0  39.0   652.0       1.5     0.0        0.0     1995.0  Chicago
1     53.0      2.0   967.0   5.0  39.0   652.0       1.5     0.0        0.0     2002.0  Chicago
2     53.0      2.0   636.0   6.0  30.0   553.0       1.0     2.0        1.0     1995.0  Chicago
3     53.0      2.0   636.0   6.0  30.0   553.0       1.0     2.0        1.0     2002.0  Chicago
4     53.0      3.0   673.0   6.0  30.0   589.0       1.0     2.0        1.0     1995.0  Chicago
..     ...      ...     ...   ...   ...     ...       ...     ...        ...        ...      ...
352   67.0      2.0   901.0   5.0  30.0  1373.0       1.0     1.0        0.0     2003.0  Chicago
353   67.0      2.0  1122.0   7.0   0.0  1131.0       1.5     1.5        0.0     2003.0  Chicago
354   68.0      4.0  1274.0   8.0  37.0     0.0       2.0     2.0        0.0     2004.0  Chicago
355   37.0      5.0  1204.0   7.0  25.0   531.0       1.5     0.0        0.0     1999.0  Chicago
356   32.0      4.0  1065.0   7.0  25.0   492.0       1.5     0.0        0.0     1999.0  Chicago
"""

# Escrita do resultado em um arquivo CSV
# df_merged.to_csv('Aula_05/data/merged/result_chicago_merge.csv', index=False)
