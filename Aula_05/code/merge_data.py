import pandas as pd

# Leitura dos arquivos CSV limpos
df_chicago_house_price = pd.read_csv(
    'Aula_05/data/processed/cleaned_chicago_house_price_data.csv')
df_chicago_built = pd.read_csv(
    'Aula_05/data/processed/cleaned_chicago_built_data.csv')

# Merge dos DataFrames
merged_df = pd.merge(df_chicago_house_price, df_chicago_built, on='Price')
print(merged_df)
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
merged_df.to_csv('Aula_05/data/merged/result_chicago_merge.csv', index=False)
