import pandas as pd

# Caminho dos arquivos
chicago_house_price_data = "Aula_05/data/raw/chicago_house_price_data.csv"
chicago_built_data = "Aula_05/data/raw/chicago_built_data.csv"

# Leitura dos arquivos baixados e armazenamento em DataFrames
df_chicago_house_price = pd.read_csv(chicago_house_price_data)
df_chicago_built = pd.read_csv(chicago_built_data)

# Exibindo as primeiras 5 linhas dos DataFrames
print("Primeiras 5 linhas do DataFrame Chicago House Price:")
print(df_chicago_house_price.head())
"""
Primeiras 5 linhas do DataFrame Chicago House Price:
   Price  Bedroom   Space  Room   Lot     Tax  Bathroom  Garage  Condition
0   53.0      2.0   967.0   5.0  39.0   652.0       1.5     0.0        0.0
1   55.0      2.0     NaN   5.0  33.0  1000.0       1.0     2.0        1.0
2   55.0      2.0     NaN   5.0  33.0  1000.0       1.0     2.0        1.0
3   56.0      3.0   900.0   5.0  35.0   897.0       1.5     1.0        0.0
4   58.0      3.0  1007.0   6.0  24.0   964.0       1.5     2.0        0.0
"""

print("\nPrimeiras 5 linhas do DataFrame Chicago Built:")
print(df_chicago_built.head())
"""
Primeiras 5 linhas do DataFrame Chicago Built:
   Price  YearBuilt Location
0   53.0     1995.0  Chicago
1   55.0     2001.0  Chicago
2   55.0     2001.0  Chicago
3   56.0     1998.0  Chicago
4   58.0     2003.0  Chicago
"""

# Exibindo informações sobre os DataFrames
print("\nInformações sobre o DataFrame Chicago House Price:")
print(df_chicago_house_price.info())
"""
Informações sobre o DataFrame Chicago House Price:
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 158 entries, 0 to 157
Data columns (total 9 columns):
 #   Column     Non-Null Count  Dtype  
---  ------     --------------  -----  
 0   Price      157 non-null    float64
 1   Bedroom    157 non-null    float64
 2   Space      145 non-null    float64
 3   Room       157 non-null    float64
 4   Lot        147 non-null    float64
 5   Tax        148 non-null    float64
 6   Bathroom   157 non-null    float64
 7   Garage     157 non-null    float64
 8   Condition  157 non-null    float64
dtypes: float64(9)
memory usage: 11.2 KB
"""

print("\nInformações sobre o DataFrame Chicago Built:")
print(df_chicago_built.info())
"""
Informações sobre o DataFrame Chicago Built:
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 103 entries, 0 to 102
Data columns (total 3 columns):
 #   Column     Non-Null Count  Dtype  
---  ------     --------------  -----  
 0   Price      102 non-null    float64
 1   YearBuilt  102 non-null    float64
 2   Location   101 non-null    object 
dtypes: float64(2), object(1)
memory usage: 2.5+ KB
"""

# Estatísticas descritivas das colunas numéricas dos DataFrames
print("\nEstatísticas descritivas do DataFrame Chicago House Price:")
print(df_chicago_house_price.describe())
"""
Estatísticas descritivas do DataFrame Chicago House Price:
            Price     Bedroom        Space        Room         Lot          Tax    Bathroom      Garage   Condition
count  157.000000  157.000000   145.000000  157.000000  147.000000   148.000000  157.000000  157.000000  157.000000
mean    56.464968    3.159236  1099.193103    6.490446   32.809524   912.304054    1.477707    0.853503    0.235669
std     12.834513    1.346932   463.543512    1.674154    8.428859   441.812773    0.529102    0.811103    0.425774
min     32.000000    1.000000   539.000000    4.000000   24.000000   418.000000    1.000000    0.000000    0.000000
25%     46.000000    2.000000   803.000000    5.000000   25.000000   652.750000    1.000000    0.000000    0.000000
50%     55.000000    3.000000   967.000000    6.000000   30.000000   824.000000    1.500000    1.000000    0.000000
75%     65.000000    4.000000  1226.000000    7.000000   37.000000  1006.250000    2.000000    1.500000    0.000000
max     90.000000    8.000000  2295.000000   12.000000   50.000000  2752.000000    3.000000    2.000000    1.000000
"""

print("\nEstatísticas descritivas do DataFrame Chicago Built:")
print(df_chicago_built.describe())
"""
Estatísticas descritivas do DataFrame Chicago Built:
            Price    YearBuilt
count  102.000000   102.000000
mean    56.470588  2001.039216
std     12.843079     3.799222
min     32.000000  1995.000000
25%     46.250000  1998.000000
50%     55.000000  2001.000000
75%     64.750000  2003.750000
max     88.000000  2010.000000
"""

# Preenchendo valores faltantes ou removendo-os
# fillna() preenche com zero (ou algum outro valor que faça sentido)
df_chicago_house_price_filled = df_chicago_house_price.fillna(0)
# dropna() remove as linhas com valores faltantes
df_chicago_built_dropped = df_chicago_built.dropna()

# Removendo linhas duplicadas
df_chicago_house_price_deduplicated = df_chicago_house_price_filled.drop_duplicates()
df_chicago_built_deduplicated = df_chicago_built_dropped.drop_duplicates()

# Salvando os DataFrames em arquivos CSV
df_chicago_house_price_deduplicated.to_csv(
    'Aula_05/data/processed/cleaned_chicago_house_price_data.csv', index=False)
df_chicago_built_deduplicated.to_csv(
    'Aula_05/data/processed/cleaned_chicago_built_data.csv', index=False)
