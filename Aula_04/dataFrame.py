import pandas as pd

# Ler o arquivo CSV a partir do URL
url = "https://raw.githubusercontent.com/uiuc-cse/data-fa14/gh-pages/data/iris.csv"
iris_data = pd.read_csv(url)

df = pd.DataFrame(iris_data).set_index("species")

# Exibir as primeiras 5 linhas do DataFrame
print("Primeiras 5 linhas do DataFrame:")
print(df.head())
"""
Primeiras 5 linhas do DataFrame:
         sepal_length  sepal_width  petal_length  petal_width
species                                                      
setosa            5.1          3.5           1.4          0.2
setosa            4.9          3.0           1.4          0.2
setosa            4.7          3.2           1.3          0.2
setosa            4.6          3.1           1.5          0.2
setosa            5.0          3.6           1.4          0.2
"""

# Exibir informações sobre o DataFrame
print("Informações sobre o DataFrame:")
print(df.info())
"""
Informações sobre o DataFrame:
<class 'pandas.core.frame.DataFrame'>
Index: 150 entries, setosa to virginica
Data columns (total 4 columns):
 #   Column        Non-Null Count  Dtype  
---  ------        --------------  -----  
 0   sepal_length  150 non-null    float64
 1   sepal_width   150 non-null    float64
 2   petal_length  150 non-null    float64
 3   petal_width   150 non-null    float64
dtypes: float64(4)
memory usage: 5.9+ KB
"""

# Obter estatísticas descritivas sobre as colunas numéricas
print("Estatísticas descritivas:")
print(df.describe())
"""
Estatísticas descritivas:
       sepal_length  sepal_width  petal_length  petal_width
count    150.000000   150.000000    150.000000   150.000000
mean       5.843333     3.054000      3.758667     1.198667
std        0.828066     0.433594      1.764420     0.763161
min        4.300000     2.000000      1.000000     0.100000
25%        5.100000     2.800000      1.600000     0.300000
50%        5.800000     3.000000      4.350000     1.300000
75%        6.400000     3.300000      5.100000     1.800000
max        7.900000     4.400000      6.900000     2.500000
"""

# Preencher os valores faltantes com o valor médio de cada coluna
avarage = df.mean()
df = df.fillna(avarage)
print(df)
"""
           sepal_length  sepal_width  petal_length  petal_width
species                                                        
setosa              5.1          3.5           1.4          0.2
setosa              4.9          3.0           1.4          0.2
setosa              4.7          3.2           1.3          0.2
setosa              4.6          3.1           1.5          0.2
setosa              5.0          3.6           1.4          0.2
...                 ...          ...           ...          ...
virginica           6.7          3.0           5.2          2.3
virginica           6.3          2.5           5.0          1.9
virginica           6.5          3.0           5.2          2.0
virginica           6.2          3.4           5.4          2.3
virginica           5.9          3.0           5.1          1.8

[150 rows x 4 columns]
"""

# Remover as linhas duplicadas
df = df.drop_duplicates()
print(df)
"""
           sepal_length  sepal_width  petal_length  petal_width
species                                                        
setosa              5.1          3.5           1.4          0.2
setosa              4.9          3.0           1.4          0.2
setosa              4.7          3.2           1.3          0.2
setosa              4.6          3.1           1.5          0.2
setosa              5.0          3.6           1.4          0.2
...                 ...          ...           ...          ...
virginica           6.7          3.0           5.2          2.3
virginica           6.3          2.5           5.0          1.9
virginica           6.5          3.0           5.2          2.0
virginica           6.2          3.4           5.4          2.3
virginica           5.9          3.0           5.1          1.8

[147 rows x 4 columns]
"""

# Salvar o DataFrame modificado em um arquivo CSV
df.to_csv("Aula_04/dados_limpos.csv", index=False)
