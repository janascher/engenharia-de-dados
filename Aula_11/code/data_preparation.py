import pyspark.sql.functions as f
from pyspark.sql import DataFrame, SparkSession

"""
Cria uma instância do SparkSession, permitindo a interação com o Spark SQL. Essa instância é a base para executar consultas e realizar operações de processamento de dados distribuídos no Spark.
Configura o host do driver como "localhost" e define o nome do aplicativo como "SparkSQL".
Obtém ou cria uma instância do SparkSession com base nas configurações fornecidas
"""
spark = (
    SparkSession.builder.config('spark.driver.host', 'localhost')
    .appName('SparkSQL')
    .getOrCreate()
)

# Caminho dos arquivos
chicago_house_price_data = "Aula_11/data/raw/chicago_house_price_data.csv"
chicago_built_data = "Aula_11/data/raw/chicago_built_data.csv"

# Leitura dos arquivos baixados e armazenamento em DataFrames
# O parâmetro `header=True` indica que o arquivo possui um cabeçalho
# O parâmetro `inferSchema=True` faz com que o Spark infira automaticamente os tipos de dados das colunas.
df_chicago_house_price = spark.read.csv(
    chicago_house_price_data, header=True, inferSchema=True)
df_chicago_built = spark.read.csv(
    chicago_built_data, header=True, inferSchema=True)

# df_chicago_built.show()

# Exibindo as primeiras 5 linhas dos DataFrames
print("\nPrimeiras 5 linhas do DataFrame Chicago House Price:")
df_chicago_house_price.show(5)
"""
Primeiras 5 linhas do DataFrame Chicago House Price:                            
+-----+-------+-----+----+---+----+--------+------+---------+
|Price|Bedroom|Space|Room|Lot| Tax|Bathroom|Garage|Condition|
+-----+-------+-----+----+---+----+--------+------+---------+
|   53|      2|  967|   5| 39| 652|     1.5|     0|        0|
|   55|      2|   NA|   5| 33|1000|       1|     2|        1|
|   55|      2|   NA|   5| 33|1000|       1|     2|        1|
|   56|      3|  900|   5| 35| 897|     1.5|     1|        0|
|   58|      3| 1007|   6| 24| 964|     1.5|     2|        0|
+-----+-------+-----+----+---+----+--------+------+---------+
only showing top 5 rows
"""

print("\nPrimeiras 5 linhas do DataFrame Chicago Built:")
df_chicago_built.show(5)
"""
Primeiras 5 linhas do DataFrame Chicago Built:
+-----+---------+--------+
|Price|YearBuilt|Location|
+-----+---------+--------+
|   53|     1995| Chicago|
|   55|     2001| Chicago|
|   55|     2001| Chicago|
|   56|     1998| Chicago|
|   58|     2003| Chicago|
+-----+---------+--------+
only showing top 5 rows
"""

# Quantidade de linhas do DataFrame
print("\nQuantidade de linhas do DataFrame Chicago House Price:")
print(df_chicago_house_price.count())
"""
Quantidade de linhas do DataFrame Chicago House Price:
158
"""

# Quantidade de linhas do DataFrame
print("\nQuantidade de linhas do DataFrame Chicago Built:")
print(df_chicago_built.count())
"""
Quantidade de linhas do DataFrame Chicago Built:
103
"""

# Exibir a estrutura do DataFrame
print("\nInformações da estrutura do DataFrame Chicago House Price:")
df_chicago_house_price.printSchema()
"""
Informações da estrutura do DataFrame Chicago House Price:
root
 |-- Price: string (nullable = true)
 |-- Bedroom: string (nullable = true)
 |-- Space: string (nullable = true)
 |-- Room: string (nullable = true)
 |-- Lot: string (nullable = true)
 |-- Tax: string (nullable = true)
 |-- Bathroom: string (nullable = true)
 |-- Garage: string (nullable = true)
 |-- Condition: string (nullable = true)
"""

# Exibir a estrutura do DataFrame
print("\nInformações da estrutura do DataFrame Chicago Built:")
df_chicago_built.printSchema()
"""
Informações da estrutura do DataFrame Chicago Built:
root
 |-- Price: string (nullable = true)
 |-- YearBuilt: string (nullable = true)
 |-- Location: string (nullable = true)
"""

# Exibir informações estatísticas resumidas do DataFrame
print("\nInformações estatísticas do DataFrame Chicago House Price:")
df_chicago_house_price.describe().show()
"""
Informações estatísticas do DataFrame Chicago House Price:
23/06/13 16:28:44 WARN package: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.
+-------+------------------+------------------+------------------+------------------+-----------------+-----------------+------------------+------------------+------------------+
|summary|             Price|           Bedroom|             Space|              Room|              Lot|              Tax|          Bathroom|            Garage|         Condition|
+-------+------------------+------------------+------------------+------------------+-----------------+-----------------+------------------+------------------+------------------+
|  count|               158|               158|               158|               158|              158|              158|               158|               158|               158|
|   mean| 56.46496815286624| 3.159235668789809| 1099.193103448276| 6.490445859872612|32.80952380952381|912.3040540540541|1.4777070063694266|0.8535031847133758|0.2356687898089172|
| stddev|12.834513097164496|1.3469317180460767|463.54351192968636|1.6741543383608992|8.428858884834687|441.8127725290968|0.5291016486745087|0.8111031315769252|0.4257742177121552|
|    min|                32|                 1|              1007|                12|               24|             1000|                 1|                 0|                 0|
|    max|                NA|                NA|                NA|                NA|               NA|               NA|                NA|                NA|                NA|
+-------+------------------+------------------+------------------+------------------+-----------------+-----------------+------------------+------------------+------------------+
"""

# Exibir informações estatísticas resumidas do DataFrame
print('\nInformações estatísticas do DataFrame Chicago Built:')
df_chicago_built.describe().show()
"""
Informações statísticas do DataFrame Chicago Built:
+-------+------------------+------------------+--------+                        
|summary|             Price|         YearBuilt|Location|
+-------+------------------+------------------+--------+
|  count|               103|               103|     102|
|   mean|56.470588235294116|2001.0392156862745|    null|
| stddev|12.843078717257868| 3.799222350412028|    null|
|    min|                32|              1995| Chicago|
|    max|                NA|                NA|      NA|
+-------+------------------+------------------+--------+
"""

# Preenchendo valores faltantes do DataFrame Chicago House Price
avarage_price = df_chicago_house_price.select(
    f.avg(f.col("price"))).collect()[0][0]
avarage_price_int = int(avarage_price)
df_chicago_house_price_filled = df_chicago_house_price.withColumn("price", f.when(
    f.col("price").isNull(), avarage_price_int).otherwise(f.col("price")))
# df_chicago_house_price_filled.show()
"""
+-----+-------+-----+----+----+----+--------+------+---------+                  
|price|bedroom|space|room| lot| tax|bathroom|garage|condition|
+-----+-------+-----+----+----+----+--------+------+---------+
|   56|      2|  967|   5|  39| 652|     1.5|     0|        0|
|   55|      2| null|   5|  33|1000|       1|     2|        1|
|   55|      2| null|   5|  33|1000|       1|     2|        1|
|   56|      3|  900|   5|  35| 897|     1.5|     1|        0|
|   58|      3| 1007|   6|  24| 964|     1.5|     2|        0|
|   64|      3| 1100|   7|  50|1099|     1.5|   1.5|        0|
|   44|      4|  897|   7|  25| 960|       2|     1|        0|
|   49|      5| 1400|   8|null| 678|       1|     1|        1|
|   70|      3| 2261|   6|  29|2700|       1|     2|        0|
|   72|      4| 1290|   8|null| 800|     1.5|   1.5|        0|
|   82|      4| 2104|   9|  40|1038|     2.5|     1|        1|
|   85|      8| 2240|  12|  50|1200|       3|     2|        0|
|   45|      2|  641|   5|  25| 860|       1|     0|        0|
|   47|      3|  862|   6|  25| 600|       1|     0|        0|
|   49|      4| 1043|   7|  30| 676|     1.5|     0|        0|
|   56|      4| 1325|   8|  50|1287|     1.5|     0|        0|
|   60|      2|  782|   5|  25| 834|       1|     0|        0|
|   62|      3| 1126|   7|  30| 734|       2|     0|        1|
|   64|      4| 1226|   8|  37|null|       2|     2|        0|
|   66|      2|  929|   5|  30|1355|       1|     1|        0|
+-----+-------+-----+----+----+----+--------+------+---------+
only showing top 20 rows
"""

# Removendo valores faltantes do DataFrame Chicago House Price
df_chicago_house_price_dropped = df_chicago_house_price_filled.na.drop()
# df_chicago_house_price_dropped.show()
"""
+-----+-------+-----+----+---+----+--------+------+---------+                   
|price|bedroom|space|room|lot| tax|bathroom|garage|condition|
+-----+-------+-----+----+---+----+--------+------+---------+
|   56|      2|  967|   5| 39| 652|     1.5|     0|        0|
|   56|      3|  900|   5| 35| 897|     1.5|     1|        0|
|   58|      3| 1007|   6| 24| 964|     1.5|     2|        0|
|   64|      3| 1100|   7| 50|1099|     1.5|   1.5|        0|
|   44|      4|  897|   7| 25| 960|       2|     1|        0|
|   70|      3| 2261|   6| 29|2700|       1|     2|        0|
|   82|      4| 2104|   9| 40|1038|     2.5|     1|        1|
|   85|      8| 2240|  12| 50|1200|       3|     2|        0|
|   45|      2|  641|   5| 25| 860|       1|     0|        0|
|   47|      3|  862|   6| 25| 600|       1|     0|        0|
|   49|      4| 1043|   7| 30| 676|     1.5|     0|        0|
|   56|      4| 1325|   8| 50|1287|     1.5|     0|        0|
|   60|      2|  782|   5| 25| 834|       1|     0|        0|
|   62|      3| 1126|   7| 30| 734|       2|     0|        1|
|   66|      2|  929|   5| 30|1355|       1|     1|        0|
|   35|      4| 1137|   7| 25| 561|     1.5|     0|        0|
|   38|      3|  743|   6| 25| 489|       1|     0|        0|
|   46|      2|  803|   5| 27| 774|       1|     0|        1|
|   46|      2|  696|   4| 30| 440|       2|     1|        0|
|   50|      2|  691|   6| 30| 549|       1|     2|        1|
+-----+-------+-----+----+---+----+--------+------+---------+
only showing top 20 rows
"""

# Preenchendo valores faltantes do DataFrame Chicago House Built
avarage_built = df_chicago_built.select(f.avg(f.col('price'))).collect()[0][0]
avarage_built_int = int(avarage_built)
df_chicago_built_filled = df_chicago_built.withColumn('price', f.when(
    f.col('price').isNull(), avarage_built_int).otherwise(f.col('price')))
# df_chicago_built_filled.show()
"""
+-----+---------+--------+                                                      
|price|yearbuilt|location|
+-----+---------+--------+
|   56|     1995| Chicago|
|   55|     2001| Chicago|
|   55|     2001| Chicago|
|   56|     1998| Chicago|
|   58|     2003| Chicago|
|   64|     2005| Chicago|
|   44|     1999| Chicago|
|   49|     2008| Chicago|
|   70|     2002| Chicago|
|   72|     2006| Chicago|
|   82|     1997| Chicago|
|   85|     null| Chicago|
|   45|     1996| Chicago|
|   47|     2000| Chicago|
|   49|     2004| Chicago|
|   56|     2007| Chicago|
|   60|     1995| Chicago|
|   62|     2001| Chicago|
|   64|     2003| Chicago|
|   66|     2002| Chicago|
+-----+---------+--------+
only showing top 20 rows
"""

# Removendo valores faltantes do DataFrame Chicago House Price
df_chicago_built_dropped = df_chicago_built_filled.na.drop()
# df_chicago_built_dropped.show()
"""
+-----+---------+--------+                                                      
|price|yearbuilt|location|
+-----+---------+--------+
|   56|     1995| Chicago|
|   55|     2001| Chicago|
|   55|     2001| Chicago|
|   56|     1998| Chicago|
|   58|     2003| Chicago|
|   64|     2005| Chicago|
|   44|     1999| Chicago|
|   49|     2008| Chicago|
|   70|     2002| Chicago|
|   72|     2006| Chicago|
|   82|     1997| Chicago|
|   45|     1996| Chicago|
|   47|     2000| Chicago|
|   49|     2004| Chicago|
|   56|     2007| Chicago|
|   60|     1995| Chicago|
|   62|     2001| Chicago|
|   64|     2003| Chicago|
|   66|     2002| Chicago|
|   38|     2001| Chicago|
+-----+---------+--------+
only showing top 20 rows
"""

# Removendo linhas duplicadas do DataFrame Chicago House Price
# print(df_chicago_house_price.count())
"""
158
"""
df_chicago_house_price_duplicated = df_chicago_house_price_dropped.distinct()
# print(df_chicago_house_price_duplicated.count())
"""
127
"""

# Removendo linhas duplicadas do DataFrame Chicago House Built
# print(df_chicago_built.count())
"""
103
"""
df_chicago_built_duplicated = df_chicago_built_dropped.distinct()
# print(df_chicago_built_duplicated.count())
"""
81
"""

# Salvando DataFrame em CSV com opção de sobrescrever o arquivo
df_chicago_house_price_duplicated.write.mode('overwrite').csv(
    'Aula_11/data/processed/cleaned_chicago_house_price', header=True)
df_chicago_built_duplicated.write.mode('overwrite').csv(
    'Aula_11/data/processed/cleaned_chicago_built', header=True)
