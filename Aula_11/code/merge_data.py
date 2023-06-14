import pyspark.sql.functions as f
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('marge_files').getOrCreate()

# Caminho dos arquivos limpos
cleaned_chicago_house_price = "Aula_11/data/processed/cleaned_chicago_house_price"
cleaned_chicago_built = "Aula_11/data/processed/cleaned_chicago_built"

# Leitura dos arquivos CSV particionados
df_chicago_house_price = spark.read.format('csv').option(
    'header', True).load(cleaned_chicago_house_price)
df_chicago_built = spark.read.format('csv').option(
    'header', True).load(cleaned_chicago_built)

# Une os DataFrames em um único DataFrame
df_merged = df_chicago_house_price.join(
    df_chicago_built, on='price', how='inner')

# Seleciona todas as colunas do DataFrame resultante
all_columns = [f.col(column) for column in df_merged.columns]
# Exibe o DataFrame resultante
df_merged.select(all_columns).show()
"""
+-----+-------+-----+----+---+----+--------+------+---------+---------+--------+
|price|bedroom|space|room|lot| tax|bathroom|garage|condition|yearbuilt|location|
+-----+-------+-----+----+---+----+--------+------+---------+---------+--------+
|   46|      2|  696|   4| 30| 440|     2.0|   1.0|        0|     2002| Chicago|
|   46|      2|  696|   4| 30| 440|     2.0|   1.0|        0|     2005| Chicago|
|   46|      2|  696|   4| 30| 440|     2.0|   1.0|        0|     1997| Chicago|
|   60|      2|  828|   5| 35| 913|     1.5|   1.0|        0|     1999| Chicago|
|   60|      2|  828|   5| 35| 913|     1.5|   1.0|        0|     1995| Chicago|
|   62|      4|  951|   7| 30| 895|     2.0|   1.0|        0|     2003| Chicago|
|   62|      4|  951|   7| 30| 895|     2.0|   1.0|        0|     2001| Chicago|
|   46|      2|  856|   5| 27| 745|     1.0|   0.0|        1|     2002| Chicago|
|   46|      2|  856|   5| 27| 745|     1.0|   0.0|        1|     2005| Chicago|
|   46|      2|  856|   5| 27| 745|     1.0|   0.0|        1|     1997| Chicago|
|   57|      5| 1198|   8| 50|1244|     1.5|   0.0|        0|     2004| Chicago|
|   47|      2|  766|   4| 30| 418|     2.0|   1.0|        0|     2003| Chicago|
|   47|      2|  766|   4| 30| 418|     2.0|   1.0|        0|     2000| Chicago|
|   63|      2|  933|   5| 30|1431|     1.0|   1.0|        0|     1995| Chicago|
|   63|      2|  933|   5| 30|1431|     1.0|   1.0|        0|     2005| Chicago|
|   85|      8| 2240|  12| 50|1200|     3.0|   2.0|        0|     1998| Chicago|
|   85|      8| 2240|  12| 50|1200|     3.0|   2.0|        0|     2005| Chicago|
|   56|      3|  939|   5| 35| 867|     1.5|   1.0|        0|     2007| Chicago|
|   56|      3|  939|   5| 35| 867|     1.5|   1.0|        0|     2001| Chicago|
|   56|      3|  939|   5| 35| 867|     1.5|   1.0|        0|     1995| Chicago|
+-----+-------+-----+----+---+----+--------+------+---------+---------+--------+
only showing top 20 rows
"""

# Escrita do resultado em um arquivo CSV
df_merged.write.mode('overwrite').csv('Aula_11/data/merged', header=True)
