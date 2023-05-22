## 📝 Exercício da Aula 05 - Manipulação e limpeza de dados com Pandas

### Questão 01

-   Num primeiro código, leia dois grandes arquivos baixados da internet utilizando o Pandas e armazene-os em `DataFrames`;
-   Utilize o método `head()` para exibir as primeiras 5 linhas dos DataFrames;
-   Utilize o método `info()` para exibir informações sobre os DataFrames, como o número de linhas, o tipo de dado de cada coluna, etc;
-   Utilize o método `describe()` para obter estatísticas descritivas sobre as colunas numéricas dos DataFrames;
-   Preencha os valores faltantes dos DataFrames utilizando o método `fillna()` com algum valor que faça sentido ou apenas retire-os dos DataFrames utilizando `dropna()`;
-   Remova as linhas duplicadas dos DataFrames utilizando o método `drop_duplicates()`;
-   Salve os DataFrames em arquivos CSV chamados _{nome_original}\_limpos.csv_ utilizando o método `to_csv()`;
-   Num segundo código, leia os dois CSVs, faça um merge entre eles e escreva o resultado num arquivo CSV.
