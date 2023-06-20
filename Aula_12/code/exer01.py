import pandas as pd
import plotly.express as px
from dash import Dash, dash_table, dcc, html

# Caminho do arquivo
gapminder_data_path = "Aula_12/data/raw/gapminder2007_data.csv"

# Leitura do arquivo e armazenamento em DataFrame
df_gapminder = pd.read_csv(gapminder_data_path)

# Agrupamento por continente
# continent_group = df_gapminder.groupby('continent')['gdpPercap']

# for continent, group in continent_group:
#     print(f"Continente: {continent}")
#     print(group)
#     print("----------------------")

# print(continent_mean)

# Calcula as médias do PIB per capita por continente
continent_mean = df_gapminder.groupby('continent')['gdpPercap'].mean().reset_index()

# Inicializa o app
app = Dash(__name__)

# Layout do app
app.layout = html.Div([
    html.H1(children='Meu Primeiro App com Dados e Gráfico'),
    html.Span(children='Médias do PIB por continente'),
    dash_table.DataTable(data=df_gapminder.to_dict('records'), page_size=8),
    dcc.Graph(figure=px.histogram(continent_mean,
              x='continent', y='gdpPercap', histfunc='avg'))
])

if __name__ == '__main__':
    # Executa o app
    app.run_server(debug=True)
