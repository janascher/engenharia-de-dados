## 📝 Exercício da Aula 01.2 - Introdução a Engenharia de Dados

### Questão 01 - Considere que você está trabalhando em um projeto de análise de dados para uma empresa que coleta informações sobre vendas. A empresa possui uma grande quantidade de dados, incluindo informações de vendas diárias de diferentes produtos em várias lojas em todo o país. Você foi solicitado a criar um modelo de dados que possa ser usado para responder a diferentes tipos de perguntas de análise, como vendas totais por loja ou produto, crescimento de vendas ao longo do tempo, correlação entre vendas de diferentes produtos, etc. Descreva como você modelaria os dados para atender aos requisitos acima, levando em consideração a granularidade (menos granularidade ou mais granularidade), esquema estrela ou floco de neve, índices e particionamento. Também descreva que tipo de processamento (lotes ou tempo real) mais se encaixa. Explique suas escolhas e justifique suas decisões.

Para modelar os dados da empresa e atender aos requisitos de análise de vendas, eu sugeriria a seguinte abordagem:

-   **Granularidade:**
    A granularidade dos dados refere-se ao nível de detalhe das informações registradas. Nesse caso, considerando que a empresa possui informações de vendas diárias de diferentes produtos em várias lojas, é recomendado manter uma granularidade diária. Isso permitirá análises mais detalhadas e flexíveis ao responder diferentes tipos de perguntas, como o desempenho das vendas em um determinado dia ou ao longo de um período específico.

-   **Esquema estrela:**
    Para criar o modelo de dados, sugiro utilizar o esquema estrela. Esse esquema é composto por uma tabela de fatos central, que armazena as medidas de vendas (como o valor da venda) e as chaves estrangeiras para as tabelas de dimensão, que contêm informações detalhadas sobre as lojas, produtos, tempo, etc. Essa abordagem facilita a análise, permitindo uma consulta eficiente e simplificada.

-   **Índices:**
    Ao criar o modelo de dados, é recomendado criar índices nas colunas que são frequentemente usadas para filtrar ou agrupar os dados, como as chaves de dimensão e colunas de datas. Isso melhorará o desempenho das consultas, permitindo uma recuperação mais rápida dos dados necessários para responder às perguntas de análise.

-   **Particionamento:**
    Considerando que a empresa possui uma grande quantidade de dados, é aconselhável realizar o particionamento dos dados. O particionamento envolve dividir os dados em partições com base em um critério, como a data de vendas. Isso ajuda na otimização do armazenamento e no desempenho das consultas, permitindo a recuperação mais eficiente dos dados relevantes para uma determinada análise.

-   **Processamento em lote:**
    Dado que as informações sobre as vendas são coletadas diariamente, o processamento em lote é mais adequado para esse cenário. O processamento em lote envolve a execução de tarefas em um cronograma programado, como processar todas as vendas do dia anterior durante a noite, quando a carga no sistema é menor. Esse tipo de processamento é ideal para análises retrospectivas e permite o processamento eficiente de grandes volumes de dados.
