# Pipeline de Engenharia de Dados de Streaming (PySpark + Databricks)

Pipeline de engenharia de dados em larga escala construído com PySpark no ambiente Databricks, simulando o ciclo de vida de dados de uma plataforma de streaming de vídeo: ingestão de logs brutos, limpeza, enriquecimento, cálculo de métricas de negócio e disponibilização de uma camada analítica governada.

## Sobre o Projeto

O pipeline processa a **Netflix User Watching Behavior Dataset** (Kaggle, ~50.000 registros), com dados de comportamento de consumo de usuários de uma plataforma de streaming: idade, país, gênero de conteúdo favorito, tempo médio assistido, taxa de conclusão, avaliação dada, dispositivo utilizado, entre outras variáveis.

O fluxo simula o ciclo de vida de dados de uma plataforma de streaming: dados brutos chegam via CSV, passam por validação e limpeza, são enriquecidos com categorizações de negócio, ganham métricas calculadas, e são disponibilizados numa camada analítica governada, pronta para consulta e tomada de decisão.

## Componentes Principais do Pipeline

**Ingestão**: leitura do CSV bruto via `spark.read`, com inferência de schema, direto de um Volume do Unity Catalog no Databricks.

**Auditoria e limpeza de dados (`clean_data`)**: antes de qualquer transformação, o dataset passou por uma auditoria completa (contagem de nulos por coluna via `summary()`, verificação de valores distintos em todas as colunas categóricas, checagem de min/max nas numéricas). O dataset original já veio limpo (zero nulos, sem inconsistência categórica), mas a função de limpeza foi construída mesmo assim como uma camada de defesa: remove registros com `user_id` nulo e corrige valores fora da faixa esperada em `completion_rate` (0-100) e `rating_given` (1.0-5.0) — regras que protegem o pipeline caso uma execução futura receba dados diferentes dos de hoje. A lógica foi validada com um conjunto de dados "sujo" criado propositalmente para testar cada regra antes de rodar em cima dos dados reais.

**Normalização e enriquecimento (`apply_mapping`)**: função reutilizável que constrói dinamicamente uma expressão `when/otherwise` a partir de um dicionário de mapeamento, evitando repetição de código. Usada para: agrupar os 8 gêneros de conteúdo em 4 categorias maiores (`genre_category`) e agrupar os 4 tipos de dispositivo em 2 categorias de uso (`device_category`).

**Métricas de negócio**: conversão de `avg_watch_time_minutes` para `avg_watch_time_hours` (`add_watch_time_hours`), e categorização de cada sessão em "Curto", "Longo" ou "Muito Longo" (`cat_session`), com base em limites fixos definidos a partir da análise de percentis do próprio dado (`approxQuantile`).

**Armazenamento de alta performance**: escrita final em Parquet, particionado por `country`, com `repartition("country")` aplicado antes da escrita para evitar o problema de arquivos pequenos (múltiplos arquivos por partição gerados por um `repartition` numérico desalinhado com a coluna de particionamento).

**Governança com Unity Catalog**: além dos arquivos particionados, o resultado final também foi registrado como uma **tabela gerenciada** no Unity Catalog (`projeto.default.netflix_clean`), via `saveAsTable`. Isso permite consultar os dados diretamente por SQL, sem precisar conhecer o caminho físico dos arquivos, e centraliza metadado, schema e documentação num só lugar. A tabela e suas colunas mais relevantes (`genre_category`, `device_category`, `session_category`, `avg_watch_time_hours`) foram documentadas com `COMMENT`.

## Decisões de Design

**Agrupamento de gêneros em 4 categorias, em vez dos 8 originais**: evita uma categorização genérica demais — 8 valores soltos não ajudam muita análise de alto nível, enquanto 4 grupos (Fiction, Comedy, Drama/Romance, Action/Adventure, Documentary) dão uma visão de negócio mais direta sem perder o sentido dos dados.

**Limites fixos (105 e 200 minutos) para categorizar sessão, calculados via percentil**: limites recalculados a cada execução impediriam comparar "sessão Longa" de um mês com "sessão Longa" de outro — a régua ficaria diferente todo mês, quebrando a comparabilidade histórica. Os números vieram de uma análise real de percentis (`approxQuantile` sobre `avg_watch_time_minutes`), depois congelados como regra de negócio fixa.

**Particionamento do Parquet por `country`**: um dos filtros mais prováveis de serem usados em análises regionais de comportamento de consumo sobre essa camada.

**Tabela gerenciada além dos arquivos Parquet**: arquivos soltos num Volume exigem conhecer o caminho físico exato. Uma tabela gerenciada no Unity Catalog dá um nome estável e consultável por SQL, além de documentação embutida e controle de acesso.

## Tecnologias Utilizadas

- **Linguagem**: Python
- **Framework de Big Data**: PySpark (DataFrame API e SQL Functions)
- **Ambiente de Desenvolvimento**: Databricks Free Edition (sucessor da Community Edition)
- **Governança de Dados**: Unity Catalog (Volumes e tabelas gerenciadas)
- **Fonte de Dados**: Netflix User Watching Behavior Dataset (Kaggle)
- **Formato de Armazenamento**: Parquet particionado por `country`, e tabela gerenciada (Delta) via Unity Catalog
