# Solução — WeCogno Data Engineering Challenge

Este diretório contém a implementação do desafio técnico de Engenharia de Dados da WeCogno.

A solução foi desenvolvida utilizando Apache Airflow, PostgreSQL e Docker, com o objetivo de construir um processo de ETL para os dados de corridas de táxi de Nova York referentes ao ano de 2022.

## Tecnologias utilizadas

* Python
* Apache Airflow 3.1.0
* PostgreSQL 16
* Docker / Docker Compose
* Pandas
* PyArrow

## Arquitetura

O processo foi organizado em três camadas de dados:

```text
Parquet
   │
   ▼
┌──────────┐
│   RAW    │
└────┬─────┘
     │
     ▼
┌──────────┐
│ TRUSTED  │
└────┬─────┘
     │
     ▼
┌──────────┐
│ REFINED  │
└──────────┘
```

### Raw

A camada `raw` recebe os dados diretamente dos arquivos Parquet.

Foi mantida a estrutura original dos dados, realizando apenas a padronização dos nomes das colunas para `snake_case`.

### Trusted

Na camada `trusted`, os dados são transformados para uma estrutura mais consistente para utilização nas etapas seguintes.

As colunas de data e hora são padronizadas:

* `tpep_pickup_datetime` → `pickup_datetime`
* `tpep_dropoff_datetime` → `dropoff_datetime`

### Refined

A camada `refined` contém os dados utilizados nas análises solicitadas pelo desafio.

Foram aplicadas as seguintes validações:

* `pickup_datetime` não pode ser nulo;
* `dropoff_datetime` não pode ser nulo;
* `dropoff_datetime` deve ser maior ou igual a `pickup_datetime`;
* `trip_distance` deve ser maior ou igual a zero.

## Estrutura do projeto

```text
challenge/
├── dags/
│   └── nyc_taxi_etl.py
├── sql/
│   └── create_tables.sql
├── docker-compose.yaml
└── README.md
```

Os dados de entrada permanecem no diretório `nyc-tlc-data`, localizado na raiz do projeto:

```text
data-engineering-challenge/
├── README.md
├── nyc-tlc-data/
│   ├── yellow_tripdata_2022-01.parquet.gz
│   ├── ...
│   └── yellow_tripdata_2022-12.parquet.gz
└── challenge/
    ├── dags/
    ├── sql/
    ├── docker-compose.yaml
    └── README.md
```

## Configuração do ambiente

A infraestrutura é executada utilizando Docker Compose.

O ambiente possui:

* Apache Airflow;
* PostgreSQL;
* Airflow Scheduler;
* Airflow DAG Processor;
* Airflow API Server.

O PostgreSQL utilizado pelo desafio é o mesmo banco utilizado pelo ambiente do Airflow, com as tabelas do desafio organizadas nos schemas `raw`, `trusted` e `refined`.

### Variáveis de ambiente

As chaves utilizadas pelo Airflow não são armazenadas diretamente no `docker-compose.yaml`.

Crie um arquivo `.env` dentro do diretório `challenge` contendo:

```env
AIRFLOW_JWT_SECRET=defina_um_valor
AIRFLOW_FERNET_KEY=defina_um_valor
```

O `docker-compose.yaml` utiliza essas variáveis para configurar:

```yaml
AIRFLOW__API_AUTH__JWT_SECRET: "${AIRFLOW_JWT_SECRET}"
AIRFLOW__CORE__FERNET_KEY: "${AIRFLOW_FERNET_KEY}"
```

O arquivo `.env` deve permanecer fora do controle de versão.

## Execução

A partir do diretório `challenge`, execute:

```bash
docker compose up -d
```

Depois, verifique o estado dos containers:

```bash
docker compose ps
```

O Airflow fica disponível em:

```text
http://localhost:8080
```

Após a inicialização do ambiente, o DAG `nyc_taxi_etl` pode ser executado pela interface do Airflow ou pela CLI:

```bash
docker compose exec airflow-scheduler airflow dags trigger nyc_taxi_etl
```

## Processo de ETL

O DAG `nyc_taxi_etl` é composto pelas seguintes etapas:

```text
create_tables
      │
      ▼
load_raw_data
      │
      ▼
transform_trusted
      │
      ▼
load_refined
      │
      ▼
quality_checks
```

### 1. `create_tables`

Cria os schemas e tabelas necessários no PostgreSQL:

* `raw.yellow_tripdata`
* `trusted.yellow_tripdata`
* `refined.yellow_tripdata`

O comando utilizado está no arquivo:

```text
sql/create_tables.sql
```

### 2. `load_raw_data`

Percorre os arquivos:

```text
yellow_tripdata_2022-01.parquet.gz
...
yellow_tripdata_2022-12.parquet.gz
```

Os arquivos são lidos utilizando Pandas/PyArrow e carregados na tabela:

```text
raw.yellow_tripdata
```

### 3. `transform_trusted`

Copia os dados da camada `raw` para a camada `trusted`, realizando a padronização dos nomes das colunas de data e hora.

### 4. `load_refined`

Carrega os dados válidos na camada `refined`, aplicando as regras de qualidade descritas anteriormente.

### 5. `quality_checks`

Realiza uma validação final para garantir que a tabela `refined.yellow_tripdata` não esteja vazia.

## Resultados

Após a execução do processo de ETL, foram obtidos os seguintes resultados.

### 1. Qual o total de registros na tabela final?

**39.642.485 registros**

Query utilizada:

```sql
SELECT COUNT(*) AS total_records
FROM refined.yellow_tripdata;
```

### 2. Qual o total de viagens iniciadas e finalizadas no dia 17 de junho?

**Viagens iniciadas:** 125.586

**Viagens finalizadas:** 125.342

Query utilizada:

```sql
SELECT
    COUNT(*) FILTER (
        WHERE pickup_datetime::date = DATE '2022-06-17'
    ) AS trips_started_on_june_17,
    COUNT(*) FILTER (
        WHERE dropoff_datetime::date = DATE '2022-06-17'
    ) AS trips_finished_on_june_17
FROM refined.yellow_tripdata;
```

### 3. Qual foi o dia da viagem mais longa percorrida?

A maior distância encontrada foi:

* **Data:** 28/10/2022
* **Distância:** 389.678,46
* **Início:** 2022-10-28 05:19:00
* **Fim:** 2022-10-28 05:32:00

Query utilizada:

```sql
SELECT
    pickup_datetime::date AS trip_day,
    trip_distance,
    pickup_datetime,
    dropoff_datetime
FROM refined.yellow_tripdata
WHERE trip_distance = (
    SELECT MAX(trip_distance)
    FROM refined.yellow_tripdata
);
```

O valor encontrado representa um possível outlier, considerando a distância registrada em relação às demais viagens.

O registro foi mantido na tabela final porque o desafio não especifica uma regra para identificação ou remoção de outliers. Dessa forma, a solução preserva os dados de origem e deixa explícita a ocorrência para análise.

### 4. Estatísticas da distribuição de distância percorrida

| Estatística   |      Valor |
| ------------- | ---------: |
| Média         |   5,959406 |
| Desvio padrão | 599,293571 |
| Mínimo        |          0 |
| Q1            |        1,1 |
| Mediana       |        1,9 |
| Q3            |       3,56 |
| Máximo        | 389.678,46 |

Query utilizada:

```sql
SELECT
    AVG(trip_distance) AS mean,
    STDDEV_POP(trip_distance) AS standard_deviation,
    MIN(trip_distance) AS min,
    MAX(trip_distance) AS max,
    PERCENTILE_CONT(0.25)
        WITHIN GROUP (ORDER BY trip_distance) AS q1,
    PERCENTILE_CONT(0.50)
        WITHIN GROUP (ORDER BY trip_distance) AS median,
    PERCENTILE_CONT(0.75)
        WITHIN GROUP (ORDER BY trip_distance) AS q3
FROM refined.yellow_tripdata;
```

A diferença entre a mediana e a média, assim como o desvio padrão elevado, é influenciada principalmente pelo valor extremo observado na distância máxima.

## Decisões técnicas

### Separação em camadas

Foi utilizada a separação `raw → trusted → refined` para deixar explícita a evolução dos dados durante o processo de ETL.

Essa estrutura facilita a rastreabilidade e permite identificar em qual etapa uma transformação ou regra de qualidade foi aplicada.

### Manutenção do registro extremo

O registro com distância de `389.678,46` não foi removido.

Apesar de aparentar ser um outlier, não foi definida uma regra estatística ou de negócio no enunciado para determinar quais registros deveriam ser descartados.

Por esse motivo, a decisão foi manter o registro e documentar seu impacto nas estatísticas.

### PostgreSQL

O PostgreSQL foi utilizado tanto para armazenamento dos dados quanto para execução das transformações SQL.

### Airflow

O Airflow foi utilizado para orquestrar o processo, garantindo uma sequência definida entre criação das tabelas, ingestão, transformação e validação.

## Possíveis melhorias

Algumas melhorias poderiam ser implementadas em uma evolução da solução:

* utilizar `COPY` ou outro mecanismo de bulk loading do PostgreSQL para acelerar a ingestão dos arquivos;
* tornar a carga da camada `raw` totalmente idempotente, evitando duplicações em caso de reexecução do DAG;
* adicionar verificações de qualidade mais abrangentes, como análise de valores nulos, limites de distância e consistência de valores financeiros;
* adicionar testes automatizados para as transformações;
* adicionar monitoramento e alertas para falhas na execução do pipeline;
* parametrizar o período de dados a ser processado.

Essas melhorias não foram adicionadas à implementação principal para manter a solução objetiva e alinhada ao escopo solicitado no desafio.
