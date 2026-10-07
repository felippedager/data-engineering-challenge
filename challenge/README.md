# Data Engineering Challenge

Pipeline de dados desenvolvido para processamento dos dados de viagens de táxi de Nova York (NYC TLC), utilizando Apache Airflow, PostgreSQL e Docker.

## 1. Objetivo

Construir um pipeline ETL capaz de:

* Ler os dados mensais de viagens de táxi disponibilizados em formato Parquet;
* Carregar os dados em PostgreSQL;
* Organizar os dados em camadas `raw`, `trusted` e `refined`;
* Aplicar validações básicas de qualidade;
* Responder às perguntas propostas no desafio.

## 2. Tecnologias utilizadas

* Python
* Apache Airflow 3.1.0
* PostgreSQL 16
* Docker / Docker Compose
* Pandas
* PyArrow

## 3. Estrutura do projeto

```text
data-engineering-challenge/
├── challenge/
│   ├── dags/
│   │   └── nyc_taxi_etl.py
│   ├── sql/
│   │   └── create_tables.sql
│   ├── docker-compose.yaml
│   └── README.md
│
└── nyc-tlc-data/
    ├── yellow_tripdata_2022-01.parquet.gz
    ├── yellow_tripdata_2022-02.parquet.gz
    ├── ...
    └── yellow_tripdata_2022-12.parquet.gz
```

Os arquivos de dados são montados no container do Airflow em modo somente leitura.

## 4. Arquitetura do ETL

O pipeline foi dividido em três camadas:

```text
Parquet
   │
   ▼
┌─────────────┐
│     RAW     │
│ Dados brutos│
└─────────────┘
   │
   ▼
┌─────────────┐
│   TRUSTED   │
│ Padronização│
└─────────────┘
   │
   ▼
┌─────────────┐
│   REFINED   │
│ Dados finais│
└─────────────┘
```

### Raw

A camada `raw` recebe os dados diretamente dos arquivos Parquet, mantendo a estrutura original dos registros e realizando apenas a padronização dos nomes das colunas para `snake_case`.

Tabela:

```text
raw.yellow_tripdata
```

### Trusted

A camada `trusted` recebe os dados da camada `raw` e padroniza os nomes dos campos de data:

* `tpep_pickup_datetime` → `pickup_datetime`
* `tpep_dropoff_datetime` → `dropoff_datetime`

Tabela:

```text
trusted.yellow_tripdata
```

### Refined

A camada `refined` contém os dados utilizados nas análises finais.

Foram aplicadas as seguintes validações:

* `pickup_datetime` não pode ser nulo;
* `dropoff_datetime` não pode ser nulo;
* `dropoff_datetime` deve ser maior ou igual a `pickup_datetime`;
* `trip_distance` deve ser maior ou igual a zero.

Tabela:

```text
refined.yellow_tripdata
```

## 5. DAG

O DAG utilizado foi:

```text
nyc_taxi_etl
```

O fluxo de execução é:

```text
create_tables
      ↓
load_raw_data
      ↓
transform_trusted
      ↓
load_refined
      ↓
quality_checks
```

### `create_tables`

Cria os schemas e tabelas necessários no PostgreSQL:

* `raw`
* `trusted`
* `refined`

### `load_raw_data`

Percorre os 12 arquivos mensais de 2022, lê os dados com Pandas e insere os registros na tabela `raw.yellow_tripdata`.

### `transform_trusted`

Copia os dados da camada `raw` para `trusted`, realizando a padronização dos nomes das colunas de data.

### `load_refined`

Carrega os dados da camada `trusted` para `refined` aplicando as validações de qualidade definidas.

### `quality_checks`

Verifica se a tabela final contém registros.

## 6. Execução

Com Docker e Docker Compose instalados, acessar a pasta:

```bash
cd challenge
```

Inicializar os containers:

```bash
docker compose up -d
```

O Airflow estará disponível na porta:

```text
http://localhost:8080
```

Após a inicialização, o DAG `nyc_taxi_etl` pode ser executado manualmente pela interface do Airflow.

## 7. Resultados

Após a execução completa do pipeline, a tabela final `refined.yellow_tripdata` apresentou:

### 7.1 Total de registros

```sql
SELECT COUNT(*) AS total_records
FROM refined.yellow_tripdata;
```

Resultado:

```text
39.642.485
```

### 7.2 Viagens iniciadas e finalizadas em 17 de junho

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

Resultado:

| Métrica                           |   Total |
| --------------------------------- | ------: |
| Viagens iniciadas em 17/06/2022   | 125.586 |
| Viagens finalizadas em 17/06/2022 | 125.342 |

### 7.3 Dia da viagem mais longa

A maior distância registrada foi identificada com:

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

Resultado:

```text
Dia: 28/10/2022
Distância: 389.678,46
Início: 05:19:00
Fim: 05:32:00
```

### 7.4 Estatísticas de `trip_distance`

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

Resultado:

| Estatística   |      Valor |
| ------------- | ---------: |
| Média         |     5,9594 |
| Desvio padrão |   599,2936 |
| Mínimo        |          0 |
| Q1 (25%)      |       1,10 |
| Mediana (50%) |       1,90 |
| Q3 (75%)      |       3,56 |
| Máximo        | 389.678,46 |

## 8. Observação sobre qualidade dos dados

Durante a análise foi identificado um valor extremo de `trip_distance` de `389.678,46`.

O registro possui horário de início às `05:19` e término às `05:32`, indicando uma duração de aproximadamente 13 minutos.

Como o desafio não definiu uma regra específica para tratamento de outliers, o registro foi mantido na camada `refined` e considerado nos cálculos estatísticos.

A decisão foi evitar a remoção ou alteração de registros com base em uma regra de negócio que não foi especificada no escopo do desafio.

## 9. Decisões técnicas

### Padronização das colunas

Os nomes das colunas foram convertidos para `snake_case`, mantendo os dados originais e facilitando a utilização no PostgreSQL.

### Tipos de dados

Os tipos foram definidos considerando os tipos encontrados nos arquivos Parquet. Campos como `passenger_count` e `ratecode_id` foram mantidos como `DOUBLE PRECISION`, pois foram identificados como valores `float64` na origem.

### Camadas de dados

A separação entre `raw`, `trusted` e `refined` foi utilizada para deixar explícitas as diferentes etapas do processamento e facilitar a rastreabilidade dos dados.

### Tratamento de outliers

Não foram aplicadas regras arbitrárias para remoção de outliers. Valores extremos foram mantidos quando não havia uma regra de negócio definida no desafio.

## 10. Possíveis melhorias

Como evolução do pipeline, algumas melhorias poderiam ser consideradas:

* Utilizar PostgreSQL `COPY` ou outra estratégia de bulk load para otimizar a ingestão de grandes volumes;
* Tornar a carga da camada `raw` totalmente idempotente para permitir reexecuções sem duplicação;
* Adicionar checks de qualidade mais abrangentes;
* Implementar monitoramento de volume e duração das etapas;
* Adicionar testes automatizados para as transformações;
* Parametrizar o período dos arquivos processados.
