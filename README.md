# Dataflow Control Plane

Система управления трактами передачи потоков бизнес-данных от реляционных
систем-источников в целевые аналитические хранилища.

Реализация декларативного управления near real-time трактами на основе паттерна
согласования желаемого и наблюдаемого состояний (`desired state reconciliation`)
над открытым стеком Debezium, Apache Kafka и Apache Flink.

## Структура репозитория

```
dataflow/
  control-plane/        Maven multi-module проект (Java 17, Spring Boot 3)
    domain/             доменные сущности и порты к внешним системам
    application/        use cases и оркестрация
    infrastructure/     адаптеры портов (Kafka, Connect, Flink, JDBC)
    reconciler/         цикл согласования и атомарные ReconcileSteps
    observer/           опрос data-plane и экспорт метрик в Prometheus
    validator/          синтаксическая и семантическая валидация
    api/                REST-контроллеры и OpenAPI 3.0 спецификация
    bootstrap/          точка входа Spring Boot
  flink-worker/         универсальное Flink-задание (отдельный shaded JAR)
  deploy/
    docker-compose.yml  локальное развёртывание полного стека
    Dockerfile.control-plane
    prometheus.yml
    helm/control-plane/ Helm-чарт промышленной установки
    demo/
      tract.yaml          декларативная спецификация демо-тракта
      source.json         тело POST /api/v1/sources/shop
      pg-init.sql         DDL источника (customers, orders, order_items, products)
      starrocks-init.sql  DDL целевых таблиц в Primary Key Model
      load-generator/     генератор имитированной нагрузки 60/30/10
      README.md           пошаговый воспроизводимый сценарий
```

## Демонстрационный тракт

Демо-тракт `shop-postgres-to-starrocks` определён в
[`dataflow/deploy/demo/tract.yaml`](dataflow/deploy/demo/tract.yaml) и
описывает захват изменений из четырёх таблиц PostgreSQL (`customers`, `orders`,
`order_items`, `products`), их дедупликацию по составному ключу
`(lsn, txId, sequence)`, фильтрацию `TRUNCATE`-событий и загрузку в одноимённые
таблицы StarRocks с использованием Primary Key Model.

## Локальное развёртывание

```
cd dataflow/deploy
docker compose up -d
```

Полный стек поднимается за время порядка двух минут:

| Сервис            | Назначение                                      | Порт |
|-------------------|-------------------------------------------------|------|
| `control-plane`   | REST API системы управления                     | 8080 |
| `postgres-meta`   | хранилище метаданных control plane              | 5433 |
| `postgres-source` | PG-источник с `wal_level=logical`               | 5432 |
| `kafka`           | KRaft-брокер                                    | 9094 |
| `kafka-connect`   | Debezium PostgreSQL Connector                   | 8083 |
| `flink-jobmanager`| REST + Web UI                                   | 8081 |
| `flink-taskmanager` | × 2 реплики                                  | —    |
| `starrocks-fe`    | FE: HTTP Stream Load 8030, MySQL-протокол 9030  | 8030, 9030 |
| `starrocks-be`    | BE                                               | —    |
| `prometheus`      | сбор метрик                                     | 9090 |
| `grafana`         | визуализация                                    | 3000 |

После старта сценарий из
[`dataflow/deploy/demo/README.md`](dataflow/deploy/demo/README.md) разворачивает
и запускает демо-тракт одной серией `curl`-запросов.

## REST API

Базовая группа эндпоинтов в соответствии с подразделом 3.3 ВКР:

| Метод и путь                              | Назначение                          |
|-------------------------------------------|-------------------------------------|
| `POST /api/v1/sources/{name}`             | регистрация источника               |
| `DELETE /api/v1/sources/{name}`           | удаление источника                  |
| `POST /api/v1/tracts`                     | создание тракта по спецификации     |
| `GET /api/v1/tracts`                      | перечень всех трактов               |
| `GET /api/v1/tracts/{name}`               | получение спецификации              |
| `PUT /api/v1/tracts/{name}`               | обновление спецификации             |
| `DELETE /api/v1/tracts/{name}`            | удаление тракта                     |
| `POST /api/v1/tracts/{name}/deploy`       | развёртывание                       |
| `POST /api/v1/tracts/{name}/suspend`      | приостановка                        |
| `POST /api/v1/tracts/{name}/resume`       | возобновление                       |
| `GET /api/v1/tracts/{name}/status`        | наблюдаемое состояние               |
| `GET /api/v1/tracts/{name}/events`        | журнал согласования тракта          |
| `GET /api/v1/tracts/{name}/dlq`           | содержимое DLQ-топика тракта        |
| `GET /actuator/health`                    | состояние самой системы             |
| `GET /actuator/prometheus`                | метрики в формате Prometheus        |

OpenAPI 3.0: фрагмент в
[`dataflow/control-plane/api/src/main/resources/openapi/control-plane.yaml`](dataflow/control-plane/api/src/main/resources/openapi/control-plane.yaml),
полная спецификация генерируется автоматически и доступна по адресу
`http://localhost:8080/v3/api-docs`. Swagger UI — `http://localhost:8080/swagger-ui`.

## Конфигурационные параметры

Перечислены ключевые ветви `application.yml`. Полный список — в
[`dataflow/control-plane/bootstrap/src/main/resources/application.yml`](dataflow/control-plane/bootstrap/src/main/resources/application.yml).

| Ветвь                                     | Назначение                                    |
|-------------------------------------------|-----------------------------------------------|
| `dataflow.reconciler.period`              | период такта согласования (по умолчанию 10 с) |
| `dataflow.reconciler.stuck-after`         | порог перевода в `Stuck`                      |
| `dataflow.reconciler.max-attempts`        | максимум попыток на шаг                       |
| `dataflow.observer.flink-probe-interval`  | период опроса Flink                           |
| `dataflow.kafka.bootstrap-servers`        | Kafka                                         |
| `dataflow.connect.url`                    | Kafka Connect REST                            |
| `dataflow.flink.url`                      | Flink REST                                    |
| `dataflow.flink.worker-jar-path`          | путь к `flink-worker.jar` в Flink-кластере    |
| `dataflow.flink.checkpoint-storage`       | долговечное хранилище чекпойнтов              |
| `dataflow.security.admin.user/password`   | админ для HTTP Basic                          |

Секреты в декларативной спецификации (`${secret:...}`) разрешаются из переменных
окружения `DATAFLOW_SECRETS_*` или из любого источника свойств Spring.

## Промышленное развёртывание

Helm-чарт лежит в [`dataflow/deploy/helm/control-plane`](dataflow/deploy/helm/control-plane).
Чарт предполагает, что компоненты data-plane (Kafka, Kafka Connect, Flink)
развёрнуты в кластере независимо собственными чартами или операторами.

```
helm install dataflow ./dataflow/deploy/helm/control-plane \
  --set kafka.bootstrapServers=kafka.kafka:9092 \
  --set connect.url=http://debezium.kafka:8083 \
  --set flink.url=http://flink-rest.flink:8081
```

## Сборка

```
cd dataflow/control-plane
./mvnw -ntp -DskipTests package

cd ../flink-worker
mvn -ntp -DskipTests package
# артефакт target/flink-worker.jar следует положить в
# /opt/flink/usrlib/ Flink-кластера
```

## Соответствие тексту ВКР

| Раздел ВКР | Артефакты в репозитории                                                                 |
|-----------:|------------------------------------------------------------------------------------------|
| 2.1        | `domain/tract/{DesiredState,ReconciliationStatus,Tract,TractStatus}`                     |
| 2.2        | модули `api`, `application`, `reconciler`, `observer`, `validator`, `infrastructure`     |
| 2.3        | `infrastructure/persistence/V1__init.sql` + `domain/spec/*` записи                       |
| 2.4        | `reconciler/plan/ReconcilePlanner` + 14 атомарных шагов в `reconciler/steps/*`           |
| 2.5        | `flink-worker/sink/{PoisonGuard,DlqRouter}`                                              |
| 2.6        | `observer/MetricsExporter`                                                                |
| 3.1        | `pom.xml` верхнего уровня                                                                |
| 3.2        | таблица модулей выше                                                                     |
| 3.3        | `api/src/main/resources/openapi/control-plane.yaml`                                       |
| 3.4        | `reconciler/{ReconcilerScheduler,ReconcileLoop,plan/PlanExecutor,plan/ReconcilePlanner}` |
| 3.5        | `flink-worker/*`                                                                          |
| 3.6        | `observer/*`                                                                              |
| 3.7        | `validator/{YamlSpecParser,SchemaValidator,SemanticValidator,SmokeTestRunner,diff/*}`    |
| 3.8        | `deploy/{docker-compose.yml,Dockerfile.control-plane,helm/}`                              |
| 3.9        | `deploy/demo/*`                                                                           |
