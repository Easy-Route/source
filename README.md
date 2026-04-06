# Dataflow Control Plane

Система управления трактами передачи потоков бизнес-данных от реляционных
систем-источников в целевые аналитические хранилища.

Реализация декларативного управления near real-time трактами на основе паттерна
согласования желаемого и наблюдаемого состояний (desired state reconciliation)
над открытым стеком Debezium, Apache Kafka и Apache Flink.

## Структура репозитория

```
dataflow/
  control-plane/      Maven multi-module проект (Java 17, Spring Boot 3)
    domain/             доменные сущности и порты
    application/        use cases и оркестрация
    infrastructure/     адаптеры портов (Kafka, Connect, Flink, JDBC)
    reconciler/         цикл согласования и атомарные шаги
    observer/           опрос data-plane и экспорт метрик
    validator/          синтаксическая и семантическая валидация
    api/                REST-контроллеры
    bootstrap/          точка входа Spring Boot
  flink-worker/       универсальное Flink-задание (отдельный shaded JAR)
  deploy/
    docker-compose.yml  локальное развёртывание полного стека
    helm/               чарт для Kubernetes
    demo/               демонстрационный тракт PostgreSQL → StarRocks
```

## Демонстрационный тракт

Демонстрационный тракт `shop-postgres-to-starrocks` определён в
[`dataflow/deploy/demo/tract.yaml`](dataflow/deploy/demo/tract.yaml) и
описывает захват изменений из четырёх таблиц PostgreSQL (`customers`, `orders`,
`order_items`, `products`), их дедупликацию по LSN, фильтрацию TRUNCATE-событий
и загрузку в одноимённые таблицы StarRocks с использованием Primary Key Model.

## Локальное развёртывание

```
cd dataflow/deploy
docker compose up -d
```

Полный стек (PostgreSQL для метаданных, Apache Kafka, Kafka Connect с Debezium
PostgreSQL Connector, Flink JobManager и TaskManager, StarRocks FE/BE,
control plane, Prometheus, Grafana) поднимается за время порядка двух минут.

## REST API

```
POST   /api/v1/sources                        регистрация источника
POST   /api/v1/tracts                         создание тракта по спецификации
GET    /api/v1/tracts                         перечень всех трактов
GET    /api/v1/tracts/{name}                  получение спецификации
PUT    /api/v1/tracts/{name}                  обновление спецификации
DELETE /api/v1/tracts/{name}                  удаление тракта
POST   /api/v1/tracts/{name}/deploy           развёртывание
POST   /api/v1/tracts/{name}/suspend          приостановка
POST   /api/v1/tracts/{name}/resume           возобновление
GET    /api/v1/tracts/{name}/status           наблюдаемое состояние
GET    /api/v1/tracts/{name}/events           журнал событий
GET    /api/v1/tracts/{name}/dlq              содержимое DLQ-топика
GET    /actuator/health                       состояние самой системы
GET    /actuator/prometheus                   метрики в формате Prometheus
```
