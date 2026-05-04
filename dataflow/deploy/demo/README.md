# Демонстрационный тракт `shop-postgres-to-starrocks`

Содержит воспроизводимый комплект из подраздела 3.9 ВКР: декларативную
спецификацию тракта, инициализационные DDL-скрипты PostgreSQL и StarRocks,
а также генератор нагрузки.

## Развёртывание

1. Поднять полный стенд:
   ```
   cd ../
   docker compose up -d
   ```
2. Дождаться готовности контейнеров (≈ 2 минуты).
3. Зарегистрировать источник:
   ```
   curl -u admin:admin -X POST http://localhost:8080/api/v1/sources/shop \
        -H 'Content-Type: application/json' \
        --data-binary @demo/source.json
   ```
4. Зарегистрировать тракт:
   ```
   curl -u admin:admin -X POST http://localhost:8080/api/v1/tracts \
        -H 'Content-Type: application/json' \
        --data "{\"document\": $(jq -Rs . demo/tract.yaml)}"
   ```
5. Развернуть тракт:
   ```
   curl -u admin:admin -X POST \
        http://localhost:8080/api/v1/tracts/shop-postgres-to-starrocks/deploy
   ```

## Генератор нагрузки

```
cd demo/load-generator
mvn -ntp -DskipTests package
java -jar target/load-generator.jar \
     --url=jdbc:postgresql://localhost:5432/shop \
     --user=dataflow_cdc --password=dataflow_cdc \
     --rate=10000 --duration=300 --threads=4
```

Профиль операций — 60 % INSERT / 30 % UPDATE / 10 % DELETE по таблицам
`customers`, `orders`, `order_items`, `products`. Целевая нагрузка 10 000
событий в секунду; реальный потолок упирается в Stream Load в одноузловой
конфигурации StarRocks.
