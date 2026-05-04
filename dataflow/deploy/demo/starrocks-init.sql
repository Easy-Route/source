-- Целевая аналитическая БД и таблицы Primary Key Model.

CREATE DATABASE IF NOT EXISTS shop_dwh;

USE shop_dwh;

CREATE TABLE IF NOT EXISTS customers (
    id          BIGINT       NOT NULL,
    email       VARCHAR(256) NOT NULL,
    full_name   VARCHAR(256),
    created_at  DATETIME,
    updated_at  DATETIME,
    __op            VARCHAR(8),
    __source_lsn    BIGINT,
    __source_ts_ms  BIGINT
)
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS products (
    id           BIGINT       NOT NULL,
    sku          VARCHAR(64)  NOT NULL,
    title        VARCHAR(512),
    price_cents  INT,
    in_stock     BOOLEAN,
    updated_at   DATETIME,
    __op            VARCHAR(8),
    __source_lsn    BIGINT,
    __source_ts_ms  BIGINT
)
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS orders (
    id            BIGINT       NOT NULL,
    customer_id   BIGINT,
    status        VARCHAR(32),
    total_cents   INT,
    created_at    DATETIME,
    updated_at    DATETIME,
    __op            VARCHAR(8),
    __source_lsn    BIGINT,
    __source_ts_ms  BIGINT
)
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS order_items (
    id           BIGINT       NOT NULL,
    order_id     BIGINT,
    product_id   BIGINT,
    qty          INT,
    price_cents  INT,
    created_at   DATETIME,
    __op            VARCHAR(8),
    __source_lsn    BIGINT,
    __source_ts_ms  BIGINT
)
PRIMARY KEY (id)
DISTRIBUTED BY HASH(id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

-- Служебная таблица-маркер для smoke-test (3.7.4).
CREATE TABLE IF NOT EXISTS _dataflow_heartbeat (
    tract_name  VARCHAR(64),
    ts          DATETIME
)
DUPLICATE KEY (tract_name)
DISTRIBUTED BY HASH(tract_name) BUCKETS 1
PROPERTIES ("replication_num" = "1");
