# Capstone — realtime-пайплайн аналитики заказов

Пайплайн собирает данные о заказах интернет-магазина из PostgreSQL через CDC
в Kafka, агрегирует в ClickHouse и отдаёт через FastAPI. Prometheus и Grafana
показывают состояние сервисов и задержки.

```
PostgreSQL ──> Debezium ──> Kafka ──> ClickHouse ──> FastAPI
                                                            └─> Prometheus ──> Grafana
```

## Стек

| Сервис | Образ | Порт | Роль |
|--------|-------|------|------|
| PostgreSQL 16 | `postgres:16-alpine` | 5432 | Исходная БД, источник изменений |
| Kafka 4.2.1 | `apache/kafka` | 9092 | Шина событий |
| Debezium | `quay.io/debezium/connect:3.3.1.Final` | 8083 | CDC из PostgreSQL в Kafka |
| ClickHouse 25.8 | `clickhouse/clickhouse-server` | 18132 | Аналитическая БД |
| API | uvicorn (Python 3.12) | 18000 | REST API + метрики |
| Prometheus | `prom/prometheus:v3.5.0` | 19004 | Сбор метрик, алерты |
| Grafana | `grafana/grafana:12.1.1` | 13007 | Дашборды |

## Быстрый старт

### Требования

- Docker Desktop (или WSL2 с Docker)
- 4 ГБ RAM минимум, 8 ГБ рекомендуется
- `docker compose` v2+
- Python 3.12 + [just](https://just.systems/) (для генератора данных)

### Запуск

```bash
# Поднять все сервисы
docker compose up -d --build

# Дождаться готовности (около 1 минуты)
docker compose ps
```

Инициализация происходит автоматически:

1. PostgreSQL создаёт схему `capstone` и таблицы из `shop_tables_ddl.sql`.
2. Debezium регистрирует connector из `connector.json` и делает initial snapshot.
3. ClickHouse создаёт базу `capstone`.
4. API подключается к ClickHouse и запускает фоновый сбор метрик.

### Проверка

```bash
curl http://localhost:18000/ping
# {"status":"ok"}
```

### Загрузка данных

Генератор создаёт заказы с позициями, оплатами и возвратами. Для нового
датасета используется уникальный `--dataset` (по умолчанию `shop_<uuid>`).

```bash
# Посмотреть, что сгенерируется (без подключений)
just preview

# Загрузить в PostgreSQL (CDC сам доставит в Kafka)
export POSTGRES_DSN='postgresql://cocojumbo:putmeup@localhost:5432/capstone'
just run postgres --orders 100 --rate 100

# Или напрямую в Kafka (для нагрузки, без CDC)
just run kafka --orders 1000 --rate 500
```

Генератор покажет итоговые суммы — используйте их для сверки.

### Где результат

| Что | Где |
|-----|-----|
| API | http://localhost:18000 |
| Prometheus | http://localhost:19004 |
| Grafana | http://localhost:13007 |
| Метрики (raw) | http://localhost:18000/metrics |

## Модель данных

Три таблицы в PostgreSQL:

```sql
capstone.orders       — заказ: id, campaign_id, status, amount_cents, version
capstone.order_items  — позиция заказа: order_id, sku, quantity, unit_price_cents
capstone.payments     — платёж: order_id, kind (capture/refund), amount_cents
```

Жизненный цикл заказа:

1. **created** — заказ создан, позиции добавлены.
2. **paid** — оплата прошла, запись платежа `capture`.
3. **cancelled** — заказ отменён, запись платежа `refund`.

Также возможны: удаление неоплаченного заказа (`is_deleted` / tombstone в Kafka).

Каждая строка имеет `dataset_id` для изоляции экспериментов и `version` для
обработки порядковых изменений (1 → 2 → 3).

## Как работает пайплайн

### Шаг 1. PostgreSQL → Debezium → Kafka

Debezium подключается к PostgreSQL через logical replication (plugin `pgoutput`).
При каждом INSERT/UPDATE/DELETE он формирует событие в формате Debezium envelope
и кладёт в Kafka-топик `capstone.<table>`.

Формат сообщения Kafka:

```json
{
  "event_id": "shop_xxx_3",
  "op": "u",          // c=create, u=update, d=delete
  "data": {
    "dataset_id": "shop_xxx",
    "id": "o000000001",
    "status": "paid",
    "version": 2,
    ...
  }
}
```

### Шаг 2. Обработчик → ClickHouse

Обработчик читает топики Kafka, группирует события по ключу `(dataset_id, id)`
и поддерживает актуальное состояние каждой строки. При поступлении события с
более высоким `version` старая версия перезаписывается.

В ClickHouse создаются таблицы:

- `orders_raw` / `order_items_raw` / `payments_raw` — сырые события.
- `orders_current_t` / `order_items_current_t` / `payments_current_t` —
  актуальное состояние (последняя версия по `version`).

### Шаг 3. API

FastAPI отдаёт агрегированные данные из ClickHouse:

```bash
# Сумма оплаченных заказов
curl "http://localhost:18000/v1/datasets/shop_xxx/metrics/summary"

# Задержка обработки и количество событий
curl "http://localhost:18000/v1/datasets/shop_xxx/telemetry/orders"

# Список заказов с фильтрами
curl "http://localhost:18000/v1/datasets/shop_xxx/orders?status=paid&limit=10"

# Один заказ
curl "http://localhost:18000/v1/datasets/shop_xxx/orders/o000000001"
```

Все ответы содержат `meta.computed_at` — время расчёта ответа.

### Шаг 4. Кэш

API кэширует ответы в памяти (TTL 30 секунд, до 128 записей). Каждый эндпоинт
имеет свой namespace ключей, чтобы не перемешивать данные. Для обхода кэша
добавьте `?refresh=true`.

## Мониторинг

### Prometheus

API экспортирует метрики на `/metrics`. Prometheus собирает их каждые 2 секунды.

Доступные метрики:

| Метрика | Тип | Описание |
|---------|-----|----------|
| `capstone_http_requests_total` | Counter | Запросы по route и status |
| `capstone_http_duration_seconds` | Histogram | Задержка ответов |
| `capstone_cache_requests_total` | Counter | Пропуски/попадания кэша |
| `capstone_clickhouse_parts_total` | Gauge | Активные parts таблиц |

Алерты в `alerts.yml`:

- **ApiUnavailable** — API недоступен более 10 секунд.
- **ApiErrors** — более 50% запросов к `/metrics/summary` возвращают 5xx.

### Grafana

Дашборд «Operations and Recovery» (автоматически загружается при старте):

1. **API requests** — скорость запросов.
2. **API 5xx rate** — доля ошибок.
3. **API p95 latency** — задержка p95.
4. **Parts таблицы orders_current_t** — количество активных parts ClickHouse.

### Как проверить свежесть данных

```bash
# Запросить свежий расчёт
curl "http://localhost:18000/v1/datasets/shop_xxx/metrics/summary?refresh=true"
```

В ответе:

```json
{
  "data": { "paid_amount_cents": 551666 },
  "meta": {
    "computed_at": "2026-11-10T16:35:28.341Z",
    "latest_event_at": "2026-11-09T21:55:36.803Z"
  }
}
```

- `computed_at` — когда ответ был рассчитан.
- `latest_event_at` — самое старое событие, которое обработчик увидел в Kafka.
  Если `latest_event_at` отстаёт от текущего времени, пайплайн не успевает.

## Как проверить корректность

### Мини-сценарий

```bash
# 1. Поднять стенд
docker compose up -d --build

# 2. Загрузить 10 заказов
export POSTGRES_DSN='postgresql://cocojumbo:putmeup@localhost:5432/capstone'
just run postgres --orders 10 --rate 100

# 3. Подождать 10 секунд (CDC + обработчик)
sleep 10

# 4. Проверить сумму
curl "http://localhost:18000/v1/datasets/shop_xxx/metrics/summary?refresh=true"
```

Генератор выведет ожидаемые суммы — сравните с ответом API.

### Проверка обновления

```bash
# 5. Изменить заказ в PostgreSQL
docker compose exec postgres psql -U cocojumbo -d capstone \
  -c "UPDATE capstone.orders SET status='paid' WHERE dataset_id='shop_xxx' AND id='o000000000'"

# 6. Подождать 10 секунд и проверить новый результат
sleep 10
curl "http://localhost:18000/v1/datasets/shop_xxx/metrics/summary?refresh=true"
```

Сумма `paid_amount_cents` должна увеличиться на `amount_cents` изменённого заказа.

## Файлы проекта

| Файл | Что делает |
|------|-----------|
| `compose.yaml` | Все сервисы, healthcheck, зависимости |
| `api.py` | FastAPI: эндпоинты, метрики, кэш, lifespan |
| `cache.py` | TTLCache в памяти (OrderedDict) |
| `Dockerfile` | Сборка API-контейнера через uv |
| `generate.py` | Генератор данных (PostgreSQL и Kafka) |
| `create_stuff.py` | Создание схемы PostgreSQL / топиков Kafka |
| `shop_tables_ddl.sql` | DDL для PostgreSQL |
| `connector.json` | Конфиг Debezium connector |
| `prometheus.yml` | Конфиг Prometheus (scrape + rules) |
| `alerts.yml` | Алерты |
| `dashboard.json` | Дашборд Grafana |
| `datasources.yml` | Провайдер datasource для Grafana |
| `dashboards.yml` | Провайдер дашбордов для Grafana |
| `justfile` | Рецепты just для генератора |

## Ограничения

- **Один брокер Kafka** — репликации нет, при остановке брокера данные теряются.
- **Обработчик не в compose** — обработчик Kafka → ClickHouse запускается
  отдельно; его код и конфигурация находятся вне этого репозитория.
- **Кэш только in-process** — при перезапуске API кэш сбрасывается.
- **Только один dataset за раз** — для нового эксперимента нужен новый `--dataset`.
- **Нет Redis** — кэш в памяти, не масштабируется на несколько инстансов API.
- **Автоматическая инициализация** — compose поднимает сервисы, но Debezium
  connector нужно зарегистрировать вручную (curl к `localhost:8083/connectors`
  с телом из `connector.json`).

## Полезные команды

```bash
# Перезапустить API после изменений
docker compose up -d --build api

# Посмотреть логи
docker compose logs -f api
docker compose logs -f connect

# Проверить состояние сервисов
docker compose ps

# Сбросить всё и начать с нуля
docker compose down -v
docker compose up -d --build

# Создать новые топики Kafka
just topics localhost:19092 capstone 2

# Прямой запрос к ClickHouse
docker compose exec clickhouse clickhouse-client --query "SELECT count() FROM capstone.orders_current_t"
```


## Эксперименты 
```
PostgreSQL ──> ClickHouse ──> FastAPI
                                    └─> Prometheus ──> Grafana
```