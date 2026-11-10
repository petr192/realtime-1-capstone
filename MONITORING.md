# Мониторинг ClickHouse: количество parts таблиц

## Зачем

ClickHouse хранит данные в «частях» (parts). Чем их больше — тем медленнее
запросы, потому что движку приходится объединять множество мелких файлов.
Для таблицы `orders_current_t` большое число активных parts — ранний сигнал,
что пора запускать `OPTIMIZE TABLE … FINAL` или пересматривать стратегию
вставки.

## Что измеряем

Метрика `capstone_clickhouse_parts_total` (Gauge) показывает количество
**активных** parts для каждой таблицы в базе `capstone`.

```
capstone_clickhouse_parts_total{table="orders_current_t"} 2.0
capstone_clickhouse_parts_total{table="orders_raw"}        2.0
capstone_clickhouse_parts_total{table="payments_raw"}      1.0
```

Активные parts — это те, что участвуют в чтении. Мёртвые (after merge/
optimize) не учитываются.

## Как устроено

### 1. Фоновая задача

В `api.py` определена функция `_refresh_clickhouse_parts`:

```python
async def _refresh_clickhouse_parts(ch_client):
    while True:
        try:
            res = await ch_client.query(
                "SELECT table, count() FROM system.parts "
                "WHERE active = 1 AND database = {db:String} "
                "GROUP BY table",
                parameters={"db": CLICKHOUSE_DATABASE},
                settings={"max_execution_time": 5},
            )
            for table, count in res.result_rows:
                CLICKHOUSE_PARTS.labels(table=table).set(int(count))
        except Exception:
            logger.exception("clickhouse_parts refresh failed")
        await asyncio.sleep(10)
```

- Опрашивает `system.parts` каждые 10 секунд.
- Фильтрует только активные parts (`active = 1`).
- Группирует по таблице и обновляет Gauge.
- Ошибки логируются, но не убивают задачу.

### 2. Жизненный цикл

Задача запускается в `lifespan` через `asyncio.create_task`:

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    ch_client = await clickhouse_connect.get_async_client(...)
    app.state.ch_client = ch_client
    asyncio.create_task(_refresh_clickhouse_parts(ch_client))
    try:
        yield
    finally:
        await ch_client.close()
        await app.state.http_ch_client.aclose()
```

Важно: клиент ClickHouse создаётся напрямую, а не через `async with`,
и закрывается в `finally`. Иначе задача `_refresh_clickhouse_parts`
умирала бы при выходе из контекста.

### 3. Prometheus и Grafana

Метрика доступна на `/metrics` и автоматически собираются Prometheus.

В Grafana добавлена панель «Parts таблицы orders_current_t в ClickHouse»
с запросом:

```promql
capstone_clickhouse_parts_total{table="orders_current_t"}
```

## Как проверить

1. Запустите стенд:
   ```bash
   docker compose up -d --build api
   ```

2. Подождите 10–15 секунд (первый цикл опроса).

3. Проверьте метрику в Prometheus:
   ```
   http://localhost:19004
   ```
   Запрос:
   ```
   capstone_clickhouse_parts_total{table="orders_current_t"}
   ```

4. Или через API:
   ```bash
   curl http://localhost:18000/metrics | Select-String "clickhouse_parts"
   ```

5. Панель доступна в Grafana:
   ```
   http://localhost:13007
   ```

## Подсказки

### Как посмотреть все таблицы

```promql
capstone_clickhouse_parts_total
```

Покажет parts для всех таблиц в базе `capstone`.

### Что считается нормальным

- Для таблицы с регулярными вставками (раз в минуту) — несколько десятков
  parts нормально.
- Если число частей растёт до сотен — пора разбираться с мерджами.
- Для таблиц с редкими вставками (единицы в день) — единицы частей.

### Как ускорить проверку

Если не хотите ждать 10 секунд, добавьте в запрос к API параметр `refresh`:
```
http://localhost:18000/v1/datasets/{dataset_id}/metrics/summary?refresh=true
```

Но это перезагрузит только бизнес-метрику. Для parts нужно ждать
фоновый цикл.

### Если метрика пустая

- Убедитесь, что API запущен и отвечает: `curl http://localhost:18000/ping`
- Проверьте логи API: `docker compose logs api`
- Убедитесь, что база `capstone` существует и в ней есть таблицы
- Проверьте, что пользователь имеет доступ к `system.parts`

### Если части не уменьшаются

ClickHouse сам мерджит части в фоне, но это может занимать время.
Можно принудительно запустить:
```sql
OPTIMIZE TABLE capstone.orders_current_t FINAL;
```

После этого число активных parts должно снизиться.

### Кэш и части

Фоновая задача опрашивает ClickHouse напрямую, минуя кэш. Это значит,
что метрика parts всегда свежая (с задержкой 10 секунд), даже если
бизнес-метрика отдаётся из кэша.
