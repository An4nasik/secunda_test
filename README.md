# Payment Service

Асинхронный сервис процессинга платежей. API принимает запрос на оплату и сразу отвечает
`202 Accepted`; дальше событие через transactional outbox доставляется в RabbitMQ,
consumer обрабатывает платёж через эмуляцию платёжного шлюза, фиксирует результат в БД
и уведомляет клиента webhook'ом. Ошибки обработки уходят в отложенные повторы
с экспоненциальной задержкой, безнадёжные сообщения — в Dead Letter Queue.

**Стек:** FastAPI + Pydantic v2 · SQLAlchemy 2.0 (async) · PostgreSQL 18 · RabbitMQ 4
(FastStream) · Alembic · structlog · Docker Compose · uv · ruff · pytest.

## Архитектура

```
POST /api/v1/payments
        │  одна транзакция: INSERT payments + INSERT outbox
        ▼
   ┌─────────┐   poll (FOR UPDATE SKIP LOCKED)    ┌──────────────┐
   │ Postgres │◄──────────────────────────────────│ outbox-relay │
   └─────────┘                                    └──────┬───────┘
        ▲                                                │ publish (persistent)
        │                                                ▼
        │                              exchange «payments» ──► queue «payments.new»
        │                                                            │
        │      ┌─────────────────────────── consumer ◄───────────────┘
        │      │ 1. эмуляция шлюза (2–5 с, 90 % успех)
        └──────┤ 2. UPDATE status, processed_at (commit)
               │ 3. POST webhook → webhook_delivered_at
               │
               │ ошибка обработки (attempt N)
               ├── N ≤ MAX_RETRIES ──► «payments.new.retry.N» (TTL 2ⁿ⁻¹·2 с)
               │                          └── по истечении TTL — обратно в «payments.new»
               └── N > MAX_RETRIES ──► «payments.new.dlq»
```

### Гарантии доставки

**Transactional outbox.** Платёж и событие для брокера записываются в одной транзакции
БД — событие не может потеряться между «записали платёж» и «опубликовали сообщение».
Отдельный сервис `outbox-relay` забирает неопубликованные строки
(`SELECT … FOR UPDATE SKIP LOCKED` — реплики relay не конфликтуют), публикует
persistent-сообщения и проставляет `published_at`. Семантика — at-least-once:
при падении между publish и commit событие уйдёт повторно, дубликаты гасятся
идемпотентностью consumer'а.

**Идемпотентность API.** `Idempotency-Key` уникален в БД; вместе с ключом хранится
SHA-256 канонизированного тела запроса:

- повтор с тем же ключом и телом → `202` с исходным платежом и заголовком
  `Idempotency-Replayed: true`;
- тот же ключ с другим телом → `409 Conflict`;
- гонка двух одновременных запросов с одним ключом разрешается через unique
  constraint: проигравший получает результат победителя.

**Идемпотентность consumer'а.** Событие несёт только `payment_id` — состояние всегда
читается из БД под row lock (`SELECT … FOR UPDATE`). При повторной доставке:

- платёж в финальном статусе повторно через шлюз не проводится;
- webhook с отметкой `webhook_delivered_at` повторно не отправляется.

Статус платежа коммитится в БД **до** отправки webhook'а: результат оплаты не
теряется, даже если уведомление упало и сообщение ушло на повтор.

**Повторы и DLQ.** Ошибка обработки (БД недоступна, webhook не отвечает) — событие
публикуется в парковочную очередь `payments.new.retry.N` без консьюмеров с
`x-message-ttl = RETRY_BASE_DELAY_SECONDS · 2^(N-1)` (по умолчанию 2 с, 4 с, 8 с);
по истечении TTL брокер сам возвращает сообщение в рабочую очередь через
dead-letter-маршрутизацию. После `MAX_RETRIES` неудач событие уходит в
`payments.new.dlq` с заголовками `x-error`, `x-error-type`, `x-attempt` для разбора.
Плагины RabbitMQ не требуются. Подстраховка: рабочая очередь объявлена с DLX на DLQ,
поэтому даже сообщение, отвергнутое из-за сбоя в самой логике маршрутизации,
не теряется. Отклонение платежа шлюзом (10 %) — не ошибка, а бизнес-результат:
платёж получает статус `failed`, и webhook о нём доставляется как обычно.

**Webhook'и** доставляются at-least-once (стандарт индустрии — приёмник должен быть
идемпотентен). Доставленным считается только ответ 2xx.

## Быстрый старт

Требуются Docker и Docker Compose v2.

```bash
docker compose up -d --build
```

Поднимутся 6 сервисов: `postgres`, `rabbitmq`, `migrations` (one-shot, применяет
Alembic-миграции), `api` (порт 8000), `consumer`, `outbox-relay`.

- Swagger UI: <http://localhost:8000/docs>
- RabbitMQ Management: <http://localhost:15672> (guest/guest)

API-ключ по умолчанию — `dev-secret-key` (меняется через `.env`, см. `.env.example`).

### Примеры запросов

Создание платежа:

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-secret-key" \
  -H "Idempotency-Key: order-42-attempt-1" \
  -d '{
    "amount": "149.99",
    "currency": "RUB",
    "description": "Заказ №42",
    "metadata": {"order_id": 42},
    "webhook_url": "https://example.com/webhooks/payments"
  }'
```

```json
{"payment_id": "019f2773-0c56-75fe-9a4a-95bc1766402e", "status": "pending", "created_at": "2026-07-03T10:07:59.022425Z"}
```

Повтор того же запроса вернёт `202` с тем же `payment_id` и заголовком
`Idempotency-Replayed: true`; тот же ключ с другим телом — `409`.

Статус платежа:

```bash
curl http://localhost:8000/api/v1/payments/019f2773-0c56-75fe-9a4a-95bc1766402e \
  -H "X-API-Key: dev-secret-key"
```

```json
{
  "payment_id": "019f2773-0c56-75fe-9a4a-95bc1766402e",
  "amount": "149.99",
  "currency": "RUB",
  "description": "Заказ №42",
  "metadata": {"order_id": 42},
  "status": "succeeded",
  "webhook_url": "https://example.com/webhooks/payments",
  "created_at": "2026-07-03T10:07:59.022425Z",
  "processed_at": "2026-07-03T10:08:02.283910Z",
  "webhook_delivered_at": "2026-07-03T10:08:02.328997Z"
}
```

Webhook, который получит клиент (суммы передаются строками — без потери точности):

```json
{
  "payment_id": "019f2773-0c56-75fe-9a4a-95bc1766402e",
  "status": "succeeded",
  "amount": "149.99",
  "currency": "RUB",
  "description": "Заказ №42",
  "metadata": {"order_id": 42},
  "processed_at": "2026-07-03T10:08:02.283910Z"
}
```

## Конфигурация

Все настройки — через переменные окружения (локально — `.env`, см. `.env.example`).

| Переменная | По умолчанию | Описание |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://payments:payments@localhost:5432/payments` | Строка подключения к PostgreSQL |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | Строка подключения к RabbitMQ |
| `API_KEY` | — (обязательна) | Статический ключ для заголовка `X-API-Key` |
| `GATEWAY_DELAY_MIN_SECONDS` | `2.0` | Мин. длительность «обработки» в шлюзе |
| `GATEWAY_DELAY_MAX_SECONDS` | `5.0` | Макс. длительность «обработки» в шлюзе |
| `GATEWAY_SUCCESS_RATE` | `0.9` | Вероятность успеха платежа |
| `WEBHOOK_TIMEOUT_SECONDS` | `10.0` | Таймаут HTTP-запроса webhook'а |
| `MAX_RETRIES` | `3` | Число повторов после первой неудачной обработки |
| `RETRY_BASE_DELAY_SECONDS` | `2.0` | База экспоненциальной задержки: `base · 2^(N-1)` |
| `OUTBOX_POLL_INTERVAL_SECONDS` | `1.0` | Пауза relay при пустом outbox |
| `OUTBOX_BATCH_SIZE` | `100` | Размер пачки публикации relay |
| `LOG_LEVEL` | `INFO` | Уровень логирования |
| `LOG_JSON` | `true` | JSON-логи (`false` — читаемый консольный вывод) |

## Разработка и тесты

```bash
uv sync                                  # окружение (Python 3.14)
uv run ruff format . && uv run ruff check .
uv run pytest                            # юнит-тесты, инфраструктура не нужна
```

E2E-тесты гоняются против поднятого compose-окружения: поднимают в процессе pytest
приёмник webhook'ов (контейнеры достучатся до него через `host.docker.internal`),
проходят полный цикл платежа, проверяют идемпотентность и попадание события в DLQ
при недоставляемом webhook'е (читают очередь напрямую через aio-pika):

```bash
docker compose up -d --build
uv run pytest -m e2e
```

Схема БД меняется только через Alembic: `uv run alembic revision -m "..."`,
применение — `uv run alembic upgrade head` (в compose это делает сервис `migrations`).

## Структура проекта

```
src/app/
├── config.py            # настройки (pydantic-settings)
├── logs.py              # structlog + мост для stdlib-логгеров
├── db.py                # async engine / session factory
├── models.py            # ORM: payments, outbox
├── schemas.py           # контракты API, событие, payload webhook'а
├── broker.py            # топология RabbitMQ: exchange'и, retry-очереди, DLQ
├── api/                 # HTTP-сервис (FastAPI): auth, роуты, error handlers
├── services/            # use case'ы: создание платежа + outbox, идемпотентность
├── consumer/            # обработчик payments.new: шлюз, webhook, retry-роутинг
└── outbox/              # relay: публикация outbox → RabbitMQ
migrations/              # Alembic (async)
tests/unit/              # быстрые тесты без инфраструктуры
tests/e2e/               # сценарии против docker compose
```

Каждый сервис — отдельный процесс с одним и тем же образом:
`python -m app.api` / `python -m app.consumer` / `python -m app.outbox`.

## Что стоило бы добавить для продакшена

Сознательно оставлено за рамками тестового, но заложено в архитектуру:

- **Подпись webhook'ов** (HMAC-SHA256 тела в заголовке) — приёмник сможет проверять
  подлинность уведомлений.
- **Quorum queues** вместо classic — репликация очередей в кластере RabbitMQ.
- **Чистка outbox** — опубликованные строки сейчас остаются в таблице как аудит;
  нужен фоновый job с ретенцией.
- **Re-drive из DLQ** — инструмент повторной отправки разобранных сообщений
  (сейчас это ручная операция через Management UI).
- **Метрики** (Prometheus: глубина outbox, лаг обработки, доля неуспешных webhook'ов)
  и алерты на рост DLQ.
- **Трассировка** — `correlation_id` уже прокидывается в сообщения, осталось
  подключить OpenTelemetry.
