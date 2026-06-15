# IoT Flink Pipeline

## Requirements
- Docker
- Docker Compose

## Запуск

```bash
docker compose up --build
```

## Что запускается

| Сервис | Адрес |
|---|---|
| Flink UI | http://localhost:8081 |
| Kafka | localhost:9092 |
| PostgreSQL | localhost:5432 |

## Проверка результатов

```bash
docker exec -it $(docker ps -qf "name=kafka") \
  kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic iot-results \
  --from-beginning
```

Результаты появляются раз в минуту

## Остановка

```bash
docker compose down -v
```
