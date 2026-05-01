# Запуск проекту — ДЗ6 Spark Streaming + Kafka

---

## Підготовка (один раз перед запуском)

Активувати віртуальне середовище:
```bash
cd ~/Documents/Kafka_hw_6
source myenv/bin/activate
```

---

## Крок 1 — Створення Kafka-топіків

```bash
python create_topics.py \
  --bootstrap-servers 77.81.230.104:9092 \
  --username admin \
  --password VawEzo1ikLtrA8Ug8THa \
  --identifier Khrystyna_Z
```

Очікуваний результат:
```
Topic 'Khrystyna_Z_building_sensors' created
Topic 'Khrystyna_Z_alerts' created
```

---

## Крок 2 — Запуск генераторів даних (два термінали одночасно)

**Термінал 1 — Сенсор A:**
```bash
python sensor_simulator.py \
  --bootstrap-servers 77.81.230.104:9092 \
  --username admin \
  --password VawEzo1ikLtrA8Ug8THa \
  --identifier Khrystyna_Z \
  --sensor-id sensor_A \
  --interval 5
```

**Термінал 2 — Сенсор B:**
```bash
python sensor_simulator.py \
  --bootstrap-servers 77.81.230.104:9092 \
  --username admin \
  --password VawEzo1ikLtrA8Ug8THa \
  --identifier Khrystyna_Z \
  --sensor-id sensor_B \
  --interval 5
```

---

## Крок 3 — Запуск Spark Streaming (новий термінал)

```bash
rm -rf checkpoints/

PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 pyspark-shell" \
python alert_aggregator.py \
  --bootstrap-servers 77.81.230.104:9092 \
  --username admin \
  --password VawEzo1ikLtrA8Ug8THa \
  --identifier Khrystyna_Z \
  --conditions-path alerts_conditions.csv
```

Очікуваний результат:
```
Streaming started. Output topic: Khrystyna_Z_alerts
```

Залишити термінал запущеним. Перші алерти з'являться через 1-2 хвилини.

---

## Крок 4 — Перевірка алертів (новий термінал)

```bash
python check_alerts.py \
  --bootstrap-servers 77.81.230.104:9092 \
  --username admin \
  --password VawEzo1ikLtrA8Ug8THa \
  --identifier Khrystyna_Z
```

Очікуваний результат:
```json
{
  "sensor_id": "sensor_A",
  "window_start": "2026-05-01 16:28:00",
  "window_end": "2026-05-01 16:29:00",
  "avg_temperature": 41.3,
  "avg_humidity": 22.5,
  "alert_code": 104,
  "message": "Too hot"
}
```