import argparse
import json
import random
import time
from datetime import datetime
from confluent_kafka import Producer

def delivery_report(err, msg):
    if err:
        print(f'Error: {err}')
    else:
        print(f'Sent to {msg.topic()} partition {msg.partition()}')

def main(bootstrap_servers, username, password, topic, sensor_id, interval):
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': username,
        'sasl.password': password
    }
    producer = Producer(conf)
    
    print(f"Sensor {sensor_id} sending every {interval}s to {topic}")
    try:
        while True:
            data = {
                "sensor_id": str(sensor_id),
                "timestamp": datetime.now().isoformat(),
                "temperature": round(random.uniform(25.0, 45.0), 2),
                "humidity": round(random.uniform(15.0, 85.0), 2)
            }
            producer.produce(topic, json.dumps(data).encode('utf-8'), callback=delivery_report)
            producer.poll(0)
            print(f"[{datetime.now().isoformat()}] {data}")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("Stopped.")
    finally:
        producer.flush()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="77.81.230.104:9092")
    parser.add_argument("--username", default="admin")
    parser.add_argument("--password", default="VawEzo1ikLtrA8Ug8THa")
    parser.add_argument("--identifier", default="Khrystyna_Z")
    parser.add_argument("--sensor-id", type=str, required=True)
    parser.add_argument("--interval", type=int, default=5)
    args = parser.parse_args()
    
    topic = f"{args.identifier}_building_sensors"
    main(args.bootstrap_servers, args.username, args.password, topic, args.sensor_id, args.interval)