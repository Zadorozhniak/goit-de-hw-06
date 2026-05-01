import argparse
import json
from confluent_kafka import Consumer

def main(bootstrap_servers, username, password, identifier):
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': username,
        'sasl.password': password,
        'group.id': f'{identifier}_checker',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe([f"{identifier}_alerts"])
    print("Listening for alerts...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg and not msg.error():
                alert = json.loads(msg.value().decode('utf-8'))
                print(json.dumps(alert, indent=2, default=str))
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="77.81.230.104:9092")
    parser.add_argument("--username", default="admin")
    parser.add_argument("--password", default="VawEzo1ikLtrA8Ug8THa")
    parser.add_argument("--identifier", default="Khrystyna_Z")
    args = parser.parse_args()
    main(args.bootstrap_servers, args.username, args.password, args.identifier)