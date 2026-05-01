import argparse
from confluent_kafka.admin import AdminClient, NewTopic

def create_topics(bootstrap_servers, username, password, identifier):
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': 'SASL_PLAINTEXT',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': username,
        'sasl.password': password
    }
    admin = AdminClient(conf)
    
    topics = [
        NewTopic(f"{identifier}_building_sensors", num_partitions=3, replication_factor=1),
        NewTopic(f"{identifier}_alerts", num_partitions=1, replication_factor=1)
    ]
    
    fs = admin.create_topics(topics)
    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic '{topic}' created")
        except Exception as e:
            print(f"Topic '{topic}' exists or error: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="77.81.230.104:9092")
    parser.add_argument("--username", default="admin")
    parser.add_argument("--password", default="VawEzo1ikLtrA8Ug8THa")
    parser.add_argument("--identifier", default="Khrystyna_Z")
    args = parser.parse_args()
    create_topics(args.bootstrap_servers, args.username, args.password, args.identifier)