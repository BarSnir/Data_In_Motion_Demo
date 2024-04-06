from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import time

def process():
    topic = 'temp_readings'
    config = {
     'bootstrap.servers': '<bootstrap-server-endpoint>',     
     'security.protocol': 'SASL_SSL',
     'sasl.mechanisms': 'PLAIN',
     'sasl.username': '<CLUSTER_API_KEY>', 
     'sasl.password': '<CLUSTER_API_SECRET>'
    }
    sr_config = {
        'url': '<schema.registry.url>',
        'basic.auth.user.info':'<SR_API_KEY>:<SR_API_SECRET>'
    }
    schema_str = ""
    schema_registry_client = SchemaRegistryClient(sr_config)
    avro_serializer = AvroSerializer(schema_str, schema_registry_client)

    producer = Producer(config)
    data = [{}]
    for value in data:
        key=''
        producer.produce(topic=topic, key=str(value.get(f'{key}')),
                         value=avro_serializer(value, 
                         SerializationContext(topic, MessageField.VALUE)),
                         on_delivery=delivery_report)

    producer.flush()

def delivery_report(err, event):
    if err is not None:
        print(f'Delivery failed on reading for {event.key().decode("utf8")}: {err}')
    else:
        print(f'Temp reading for {event.key().decode("utf8")} produced to {event.topic()}')

if __name__ == "__main__":
    process()