
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient

# Basic configuration for all clients
conf = {
    # 'bootstrap.servers': 'b-1.fasttrackprodcluste.dvffuo.c5.kafka.eu-central-1.amazonaws.com:9196,b-2.fasttrackprodcluste.dvffuo.c5.kafka.eu-central-1.amazonaws.com:9196,b-3.fasttrackprodcluste.dvffuo.c5.kafka.eu-central-1.amazonaws.com:9196,kafka.egt-digital.com:9192,kafka.egt-digital.com:9193',
    'bootstrap.servers': 'kafka.egt-digital.com:9192',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-512',
    'sasl.username': 'dream-mx-readonly',  
    'sasl.password': 'odpfeDFGPIU345782#$%^&', 
    # Optional debugging (uncomment if needed)
    # 'debug': 'security,broker',
    # For consumers only:
    'auto.offset.reset': 'earliest',
    'group.id': 'test-group',
    # 'api.version.request': False  # Add this line to disable ApiVersion requests
    # 'auto.offset.reset': 'earliest'
}

def list_topics():
    admin = AdminClient(conf)
    metadata = admin.list_topics(timeout=3000)
    print("Available topics and details:")
    for topic_name, topic in metadata.topics.items():
        print(f"\nTopic: {topic_name}")
        if topic.error is not None:
            print(f"  [Error]: {topic.error}")
            continue
        for partition_id, partition in topic.partitions.items():
            print(f"  Partition: {partition_id}")
            print(f"    Leader: {partition.leader}")
            print(f"    Replicas: {partition.replicas}")
            print(f"    ISR: {partition.isrs}")

def produce_message(topic, message):
    producer = Producer(conf)
    producer.produce(topic, value=message)
    producer.flush()
    print(f"Message sent to {topic}")

def describe_partitions_and_offsets(topic):
    admin = AdminClient(conf)
    try:
        # Obtener metadatos del tópico
        metadata = admin.list_topics(timeout=10)
        if topic not in metadata.topics:
            print(f"El tópico '{topic}' no existe en el clúster.")
            return

        print(f"Detalles del tópico '{topic}':")
        topic_metadata = metadata.topics[topic]
        for partition_id, partition in topic_metadata.partitions.items():
            print(f"\nPartición: {partition_id}")
            print(f"  Líder: {partition.leader}")
            print(f"  Réplicas: {partition.replicas}")
            print(f"  ISR: {partition.isrs}")

            # Obtener los offsets más recientes
            low, high = admin.get_watermark_offsets(
                topic, partition_id, timeout=10
            )
            print(f"  Offset más bajo: {low}")
            print(f"  Offset más alto: {high}")

    except Exception as e:
        print(f"Error al describir particiones y offsets: {str(e)}")

def consume_messages(topic):
    consumer_conf = conf.copy()
    consumer_conf.update({
        'group.id': 'test-group',
        'auto.offset.reset': 'earliest'
    })
    
    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            print(f"Received message: {msg.value().decode('utf-8')}")
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    print("Testing Kafka connection...")
    try:
        list_topics()
        print("Ahora consumiendo mensajes del tópico sport-bets-ftmx:")
        consume_messages('sport-bets-ftmx')
        # Cambiarlo a solo llamar de uno en uno si agregamos todos van a tronar
        list_topics()
        print("Connection successful!")
    except Exception as e:
        print(f"Connection failed: {str(e)}")
