# from confluent_kafka import Consumer

# conf = {
#     'bootstrap.servers': 'kafka.egt-digital.com:9191,kafka.egt-digital.com:9192,kafka.egt-digital.com:9193',
#     'security.protocol': 'PLAINTEXT',
#     'sasl.mechanism': 'SCRAM-SHA-512',
#     'sasl.username': '___',
#     'sasl.password': '__',
#     'group.id': 'test-group-consumer',
#     'enable.auto.commit': 'false',
#     'auto.offset.reset': 'earliest'
# }

# print("Configuración del consumidor:", conf)

# try:
#     print("Intentando conectar con SSL...")
#     consumer = Consumer(conf)

#     print("\nSubscribiendo al topico 'player-data-ftmx'...")
#     consumer.subscribe(['player-data-ftmx'])

#     print("Empezando a consumir mensajes. Presiona Ctrl+C para detener.")
#     conexion_exitosa = False
#     try:
#         while True:
#             msg = consumer.poll(timeout=1.0)

#             if msg is None:
#                 print("Esperando por mas mensajes...")
#                 continue
#             if msg.error():
#                 print(f"Error en el consumidor: {msg.error()}")
#                 break

#             if not conexion_exitosa:
#                 print("Conexión a Kafka exitosa.")
#                 conexion_exitosa = True

#             print(f'Recibido mensaje: {msg.value().decode("utf-8")}')

#     except KeyboardInterrupt:
#         print("\nConsumidor detenido por el usuario.")

#     finally:
#         print("Cerrando consumidor.")
#         consumer.close()

# except Exception as e:
#     print("Error al conectar con Kafka:")
#     print(repr(e))



#https://b-1-public.fasttrackprodcluste.dvffuo.c5.kafka.eu-central-1.amazonaws.com:9196, https://b-3-public.fasttrackprodcluste.dvffuo.c5.kafka.eu-central-1.amazonaws.com:9196, https://b-1-public.fasttrackprodcluste.dvffuo.c5.kafka.eu-central-1.amazonaws.com:9196 

# from confluent_kafka.admin import AdminClient

# conf = {
#     'bootstrap.servers': 'kafka.egt-digital.com:9191,kafka.egt-digital.com:9192,kafka.egt-digital.com:9193',
#     'security.protocol': 'SASL_SSL',
#     # 'debug': 'security,broker',
#     'sasl.mechanism': 'SCRAM-SHA-512',
#     'sasl.username': ' ',
#     'sasl.password': ' ',
#     # 'group.id': 'test-group-consumer',
#     # 'enable.auto.commit': 'false',
#     # 'auto.offset.reset': 'earliest'
# }

# print("Configuración del consumidor:", conf)

# try:
#     print("Intentando conectar con Kafka y listar tópicos...")
#     admin_client = AdminClient(conf)
#     metadata = admin_client.list_topics(timeout=10)
#     print("Conexión a Kafka exitosa.")
#     print("Tópicos disponibles en el clúster:")
#     for topic in metadata.topics:
#         print(f"- {topic}")
# except Exception as e:
#     print("Error al conectar con Kafka o al listar tópicos:")
#     print(repr(e))


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
        print("Ahora consumiendo mensajes del tópico player-data-ftmx:")
        consume_messages('player-data-ftmx, payment-transfers-ftmx, casino-bets-ftmx, sport-bets-ftmx, player-login-records-ftmx')
        list_topics()
        print("Connection successful!")
    except Exception as e:
        print(f"Connection failed: {str(e)}")

# from kafka import KafkaConsumer
# import json

# class Consumer:
#     def __init__(self, topic):
#         self.consumer = None
#         try:
#             self.consumer = KafkaConsumer(
#                 topic,
#                 bootstrap_servers=[
#                     'kafka.egt-digital.com:9191',
#                     'kafka.egt-digital.com:9192',
#                     'kafka.egt-digital.com:9193'
#                 ],
#                 value_deserializer=lambda x: json.loads(x.decode('utf-8')),
#                 auto_offset_reset='earliest',
#                 enable_auto_commit=True,
#                 group_id='test',
#                 security_protocol='SASL_PLAINTEXT',
#                 sasl_mechanism='PLAIN',
#                 sasl_plain_username='dream-mx-readonly',
#                 sasl_plain_password='odpfeDFGPIU345782#$%^&'
#             )
#             print("Conexión a Kafka exitosa.")
#         except Exception as e:
#             print("Error al conectar con Kafka:")
#             print(repr(e))

#     def start_read(self):
#         if not self.consumer:
#             print("No se pudo crear el consumidor. Revisa el error de conexión arriba.")
#             return
#         try:
#             for message in self.consumer:
#                 print(f'Recibido: {message.value}')
#         except Exception as e:
#             print("Error al consumir mensajes:")
#             print(repr(e))

# if __name__ == '__main__':
#     topic = 'player-data-ftmx'
#     consumer = Consumer(topic)
#     consumer.start_read()