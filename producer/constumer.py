# from confluent_kafka import Consumer

# conf = {
#     'bootstrap.servers': '',
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





from confluent_kafka.admin import AdminClient

conf = {
    'bootstrap.servers': '',
    'security.protocol': 'SASL_SSL',
    # 'debug': 'security,broker',
    'sasl.mechanism': 'SCRAM-SHA-512',
    'sasl.username': '',
    'sasl.password': '',
    'group.id': 'test-group-consumer',
    'enable.auto.commit': 'false',
    'auto.offset.reset': 'earliest'
}

print("Configuración del consumidor:", conf)

try:
    print("Intentando conectar con Kafka y listar tópicos...")
    admin_client = AdminClient(conf)
    metadata = admin_client.list_topics(timeout=10)
    print("Conexión a Kafka exitosa.")
    print("Tópicos disponibles en el clúster:")
    for topic in metadata.topics:
        print(f"- {topic}")
except Exception as e:
    print("Error al conectar con Kafka o al listar tópicos:")
    print(repr(e))



# from kafka import KafkaConsumer
# import json

# class Consumer:
#     def __init__(self, topic):
#         self.consumer = None
#         try:
#             self.consumer = KafkaConsumer(
#                 topic,
#                 bootstrap_servers=[
#                     
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