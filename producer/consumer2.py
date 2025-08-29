from confluent_kafka import Consumer, TopicPartition

consumer = Consumer({
    'bootstrap.servers': 'kafka.egt-digital.com:9192',
    # No se configura 'group.id'
    'auto.offset.reset': 'earliest'
})

# Asignar particiones manualmente sin usar group.id
partitions = [TopicPartition('sport-bets-ftmx', 0)]
consumer.assign(partitions)

# Consumir mensajes
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Error: {}".format(msg.error()))
        continue
    print(f"Mensaje recibido: {msg.value().decode('utf-8')}")
