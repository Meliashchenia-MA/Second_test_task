from kafka import KafkaConsumer
import json

# Создаем потребителя Kafka
consumer = KafkaConsumer(
    'your_topic',  # Замените на название вашего топика
    bootstrap_servers=['localhost:9092'],  # Адрес вашего Kafka сервера
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='your_group_id',  # Замените на ваш ID группы
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Обрабатываем сообщения
for message in consumer:
    key = message.key.decode('utf-8') if message.key else None
    value = message.value
    print(f"Получено сообщение: ключ={key}, значение={value}")