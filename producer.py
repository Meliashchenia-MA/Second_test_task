from kafka import KafkaProducer
import json

# Создаем продюсера Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Адрес вашего Kafka сервера
    key_serializer=lambda k: k.encode('utf-8') if k else None,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Функция для отправки сообщения
def send_message(topic, key, value):
    producer.send(topic, key=key, value=value)
    producer.flush()
    print(f"Отправлено сообщение: ключ={key}, значение={value}")

# Пример использования
send_message('your_topic', 'example_key', {'example': 'value'})