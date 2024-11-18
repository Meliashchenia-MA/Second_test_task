import io
from confluent_kafka import Producer
import os
from PIL import Image

conf = {
    'bootstrap.servers': 'localhost:29092'
}

producer = Producer(conf)


def delivery_report(err, msg):
    if err is not None:
        print(f'Ошибка доставки сообщения: {err}')
    else:
        print(f'Сообщение доставлено в {msg.topic()} [{msg.partition()}]')


topic = 'my_topic'


def send_image(topic, image_path):
    image_name = os.path.basename(image_path)
    image_bytes = serialize_image(image_path)
    producer.produce(topic, key=image_name, value=image_bytes)
    producer.flush()


def serialize_image(image_path):
    with Image.open(image_path) as img:
        img_byte_arr = io.BytesIO()
        format = img.format
        img.save(img_byte_arr, format=format)
        return img_byte_arr.getvalue()


IMAGE_PATH = "Your path"

send_image('my_topic', IMAGE_PATH)