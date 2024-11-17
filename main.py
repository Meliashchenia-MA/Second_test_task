import io
import json
import os
from datetime import datetime

import redis

from confluent_kafka import Consumer, KafkaException, KafkaError
from multiprocessing import Process, Queue
import numpy as np
from PIL import Image

conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}


consumer = Consumer(conf)
TOPIC = 'my_topic'

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)


def consume_image(topic):
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            image_name = msg.key().decode('utf-8')
            image_data = Image.open(io.BytesIO(msg.value()))
            return image_name, image_data

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


def process_message(queue, image_name, image_data):
    np_array = np.array(image_data)
    queue.put((image_name, np_array))


def save_image(queue):
    directory = 'images'
    if not os.path.exists(directory):
        os.makedirs(directory)

    item = queue.get()
    image_name, image_data = item
    image = Image.fromarray(image_data)
    image_path = os.path.join(directory, image_name)
    image.save(image_path)

    absolute_image_path = os.path.abspath(image_path)

    image_size = os.path.getsize(absolute_image_path)

    image_info = {
        "path": absolute_image_path,
        "size": image_size,
        "saved_at": datetime.now().isoformat()
    }

    redis_client.set(image_name, json.dumps(image_info))

    stored_info = redis_client.get(image_name)
    if stored_info:
        print(f"Image {image_name} successfully saved in Redis.")
    else:
        print(f"Failed to save image {image_name} in Redis.")


if __name__ == "__main__":
    queue = Queue()
    image_name, image_bytes = consume_image(TOPIC)
    producer_process = Process(target=process_message, args=(queue, image_name, image_bytes,))
    consumer_process = Process(target=save_image, args=(queue,))

    producer_process.start()
    consumer_process.start()

    producer_process.join()
    consumer_process.join()





