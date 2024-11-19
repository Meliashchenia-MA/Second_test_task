import io
import json
import os
from datetime import datetime

import confluent_kafka
import redis

from confluent_kafka import Consumer, KafkaException, KafkaError
from multiprocessing import Process, Queue
import numpy as np
from PIL import Image

CONFIG = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}


TOPIC = 'my_topic'

SAVING_DIRECTORY = 'images'

class ImagesConsumer(Process):
    def __init__(self, config, topic, queue):
        Process.__init__(self)
        self.config = config
        self.topic = topic
        self.queue = queue

    def run(self):
        print(str(self.__class__.__name__) + " started")

        consumer = confluent_kafka.Consumer(self.config)
        consumer.subscribe([self.topic])

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
                np_array = np.array(image_data)
                self.queue.put((image_name, np_array))
                print(f"Put image {image_name} into queue")

        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()


class ImagesProcessor(Process):
    def __init__(self, queue, directory):
        super().__init__()
        self.queue = queue
        self.directory = directory

    def run(self):
        print(str(self.__class__.__name__) + " started")

        redis_client = redis.StrictRedis(host='redis', port=6379, db=0)
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)

        while True:
            try:
                image_name, image_data = self.queue.get(timeout=5)
                image = Image.fromarray(image_data)
                image_path = os.path.join(self.directory, image_name)
                image.save(image_path)

                absolute_image_path = os.path.abspath(image_path)
                image_size = os.path.getsize(absolute_image_path)

                image_info = {
                    "path": absolute_image_path,
                    "size": image_size,
                    "saved_at": datetime.now().isoformat()
                }

                try:
                    if redis_client.set(image_name, json.dumps(image_info)):
                        print(f"Image {image_name} successfully saved in Redis.")
                    else:
                        print(f"Failed to save image {image_name} in Redis.")
                except Exception as e:
                    print(f"An error occurred while saving to Redis: {e}")

            except Exception as e:
                print(f"An error occurred: {e}")


if __name__ == "__main__":
    queue = Queue()

    ImageConsumer = ImagesConsumer(config=CONFIG, topic=TOPIC, queue=queue)
    ImageProcessor = ImagesProcessor(queue=queue, directory=SAVING_DIRECTORY)

    ImageConsumer.start()
    ImageProcessor.start()

    ImageConsumer.join()
    ImageProcessor.join()
