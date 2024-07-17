# import socket

from confluent_kafka import Producer

import settings


class MyKafkaProducer():  # WIP: todo=register topics, schemas, hide methods and give a simple publish event/command to be inherited and specialised
    _config = {
        'bootstrap.servers': settings.KAFKA_HOST,
        # 'client.id': socket.gethostname()
        'acks': 'all'
    }

    def __init__(self):  # , *topics):
        self.producer = Producer(self._config)
        # self.__topics = topics

    @staticmethod
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))


if __name__ == '__main__':
    TOPIC_NAME = 'test'

    from random import choice

    test_producer = MyKafkaProducer()
    
    # Produce data by selecting random values from these lists.
    user_ids = ['eabara', 'jsmith', 'sgarcia', 'jbernard', 'htanaka', 'awalther']
    products = ['book', 'alarm clock', 't-shirts', 'gift card', 'batteries']

    for _ in range(10):
        user_id = choice(user_ids)
        product = choice(products)
        test_producer.producer.produce(TOPIC_NAME, product, user_id, callback=MyKafkaProducer.delivery_callback)

    # Block until the messages are sent.
    test_producer.producer.poll(10000)
    test_producer.producer.flush()
