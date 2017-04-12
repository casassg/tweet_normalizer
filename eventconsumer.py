import json
import os

import logging
from kafka import KafkaConsumer

event = os.environ.get('EVENT_KEY', '')

assert event, 'Event key must be specified as environment variable'

tokens = os.environ.get('TOKENS', '').split(',')
bootstrap_servers = os.environ.get('KAFKA_SERVERS', 'localhost:9092').split(',')


def main():
    consumer = KafkaConsumer('raw_tweets', group_id=event, value_deserializer=lambda m: json.loads(m.decode('utf-8')))

    for message in consumer:
        tweet = json.loads(message.value)
        if 'id' not in tweet:
            logging.error('ERRONIOUS TWEET: %s' % tweet)
            continue
        if any(token in tweet['text'] for token in tokens):
            logging.info("Tweet received: %s:%d:%d: key=%s tweet_id=%s" % (message.topic, message.partition,
                                                                           message.offset, message.key,
                                                                           tweet['id']))


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    logging.info('Event: %s' % event)
    logging.info('Tracking keywords: %s' % ','.join(tokens))
    logging.info('Kafka servers: %s' % ','.join(bootstrap_servers))
    logging.info('Start stream track')
    main()
