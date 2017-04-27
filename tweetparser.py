import json
import logging
import os

from kafka import KafkaConsumer

EVENT_KEY = os.environ.get('EVENT_KEY', '')

assert EVENT_KEY, 'Event key must be specified as environment variable'

TOKENS = list(filter(None, os.environ.get('TOKENS', '').split(',')))

assert TOKENS, 'Tokens can\'t be empty'

KAFKA_SERVER = os.environ.get('KAFKA_SERVERS', 'localhost:9092').split(',')


def main(save, event_dict):
    consumer = KafkaConsumer('raw_tweets', group_id=EVENT_KEY, bootstrap_servers=KAFKA_SERVER,
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))

    for message in consumer:
        tweet = json.loads(message.value)
        if 'id' not in tweet:
            logging.error('ERRONIOUS TWEET: %s' % tweet)
            continue
        if any(token in tweet['text'] for token in TOKENS):
            logging.info("Tweet accepted: %s:%d:%d: key=%s tweet_id=%s" % (message.topic, message.partition,
                                                                           message.offset, message.key,
                                                                           tweet['id']))
            save(tweet, message.value, event_dict)
            # Open file to mark that we are healthy (This way K8s knows we are working here)
            open('/tmp/healthy', 'a').close()


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    import model

    logging.info('Event: %s' % EVENT_KEY)
    logging.info('Tracking keywords: %s' % ','.join(TOKENS))
    logging.info('Kafka servers: %s' % ','.join(KAFKA_SERVER))
    logging.info('Connecting to Cassandra...')
    logging.info('Start stream track')
    if not TOKENS:
        logging.error('Tokens can\'t be empty')
    event_dict = model.create_dict(EVENT_KEY, TOKENS)
    main(model.save_tweet, event_dict)
