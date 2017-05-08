import logging
import os

import ujson
from kafka import KafkaConsumer

EVENT_KEY = os.environ.get('EVENT_KEY', '')

assert EVENT_KEY, 'Event key must be specified as environment variable'

TOKENS = list(filter(None, os.environ.get('TOKENS', '').split(',')))

assert TOKENS, 'Tokens can\'t be empty'

KAFKA_SERVER = os.environ.get('KAFKA_SERVERS', 'localhost:9092').split(',')


def main(save):
    consumer = KafkaConsumer('raw_tweets', group_id=EVENT_KEY, bootstrap_servers=KAFKA_SERVER,
                             value_deserializer=lambda m: ujson.loads(m.decode('utf-8')))

    for message in consumer:

        # first = time.time()
        # start_time = time.time()

        tweet = ujson.loads(message.value)
        # logging.info('Parse JSON: %s' % (time.time() - start_time))
        if 'id' not in tweet:
            logging.error('ERRONIOUS TWEET: %s' % tweet)
            continue
        # start_time = time.time()
        if any(token in tweet['text'] for token in TOKENS):
            # logging.info('Filtered tweet: %s' % (time.time() - start_time))

            logging.info("Tweet accepted: %s:%d:%d: key=%s tweet_id=%s" % (message.topic, message.partition,
                                                                           message.offset, message.key,
                                                                           tweet['id']))
            # start_time = time.time()
            save(tweet, EVENT_KEY, TOKENS)
            # logging.info('Tweet Saved: %s' % (time.time() - start_time))
            # Open file to mark that we are healthy (This way K8s knows we are working here)
            open('/tmp/healthy', 'a').close()
            # logging.info('Total elapsed time : %s' % (time.time() - first))


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
    main(model.save_tweet)
