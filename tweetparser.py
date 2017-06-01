import logging
import os
import sys
import ujson

from confluent_kafka import Consumer, KafkaError, KafkaException

EVENT_KEY = os.environ.get('EVENT_KEY', '')

assert EVENT_KEY, 'Event key must be specified as environment variable'

TOKENS = list(filter(None, os.environ.get('TOKENS', '').split(',')))

assert TOKENS, 'Tokens can\'t be empty'

KAFKA_SERVER = os.environ.get('KAFKA_SERVERS', 'localhost:9092')


def main(save):
    conf = {'bootstrap.servers': KAFKA_SERVER,
            'group.id': EVENT_KEY,
            'session.timeout.ms': 6000,
            'default.topic.config': {'auto.offset.reset': 'smallest'}
            }
    c = Consumer(**conf)

    def print_assignment(consumer, partitions):
        logging.info('Assignment: %s' % partitions)

    # Subscribe to topics
    c.subscribe(['raw_tweets', ], on_assign=print_assignment)

    msg_count = 0
    while True:
        msg = c.poll()
        if msg is None:
            continue
        if msg.error():
            # Error or event
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                logging.info('%% %s [%d] reached end at offset %d' % (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                # Error
                raise KafkaException(msg.error())
        else:
            tweet = None
            try:
                tweet = ujson.loads(msg.value())
            except TypeError:
                logging.error("Message not json: %s" % msg.value())
                continue
            except ValueError:
                logging.error("Message not json: %s" % msg.value())
                continue
            if 'text' not in tweet:
                logging.info('Internal message: %s' % tweet)
                continue
            msg_count += 1
            # Proper message

            if any(token in tweet['text'] for token in TOKENS):
                logging.info('Tweet accepted: %s:%d:%d: key=%s tweet_id=%s' %
                             (msg.topic(), msg.partition(), msg.offset(),
                              str(msg.key()), tweet['id']))
                save(tweet, EVENT_KEY, TOKENS)

        if msg_count == 1:
            open('/tmp/healthy', 'a').close()


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s%(levelname)s:%(message)s',
        level=logging.INFO
    )
    import model

    logging.info('Event: %s' % EVENT_KEY)
    logging.info('Tracking keywords: %s' % ','.join(TOKENS))
    logging.info('Kafka servers: %s' % KAFKA_SERVER)
    logging.info('Connecting to Cassandra...')
    logging.info('Start stream track')
    if not TOKENS:
        logging.error('Tokens can\'t be empty')
    main(model.save_tweet)
