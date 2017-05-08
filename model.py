import logging
import os
import socket
import time
import uuid
import ujson

from cassandra.cluster import Cluster
from cassandra.cqlengine import columns, connection
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model

CASSANDRA_IPS = list(
    map(socket.gethostbyname, os.environ.get('CASSANDRA_NODES', '127.0.0.1').replace(' ', '').split(',')))
KEYSPACE = 'twitter_analytics'


class Tweet(Model):
    id = columns.UUID(primary_key=True, default=uuid.uuid4)
    event_name = columns.Text(index=True)
    t_id = columns.Text(primary_key=True, clustering_order="DESC")
    event_kw = columns.Text()
    t_created_at = columns.DateTime()
    t_text = columns.Text()
    t_retweet_count = columns.Integer()
    t_favorite_count = columns.Integer()
    t_geo = columns.Text()
    t_coordinates = columns.Text()
    t_favorited = columns.Boolean()
    t_retweeted = columns.Boolean()
    t_is_a_retweet = columns.Boolean()
    t_lang = columns.Text()
    u_id = columns.Text()
    u_name = columns.Text()
    u_screen_name = columns.Text()
    u_location = columns.Text()
    u_url = columns.Text()
    u_lang = columns.Text()
    u_description = columns.Text()
    u_time_zone = columns.Text()
    u_geo_enabled = columns.Boolean()
    media_url = columns.Text()
    um_screen_name = columns.Text()
    um_name = columns.Text()
    um_id = columns.Text()
    u_followers_count = columns.Integer()
    u_friends_count = columns.Integer()
    u_listed_count = columns.Integer()
    u_favourites_count = columns.Integer()
    u_utc_offset = columns.Integer()
    u_statuses_count = columns.Integer()
    u_created_at = columns.DateTime()
    hashtags = columns.List(value_type=columns.Text)
    urls = columns.List(value_type=columns.Text)


logging.info('Connecting to cassandra...')

cluster = Cluster(CASSANDRA_IPS)
with cluster.connect() as session:
    logging.info('Creating keyspace...')
    session.execute("""
           CREATE KEYSPACE IF NOT EXISTS %s
           WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
           """ % KEYSPACE)

connection.setup(CASSANDRA_IPS, KEYSPACE, protocol_version=3)
logging.info('Creating table...')
sync_table(Tweet)


def create_dict(event_key, event_kw, tweet):
    return {
        't_id': tweet['id_str'],
        'event_kw': ','.join(event_kw),
        'event_name': event_key,
        't_created_at': time.mktime(time.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')),
        't_text': tweet['text'],
        't_retweet_count': tweet['retweet_count'],
        't_favorite_count': tweet['favorite_count'],
        't_geo': str(tweet['geo']),
        't_coordinates': str(tweet['coordinates']),
        't_favorited': tweet['favorited'],
        't_retweeted': tweet['retweeted'],
        't_is_a_retweet': 'retweeted_status' in tweet,
        't_lang': tweet['lang'],
        'u_id': tweet['user']['id_str'],
        'u_name': tweet['user']['name'],
        'u_screen_name': tweet['user']['screen_name'],
        'u_location': tweet['user']['location'],
        'u_url': tweet['user']['url'],
        'u_lang': tweet['user']['lang'],
        'u_description': tweet['user']['description'],
        'u_time_zone': tweet['user']['time_zone'],
        'u_geo_enabled': bool(tweet['user']['geo_enabled']),
        'u_followers_count': tweet['user']['followers_count'],
        'u_friends_count': tweet['user']['friends_count'],
        'u_favourites_count': tweet['user']['favourites_count'],
        'u_statuses_count': tweet['user']['statuses_count'],
        'u_created_at': time.mktime(time.strptime(tweet['user']['created_at'], '%a %b %d %H:%M:%S +0000 %Y')),
        'hashtags': list(map(lambda h: h['text'], tweet['entities']['hashtags'])),
        'urls': list(map(lambda url: url['url'], tweet['entities']['urls'])),
        # Concat names with a space separation
        'um_screen_name': ' '.join(map(lambda um: str(um['screen_name']), tweet['entities']['user_mentions'])),
        'um_name': ' '.join(map(lambda um: str(um['name']), tweet['entities']['user_mentions'])),
        'um_id': ' '.join(map(lambda um: str(um['id_str']), tweet['entities']['user_mentions'])),
        'media_url': ' '.join(map(lambda m: str(m['media_url_https']), tweet['entities']['media']))
        if 'media' in tweet['entities'] else None,

    }


session = cluster.connect(KEYSPACE)
prep_query = session.prepare("INSERT INTO tweet JSON ?")


def save_tweet(tweet, event_key, event_kw):
    session.execute_async(prep_query, [ujson.dumps(create_dict(event_key, event_kw, tweet)),])

    # Tweet.create(**create_dict(event_key, event_kw, tweet))
