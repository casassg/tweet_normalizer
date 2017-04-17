import logging

import time
from cassandra.cluster import Cluster
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.models import Model
from cassandra.cqlengine import columns, connection
import socket

import os

CASSANDRA_IPS = list(
    map(socket.gethostbyname, os.environ.get('CASSANDRA_NODES', '127.0.0.1').replace(' ', '').split(',')))
KEYSPACE = 'twitter_analytics'


class Tweet(Model):
    id = columns.Text(primary_key=True)
    event_kw = columns.Text()
    event_name = columns.Text()
    t_id = columns.Text()
    t_created_at = columns.DateTime()
    t_text = columns.Text()
    raw = columns.Text()
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


def create_dict(event_key, event_kw):

    return {
        'id': lambda x: event_key + x['id_str'],
        't_id': lambda x: x['id_str'],
        'event_kw': lambda x: ','.join(event_kw),
        'event_name': lambda x: event_key,
        't_created_at': lambda x: time.mktime(time.strptime(x['created_at'], '%a %b %d %H:%M:%S +0000 %Y')),
        't_text': lambda x: x['text'],
        't_retweet_count': lambda x: x['retweet_count'],
        't_favorite_count': lambda x: x['favorite_count'],
        't_geo': lambda x: str(x['geo']),
        't_coordinates': lambda x: str(x['coordinates']),
        't_favorited': lambda x: x['favorited'],
        't_retweeted': lambda x: x['retweeted'],
        't_is_a_retweet': lambda x: 'retweeted_status' in x,
        't_lang': lambda x: x['lang'],
        'u_id': lambda x: x['user']['id_str'],
        'u_name': lambda x: x['user']['name'],
        'u_screen_name': lambda x: x['user']['screen_name'],
        'u_location': lambda x: x['user']['location'],
        'u_url': lambda x: x['user']['url'],
        'u_lang': lambda x: x['user']['lang'],
        'u_description': lambda x: x['user']['description'],
        'u_time_zone': lambda x: x['user']['time_zone'],
        'u_geo_enabled': lambda x: bool(x['user']['geo_enabled']),
        'u_followers_count': lambda x: x['user']['followers_count'],
        'u_friends_count': lambda x: x['user']['friends_count'],
        'u_favourites_count': lambda x: x['user']['favourites_count'],
        'u_statuses_count': lambda x: x['user']['statuses_count'],
        'u_created_at': lambda x: time.mktime(time.strptime(x['user']['created_at'], '%a %b %d %H:%M:%S +0000 %Y')),
        'hashtags': lambda x: list(map(lambda h:h['text'], x['entities']['hashtags'])),
        'urls': lambda x: list(map(lambda url:url['url'], x['entities']['urls'])),
        # 'um_screen_name': lambda x: x['user']['geo_enabled'],
        # 'media_url': lambda x: x['user']['geo_enabled'],
        # 'um_name': lambda x: x['user']['geo_enabled'],
        # 'um_id': lambda x: x['user']['geo_enabled'],

    }


def save_tweet(tweet,raw_tweet, event_dict):
    kwargs = {}
    for k, f in event_dict.items():
        conversed = f(tweet)
        kwargs[k] = conversed if conversed else None
    kwargs['raw'] = raw_tweet
    Tweet.create(**kwargs)
