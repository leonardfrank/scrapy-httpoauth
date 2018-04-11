import logging
from collections import namedtuple

from bson import DEFAULT_CODEC_OPTIONS
from scrapy import Spider
from scrapy.settings import Settings
from twisted.internet.defer import inlineCallbacks
from txmongo.connection import ConnectionPool

from ..settings.default_settings import MONGODB_COLLECTION
from ..settings.default_settings import MONGODB_DATABASE
from ..utils.get_mongo_uri import get_mongo_uri

logger = logging.getLogger(__name__)

TOKEN = namedtuple('twitter_token', ['client_key',
                                     'client_secret'
                                     'resource_owner_key',
                                     'resource_owner_secret'])


class MongoDBStorage(object):

    def __init__(self, settings: Settings):
        self.settings = settings
        self.uri = get_mongo_uri(self.settings)
        self.codec_options = DEFAULT_CODEC_OPTIONS.with_options(
            unicode_decode_error_handler='ignore')
        self.cnx = None
        self.db = None
        self.coll = None

    @inlineCallbacks
    def open_spider(self, spider: Spider):
        self.cnx = yield ConnectionPool(
            self.uri,
            codec_options=self.codec_options
        )

        self.db = yield getattr(
            self.cnx,
            self.settings[MONGODB_DATABASE]
        )
        self.coll = yield getattr(
            self.db,
            self.settings[MONGODB_COLLECTION]
        )
        self.coll.with_options(codec_options=self.codec_options)

        logger.info(
            'Spider opened: Open the connection to MongoDB: {}'.format(self.uri)
        )

    @inlineCallbacks
    def close_spider(self, spider: Spider):
        yield self.cnx.disconnect()
        logger.info(
            'Spider closed: Close the connection to MongoDB: {}'.format(
                self.uri)
        )

    @inlineCallbacks
    def retrieve_token(self):
        docs = yield self.coll.find()
        return {doc.pop('_id'): doc for doc in docs}
