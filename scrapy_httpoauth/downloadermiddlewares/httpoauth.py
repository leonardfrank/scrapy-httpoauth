import logging
from urllib.parse import urlparse

from oauthlib.oauth1 import Client as Oauth1Client
from oauthlib.oauth1 import SIGNATURE_TYPE_QUERY
from scrapy import Spider
from scrapy import signals
from scrapy.crawler import Crawler
from scrapy.exceptions import NotConfigured
from scrapy.http import Request
from scrapy.http import Response
from scrapy.utils.misc import load_object
from twisted.internet.defer import inlineCallbacks

from ..signals import _reload_tokens

logger = logging.getLogger(__name__)


class HttpOAuth1Middleware(object):
    """Oauth 1.0 RFC 5849"""

    def __init__(self, crawler: Crawler):
        self.crawler = crawler
        self.settings = crawler.settings
        self.storage = load_object(self.settings['TWITTER_TOKEN_STORAGE'])(
            self.settings
        )
        self._cycle_token = None
        self.tokens = {}
        self.all_tokens_id = set()
        self.invalid_tokens_id = set()

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        if not crawler.settings.getbool('OAUTH_ENABLED'):
            raise NotConfigured
        obj = cls(crawler)
        crawler.signals.connect(obj.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(obj.spider_closed, signal=signals.spider_closed)
        return obj

    @inlineCallbacks
    def spider_opened(self, spider: Spider, *args, **kwargs):
        yield self.storage.open_spider(spider)
        self.tokens = yield self.storage.retrieve_token()
        self.all_tokens_id = self.tokens.keys()
        logger.info('{} tokens loaded.'.format(len(self.tokens)))

    @inlineCallbacks
    def spider_closed(self, spider: Spider, *args, **kwargs):
        yield self.storage.close_spider(spider)

    def process_request(self, request: Request, spider: Spider):
        if not request.meta.get('oauth_enabled'):
            return self._set_oauth(request)

    def process_response(self, request: Request, response: Response,
                         spider: Spider):
        if 400 <= response.status < 500:
            meta = request.meta
            if meta.get('oauth_enabled'):
                query = urlparse(response.url).query
                query_params = dict(i.split('=') for i in query.split('&'))

                if response.status == 401:
                    invalid_token_id = query_params.get('oauth_token')
                    self.invalid_tokens_id.add(invalid_token_id)
                    logger.info(
                        'one token invalid: {}'.format(invalid_token_id))

                query_info = {k: v for k, v in query_params.items()
                              if not k.startswith('oauth')}
                r = spider.partial_form_request(
                    formdata=query_info,
                    meta=meta,
                    dont_filter=True
                )
                return self._set_oauth(r)
        return response

    def _set_oauth(self, request: Request):
        if self._cycle_token is None:
            self._cycle_token = self._get_token(request)
        token = next(self._cycle_token)
        while not isinstance(token, type({})):
            token = next(self._cycle_token)
        client = Oauth1Client(
            client_key=token['client_key'],
            client_secret=token['client_secret'],
            resource_owner_key=token['resource_owner_key'],
            resource_owner_secret=token['resource_owner_secret'],
            signature_type=SIGNATURE_TYPE_QUERY)
        uri, headers, body = client.sign(request.url)
        request.meta.update({'oauth_enabled': True})
        oauth_r = request.replace(url=uri, meta=request.meta)
        return oauth_r

    def _get_token(self, request: Request):
        while True:
            self.valid_tokens_id = self.all_tokens_id - self.invalid_tokens_id
            for valid_token_id in self.valid_tokens_id:
                if valid_token_id not in self.invalid_tokens_id:
                    yield self.tokens.get(valid_token_id)
                else:
                    continue
            yield self._reload_coll(request)

    @inlineCallbacks
    def _reload_coll(self, request: Request):
        self.tokens = yield self.storage.retrieve_token()
        self.all_tokens_id = self.tokens.keys()
        self.valid_tokens_id = self.all_tokens_id - self.invalid_tokens_id
        count = len(self.valid_tokens_id)
        self.crawler.signals.send_catch_log(_reload_tokens, count=count,
                                            request=request)
