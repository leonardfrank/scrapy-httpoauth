import logging
import pprint

from scrapy import Spider
from scrapy import signals
from scrapy.crawler import Crawler
from scrapy.exceptions import NotConfigured
from scrapy.http import Request

from ..signals import _reload_tokens

logger = logging.getLogger(__name__)

pp = pprint.PrettyPrinter(indent=4, width=120)


class AutoTap(object):

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        return cls(crawler)

    def __init__(self, crawler):
        self.crawler = crawler
        self.settings = crawler.settings
        if not crawler.settings.getbool('AUTOTAP_ENABLED'):
            raise NotConfigured

        self.debug = crawler.settings.getbool('AUTOTAP_DEBUG')
        crawler.signals.connect(self._spider_opened,
                                signal=signals.spider_opened)
        crawler.signals.connect(self._reload_tokens, signal=_reload_tokens)

    def _spider_opened(self, spider: Spider):
        self.min_delay = self._min_delay(spider)
        self.max_delay = self._max_delay(spider)
        spider.download_delay = self._start_delay(spider)

    def _min_delay(self, spider: Spider):
        return getattr(spider, 'download_delay',
                       self.settings.getfloat('DOWNLOAD_DELAY'))

    def _max_delay(self, spider: Spider):
        return self.settings.getfloat('AUTOTAP_MAX_DELAY')

    def _start_delay(self, spider: Spider):
        return max(self.min_delay,
                   self.settings.getfloat('AUTOTAP_START_DELAY'))

    def _reload_tokens(self, count: int, request: Request):
        logger.info('{} tokens available now'.format(count))
        key, slot = self._get_slot()

        old_delay = slot.delay
        self._adjust_delay(slot, count)
        if self.debug:
            diff = slot.delay - old_delay
            conc = slot.concurrency

            logger.info("slot: %(slot)s | conc:%(concurrency)2d | "
                        "delay: %(delay)5d ms (%(delaydiff)+d) | ",
                        {'slot': key, 'concurrency': conc,
                         'delay': slot.delay * 1000, 'delaydiff': diff * 1000
                         })

    def _get_slot(self):
        spider = self.crawler.spider
        key = spider.allowed_domains[0]
        slots = self.crawler.engine.downloader.slots
        slot = slots.get(key)
        return key, slot

    def _adjust_delay(self, slot, count):
        target_concurrency = self.settings.getfloat(
            'AUTOTAP_TARGET_CONCURRENCY')
        slot.concurrency = count if count < target_concurrency else target_concurrency
        try:
            new_delay = self.settings.getfloat(
                'AUTOTAP_LOOP_TIME') / slot.concurrency
        except ZeroDivisionError:
            new_delay = max(slot.delay, self.max_delay)
            self.concurrency = 1
        else:
            pass
        finally:
            slot.delay = new_delay
