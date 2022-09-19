import logging
from typing import Optional, Set, Type, TypeVar

from scrapy.http.request import Request
from scrapy.settings import BaseSettings
from scrapy.spiders import Spider
from scrapy.dupefilters import BaseDupeFilter
from elasticsearch import Elasticsearch
from http import HTTPStatus

ESDupeFilterTV = TypeVar("ESDupeFilterTV", bound="ESDupeFilter")

class ESDupeFilter(BaseDupeFilter):
    """Request ES duplicates filter"""

    def __init__(self, es_url: Optional[str] = None, es_index: Optional[str] = None, debug: bool = False) -> None:
        self.es_url = "localhost:9200"
        self.es_index = None
        self.logdupes = True
        self.es_client = None
        self.debug = debug
        self.logger = logging.getLogger(__name__)
        if es_url:
            self.es_url = es_url
        if es_index:
            self.es_index = es_index
        else:
            self.logger.error("es_index not specified! filter is not enabled!")
        if self.es_url and self.es_index:
            self.es_client = Elasticsearch([self.es_url])

    @classmethod
    def from_settings(cls: Type[ESDupeFilterTV], settings: BaseSettings) -> ESDupeFilterTV:
        debug = settings.getbool('DUPEFILTER_DEBUG')
        es_url = settings.get('ES_URL')
        es_index = settings.get('ES_INDEX_NAME')
        return cls(es_url, es_index, debug)

    def is_request_dup(self, request: Request, res) -> bool:
        return res and "found" in res and res["found"]

    def request_seen(self, request: Request) -> bool:
        fp = self.request_fingerprint(request)
        if fp and self.es_client:
            res = self.es_client.get(index=self.es_index, id=fp, ignore=[HTTPStatus.NOT_FOUND])
            return self.is_request_dup(request, res)
        else:
            return False

    def request_fingerprint(self, request: Request) -> str:
        if request.cb_kwargs and 'id' in request.cb_kwargs:
            return request.cb_kwargs['id']
        else:
            self.logger.warning(f'Request[{request.url}] cb_kwargs not found "id" argument, do not filter.')
            return ''

    def close(self, reason: str) -> None:
        pass

    def log(self, request: Request, spider: Spider) -> None:
        unique_id = self.request_fingerprint(request)
        if self.debug:
            msg = "Filtered duplicate request: %(request)s (unique_id: %(unique_id)s)"
            args = {'request': request, 'unique_id': unique_id}
            self.logger.info(msg, args, extra={'spider': spider})
        elif self.logdupes:
            msg = ("Filtered duplicate request: %(request)s (unique_id: %(unique_id)s)"
                   " - no more duplicates will be shown"
                   " (see DUPEFILTER_DEBUG to show all duplicates)")
            self.logger.info(msg, {'request': request, 'unique_id': unique_id}, extra={'spider': spider})
            self.logdupes = False

        spider.crawler.stats.inc_value('dupefilter/filtered', spider=spider)
