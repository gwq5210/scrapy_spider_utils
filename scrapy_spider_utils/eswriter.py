# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from http import HTTPStatus
from scrapy_spider_utils.esclient import ESClient
from datetime import datetime


class ESWriterPipeline:
    def __init__(self, settings):
        self.es_client = ESClient.from_settings(settings)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def open_spider(self, spider):
        pass

    def close_spider(self, spider):
        pass

    def set_crawl_time(self, res, item, spider):
        if 'status' in res and res['status'] == HTTPStatus.NOT_FOUND:
            return
        now_time = int(datetime.now().timestamp())
        if "first_crawl_time" in item.fields:
            if res["found"] and "first_crawl_time" in res["_source"] and res["_source"]["first_crawl_time"] != 0:
                item["first_crawl_time"] = res["_source"]["first_crawl_time"]
            else:
                item["first_crawl_time"] = now_time
        if "crawl_time" in item.fields:
            item["crawl_time"] = now_time

    def before_write_impl(self, res, item, spider):
        self.set_crawl_time(res, item, spider)
        self.before_write(res, item, spider)

    def before_write(self, res, item, spider):
        pass

    def process_item(self, item, spider):
        if not self.es_client or "id" not in item.keys():
            return item
        res = self.es_client.get(id=item["id"], ignore=[HTTPStatus.NOT_FOUND])
        spider.logger.debug(f'es get {res}')
        self.before_write_impl(res, item, spider)
        res = self.es_client.index(id=item["id"], body=ItemAdapter(item).asdict())
        spider.logger.debug(f'es index {res}')
        return item
