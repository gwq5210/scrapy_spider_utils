# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from http import HTTPStatus
from scrapy_spider_utils.esclient import ESClient


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

    def set_crawl_time(self, res, item):
        if 'status' in res and res['status'] == HTTPStatus.NOT_FOUND:
            return
        if "first_crawl_time" in item.fields:
            if res["found"] and "first_crawl_time" in res["_source"]:
                item["first_crawl_time"] = res["_source"]["first_crawl_time"]
            else:
                item["first_crawl_time"] = 0
    
    def set_msg_sended(self, res, item):
        if 'status' in res and res['status'] == HTTPStatus.NOT_FOUND:
            return
        if "msg_sended" in item.fields and res["found"] and "msg_sended" in res["_source"]:
            item["msg_sended"] = res["_source"]["msg_sended"]

    def process_item(self, item, spider):
        if not self.es_client or "id" not in item.keys():
            return item
        res = self.es_client.get(id=item["id"], ignore=[HTTPStatus.NOT_FOUND])
        spider.logger.debug(f'es get {res}')
        self.set_crawl_time(res, item)
        self.set_msg_sended(res, item)
        res = self.es_client.index(id=item["id"], body=ItemAdapter(item).asdict())
        spider.logger.debug(f'es index {res}')
        return item
