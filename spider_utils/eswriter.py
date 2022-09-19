# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import os
import logging
from itemadapter import ItemAdapter
from elasticsearch import Elasticsearch
from http import HTTPStatus

logger = logging.getLogger(__name__)


class ESClient(Elasticsearch):
    def __init__(self, index_name, url = ['http://localhost:9200'], index_mapping_file = 'mapping.json'):
        super().__init__(url)
        self.url = url
        self.index_name = index_name
        self.index_mapping_file = index_mapping_file
        self.create_index()

    @classmethod
    def from_settings(cls, settings):
        es_url = settings.get('ES_URL', 'localhost:9200')
        index_name = settings.get('ES_INDEX_NAME')
        es_client = cls(index_name, [es_url])
        return es_client

    def get(self, id, **kwargs):
        return super().get(index=self.index_name, id=id, **kwargs)

    def index(self, body, **kwargs):
        return super().index(index=self.index_name, body=body, **kwargs)

    def create_index(self):
        if self.indices.exists(index=self.index_name):
            logger.info('es index(%s) already exists' % (self.index_name))
            return
        if not self.index_mapping_file or not os.path.exists(self.index_mapping_file):
            logger.error('es index mapping file %s does not exist' % (self.index_mapping_file))
            return
        json_str = ''
        with open(self.index_mapping_file) as f:
            json_str = f.read()
        if json_str:
            self.indices.create(index=self.index_name, body=json_str)
        else:
            logger.error('read es index mapping file(%s) failed' % (self.es_index_mapping_file))

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

if __name__ == '__main__':
    index_name = "douban_house"
    url = "https://gwq5210.com/es"
    es_client = Elasticsearch([url])
    doc = es_client.get(index=index_name, id="274839322")
    print(doc)