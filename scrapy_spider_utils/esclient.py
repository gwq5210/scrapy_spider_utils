import json
import os
import logging
from elasticsearch import Elasticsearch
import pkgutil


logger = logging.getLogger(__name__)


class ESClient(Elasticsearch):
    def __init__(self, index_name, url = ['http://localhost:9200'], index_mapping_file = 'mapping.json', package_name = '', *args, **kwargs):
        super().__init__(url, *args, **kwargs)
        self.url = url
        self.index_name = index_name
        self.index_mapping_file = index_mapping_file
        self.package_name = package_name
        self.create_index()

    @classmethod
    def from_settings(cls, settings):
        es_url = settings.get('ES_URL', 'http://localhost:9200')
        index_name = settings.get('ES_INDEX_NAME')
        index_mapping_file = settings.get('ES_INDEX_MAPPING_FILE_NAME', 'mapping.json')
        es_user = settings.get('ES_USER', '')
        es_password = settings.get('ES_PASSWORD', '')
        http_auth = None
        if es_user or es_password:
            http_auth = (es_user, es_password)
        package_name = settings.get('BOT_NAME', '')
        es_client = None
        if http_auth:
            es_client = cls(index_name, [es_url], index_mapping_file, package_name, http_auth=http_auth)
        else:
            es_client = cls(index_name, [es_url], index_mapping_file, package_name)
        return es_client

    def get(self, id, **kwargs):
        return super().get(index=self.index_name, id=id, **kwargs)

    def index(self, body, **kwargs):
        return super().index(index=self.index_name, body=body, **kwargs)

    def read_index_mapping_content(self):
        if self.package_name:
            data = pkgutil.get_data(self.package_name, self.index_mapping_file)
            if not data:
                logger.error('read index mapping file %s from package %s failed' % (self.index_mapping_file, self.package_name))
            return data

        if not self.index_mapping_file or not os.path.exists(self.index_mapping_file):
                logger.error('es index mapping file %s does not exist' % (self.index_mapping_file))
                return None
        json_str = ''
        with open(self.index_mapping_file) as f:
            json_str = f.read()
        if not json_str:
            logger.error('read es index mapping file(%s) failed' % (self.es_index_mapping_file))
        return json_str

    def create_index(self):
        if self.indices.exists(index=self.index_name):
            logger.info('es index(%s) already exists' % (self.index_name))
            return
        json_str = self.read_index_mapping_content()
        if json_str:
            self.indices.create(index=self.index_name, body=json_str)

if __name__ == '__main__':
    index_name = "douban_house"
    url = "https://gwq5210.com/api/es"
    http_auth = ('gwq5210', '')
    es_client = Elasticsearch([url], http_auth=http_auth)
    doc = es_client.get(index=index_name, id="274839322")
    print(doc)