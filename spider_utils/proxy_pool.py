import requests
import random
import logging

logger = logging.getLogger(__name__)

def get_proxy():
    res_json = requests.get("http://127.0.0.1:5010/get/?type=https").json()
    if 'proxy' in res_json and res_json['proxy']:
        return res_json['proxy']
    return ''


def delete_proxy(proxy):
    requests.get("http://127.0.0.1:5010/delete/?proxy={}".format(proxy))


def load_proxy_file(proxy_file):
    proxy_list = []
    if proxy_file:
        with open(proxy_file) as f:
            proxy_list = f.read().split('\n')
    logger.info(f'proxy_list_len {len(proxy_list)} {proxy_list}')
    return proxy_list

class ProxyPoolMiddleware(object):
    def __init__(self, crawler):
        super(ProxyPoolMiddleware, self).__init__()
        self.proxy = None
        self.proxy_file = crawler.settings.get('PROXY_FILE', '')
        self.proxy_list = []
        if self.proxy_file:
            self.proxy_list = load_proxy_file(self.proxy_file)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def get_proxy(self, spider):
        if len(self.proxy_list) > 0:
            return self.proxy_list[random.randint(0, len(self.proxy_list) - 1)]
        else:
            return get_proxy()

    def set_proxy(self, request, spider):
        self.proxy = self.get_proxy(spider)
        if self.proxy:
            self.proxy = 'http://' + self.proxy
            # request.meta["proxy"] = self.proxy
            spider.logger.info(f'request url {request.url}, proxy:{self.proxy}')

    def process_request(self, request, spider):
        self.set_proxy(request, spider)

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # if self.proxy:
            # delete_proxy(self.proxy)
        # spider.logger.info('delete proxy: %s' % self.proxy)
        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response
