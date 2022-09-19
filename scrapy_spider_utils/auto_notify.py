from scrapy_spider_utils.mirai_client import MiraiClient
import os
import sys
import pprint
from scrapy import signals
from datetime import datetime
from typing import List

sys.path.append(os.path.abspath(os.path.dirname(os.getcwd())))


class NotifyInfo:
    def __init__(self, body, recipients):
        self.body = body
        self.recipients = recipients


class AutoNotify:
    def __init__(self, settings):
        self.spider = None
        self.item_count = 0
        self.auto_notify_stats = settings.getbool('AUTO_NOTIFY_STATS', False)
        self.auto_notify_interval = settings.getint('AUTO_NOTIFY_INTERVAL', 3600)
        self.auto_notify_item_count_interval = settings.getint('AUTO_NOTIFY_ITEM_COUNT_INTERVAL', 1000)
        self.last_stats_time = int(datetime.now().timestamp())
        self.stat_recipients = settings.getlist('AUTO_NOTIFY_RECIPIENTS')
        self.mirai_client = MiraiClient.from_settings(settings)

    @classmethod
    def from_crawler(cls, crawler):
        # instantiate the extension object
        ext = cls(crawler.settings)

        # connect the extension object to signals
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)

        return ext

    def spider_opened(self, spider):
        self.spider = spider
        self.mirai_client.send_text_msg(self.stat_recipients, f'spider {self.spider.name} opened')

    def spider_closed(self, spider):
        self.mirai_client.send_text_msg(self.stat_recipients, f'spider {self.spider.name} closed')

    def get_notify_infos(self, item) -> List[NotifyInfo]:
        return []

    def get_stat_body(self, item):
        mail_body = 'processed item count: %d, stats info: %s' % (
            self.item_count, pprint.pformat(self.spider.crawler.stats.get_stats()))
        return f'[{self.spider.name}]\n{mail_body}'

    def send_msg(self, recipients, body):
        self.mirai_client.send_text_msg(recipients, body)

    def send_stat_msg(self, item, now_time):
        body = self.get_stat_body(item)
        self.spider.logger.info(f'send_stat_msg: body({body})')
        self.last_stats_time = now_time
        self.send_msg(self.stat_recipients, body)

    def item_scraped(self, item, spider):
        self.item_count += 1
        notify_infos = self.get_notify_infos(item)
        for notify_info in notify_infos:
            self.send_msg(notify_info.recipients, notify_info.body)

        now_time = int(datetime.now().timestamp())
        if self.auto_notify_stats and ((self.item_count % self.auto_notify_item_count_interval == 0) or (now_time - self.last_stats_time >= self.auto_notify_interval)):
            self.send_stat_msg(item, now_time)
        return item
