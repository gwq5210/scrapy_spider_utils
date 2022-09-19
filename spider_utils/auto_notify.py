from mirai_client import MiraiClient
import os
import sys
import pprint
from scrapy import signals
from datetime import datetime
from typing import List

sys.path.append(os.path.abspath(os.path.dirname(os.getcwd())))


class NotifyConfig:
    def __init__(self, keys, recipients, filter_keys=[]):
        self.keys = keys
        self.recipients = recipients
        self.filter_keys = filter_keys

    @classmethod
    def from_config(cls, config):
        return cls(config['keys'], config['recipients'], config['filter_keys'])

    @classmethod
    def from_configs(cls, configs):
        res = []
        for config in configs:
            res.append(NotifyConfig.from_config(config))
        return res

    @classmethod
    def get_recipients(cls, notify_configs):
        recipients = []
        for notify_config in notify_configs:
            recipients.extend(notify_config.recipients)
        return recipients


class NotifyInfo:
    def __init__(self, subject, body, recipients):
        self.subject = subject
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
        self.notify_configs = NotifyConfig.from_configs(settings.get('NOTIFY_CONFIGS'))
        self.mirai_recipients = NotifyConfig.get_recipients(self.notify_configs)
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
        self.mirai_client.send_text_msg(self.mirai_recipients, f'spider {self.spider.name} opened')

    def spider_closed(self, spider):
        self.mirai_client.send_text_msg(self.mirai_recipients, f'spider {self.spider.name} closed')

    def get_notify_infos(self, item) -> List[NotifyInfo]:
        return []

    def get_stat_body(self, item):
        mail_body = 'processed item count: %d, stats info: %s' % (
            self.item_count, pprint.pformat(self.spider.crawler.stats.get_stats()))
        return mail_body

    def get_stat_subject(self, item):
        return 'spider[%s] auto stats info' % (self.spider.name)

    def send_msg(self, recipients, body, subject=None):
        if not subject:
            subject = body
        self.mirai_client.send_text_msg(recipients, body)

    def send_stat_msg(self, item, now_time):
        body = self.get_stat_body(item)
        subject = self.get_stat_subject(item)
        self.spider.logger.info(f'send_stat_msg: subject({subject}), body({body})')
        self.last_stats_time = now_time
        self.send_msg(self.mirai_recipients, body, subject)

    def item_scraped(self, item, spider):
        self.item_count += 1
        notify_infos = self.get_notify_infos(item)
        for notify_info in notify_infos:
            self.send_msg(notify_info.recipients, notify_info.body, notify_info.subject)

        now_time = int(datetime.now().timestamp())
        if self.auto_notify_stats and ((self.item_count % self.auto_notify_item_count_interval == 0) or (now_time - self.last_stats_time >= self.auto_notify_interval)):
            self.send_stat_msg(item, now_time)
        return item
