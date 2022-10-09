import requests
import logging
import http
import atexit
from urllib.parse import urljoin

logger = logging.getLogger(__name__)


class MiraiClient:
    OK = 0

    def __init__(self, sender, api_key, api_url='http://localhost:8080/'):
        self.api_url = api_url
        if not api_url.endswith("/"):
            self.api_url += "/"
        self.verify = True
        self.sender = int(sender)
        self.api_key = api_key
        self.session = ''
        self.verify_url = urljoin(self.api_url, 'verify')
        self.bind_url = urljoin(self.api_url, 'bind')
        self.release_url = urljoin(self.api_url, 'release')
        self.send_msg_url = urljoin(self.api_url, 'sendFriendMessage')
        self.bind()
        atexit.register(self.release)

    @classmethod
    def from_settings(cls, settings):
        client = cls(
            sender=settings.getint('MIRAI_SENDER', 0),
            api_key=settings.get('MIRAI_API_KEY'),
            api_url=settings.get('MIRAI_API_URL', 'http://localhost:8080')
        )
        return client

    def process_res(self, res):
        if res.status_code != http.HTTPStatus.OK:
            logger.error(f'request {res.url} failed, status: {res.status_code}')
            return None
        try:
            res_json = res.json()
            if res_json['code'] != self.OK:
                logger.error(f'request {res.url} failed, {res_json}')
                return None
            else:
                logger.info(f'request {res.url} success, {res_json}')
                return res_json
        except Exception as e:
            logger.error(f'request {res.url} failed, exception: {e}')
            return None


    def bind(self):
        res = requests.post(self.verify_url, json={
            'verifyKey': self.api_key
        }, verify=self.verify)
        res_json = self.process_res(res)
        if not res_json:
            return False
        self.session = res_json['session']
        res = requests.post(self.bind_url, json={
            'sessionKey': self.session,
            'qq': self.sender,
        }, verify=self.verify)
        if not self.process_res(res):
            return False

    def release(self):
        res = requests.post(self.release_url, json={
            'sessionKey': self.session,
            'qq': self.sender,
        }, verify=self.verify)
        self.process_res(res)

    def send_text_msg(self, recipients, msg):
        if type(recipients) is int or type(recipients) is str:
            self._send_text_msg(recipients, msg)
            return
        for recipient in recipients:
            self._send_text_msg(recipient, msg)

    def _send_text_msg(self, recipient, msg):
        res = requests.post(self.send_msg_url, json={
            "sessionKey": self.session,
            "target": int(recipient),
            "messageChain": [
                {"type": "Plain", "text": msg},
            ]
        }, verify=self.verify)
        res_json = self.process_res(res)
        logger.info(f'client_send_text_msg({msg}) to ({recipient}) {res_json}')


if __name__ == '__main__':
    sh = logging.StreamHandler()  # 往屏幕上输出
    logger.setLevel(logging.DEBUG)
    logger.addHandler(sh)
    sender = 2423087292
    api_key = 'a123456789'
    api_url = "https://mirai.gwq5210.com"
    recipients = [457781132]
    client = MiraiClient(sender, api_key, api_url)
    client.send_text_msg(recipients, 'hello world')
