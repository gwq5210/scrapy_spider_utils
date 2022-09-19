import requests
import logging
from spider_utils.login.baseclient import BaseClient


'''豆瓣客户端'''
class DoubanClient(BaseClient):
    def __init__(self, reload_history=True, **kwargs):
        super(DoubanClient, self).__init__(website_name='douban', reload_history=reload_history, **kwargs)
    '''检查会话是否已经过期, 过期返回True'''
    def checksessionstatus(self, session, infos_return):
        url = 'https://www.douban.com/'
        response = session.get(url)
        if infos_return['username'] in response.text:
            return False
        return True

def douban_client_test():
    douban_client = DoubanClient()
    username = '18682085392'
    pwd = ''
    infos_return, session = douban_client.login(username, pwd, 'scanqr')
    print(infos_return['username'])
    print(session)
    print(session.cookies)
    cookie_string = "; ".join([str(k)+"="+str(v) for k,v in session.cookies.items()])
    print(cookie_string)
    res = requests.get('https://www.douban.com/group/beijingzufang/discussion?start=0&type=new', headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
        'Cookie': cookie_string
    })
    print(res)

if __name__ == '__main__':
    # These two lines enable debugging at httplib level (requests->urllib3->http.client)
    # You will see the REQUEST, including HEADERS and DATA, and RESPONSE with HEADERS but without DATA.
    # The only thing missing will be the response.body which is not logged.
    try:
        import http.client as http_client
    except ImportError:
        # Python 2
        import httplib as http_client
    http_client.HTTPConnection.debuglevel = 1

    # You must initialize logging, otherwise you'll not see debug output.
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True

    douban_client_test()
