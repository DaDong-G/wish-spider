import hashlib
import random
import re
import time
import traceback
from queue import Queue
from threading import Thread
from hashlib import md5

import datetime
import pymongo
import requests
import sys
from retrying import retry

from logs import Mylog

mylog = Mylog("Wish_logs")
mongo = pymongo.MongoClient()
collection = mongo['wish']['msg']


def make_csrf_token():
    csrf_token_8 = ''.join(random.sample('01234567890123456789fedcbafedcbafedcbafedcba', 8))
    csrf_token_32 = ''.join(random.sample('01234567890123456789fedcbafedcbafedcbafedcba', 32))
    return '2' + "|" + csrf_token_8 + '|' + csrf_token_32 + "|" + str(int(time.time()))


def get_useragent():
    useragent_list = [
        "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.133 Safari/534.16",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 OPR/26.0.1656.60",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.101 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 OPR/26.0.1656.60",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11",
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 SE 2.X MetaSr 1.0",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 UBrowser/4.0.3214.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
        "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24"
    ]
    return random.choice(useragent_list)


def proxies():
    """
    代理
    :return: 代理
    """
    proxies = [
        "http://beijiu572:WBRUHG4AQ6L7UQIJ95W9PEEM@37.114.122.229:32555",
        "http://beijiu572:WBRUHG4AQ6L7UQIJ95W9PEEM@45.130.124.97:13000",
        "http://beijiu572:WBRUHG4AQ6L7UQIJ95W9PEEM@45.130.124.100:13000",
        "http://beijiu572:WBRUHG4AQ6L7UQIJ95W9PEEM@82.211.18.17:43556"
    ]
    p = random.choice(proxies)
    proxy = {
        'http': p,
        'https': p
    }
    return proxy


class GetAllProductsLink():

    def __init__(self, category_url, product_link_queue, product_category):
        """
        :param category_url: 店铺链接
        :param product_link_queue: 商品链接队列
        """
        self.category_url = category_url
        self.product_link_queue = product_link_queue
        self.product_category = product_category
        self.start = 0

    # 采集
    def clawer(self, category_url):
        try:

            # time.sleep(random.randint(1, 2))
            data = self.request(category_url, self.start)
            self.get_productlink(category_url, data)

            if self.start <= 950:
                print(self.start)
                self.start += 50
                self.clawer(category_url)

            else:
                self.start = 0
                return

        except Exception as err:
            print(err)
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()

    # 通过requests请求数据
    # @retry(stop_max_attempt_number=5)
    def request(self, category_url, start):
        try:
            query = re.search(r'feed/(.+)[?]?', category_url).group(1)
            headers = {
                'sec-fetch-mode': 'cors',
                'origin': 'https://www.wish.com',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'zh-CN,zh;q=0.9,und;q=0.8',
                'cookie': 'notice_gdpr_prefs=0,1,2:; notice_preferences=2:; G_ENABLED_IDPS=google; '
                          '_fbp=fb.1.1570605410969.840145514; cto_lwid=6c2c05d2-cfb6-44e9-8262-52623b8fabec; '
                          '__stripe_mid=b4cec38e-6858-47b2-b32b-2f3c9249bc14; __utmz=96128154.1570609102.1.1.utmcsr=('
                          'direct)|utmccn=(direct)|utmcmd=(none); '
                          '_xsrf=2|02cfc6b7|3172778b79e67f7c92f6e8c9409fa676|1570619572; '
                          '_ga=GA1.2.993791185.1570609102; __zlcmid=uiibSPdZGdchch; '
                          '__utma=96128154.993791185.1570609102.1570785694.1570879036.3; '
                          '_derived_ep ik=dj0yJnU9MGQxVXU4V2s5TUc3VjFMXzJDR2FBNUJBQTB6NE1jRWImbj1KNG54OS1KVUNFUGlzTFJGZlVlMUpBJm09NyZ0PUFBQUFBRjJodWQ4; sweeper_session="2|1:0|10:1571020401|15:sweeper_session|84:YzViYWIyYzktMTgxYS00MGNiLWIxODYtNTIxMjNmNWNiYWYxMjAxOS0xMC0wOSAwNzoxODowMi44NzgwNzA=|5e601a4a6149e20143a28c7dc277651a21fd93122e1bf477e8120821016776c5"; sessionRefreshed_5d9d89aa3105d542bbc0b1ac=true; isDailyLoginBonusModalLoaded=true; bsid=646415312cfc1304ed3e62d26d52f83f; _timezone=8; _is_desktop=true; __stripe_sid=4af6bb19-98d4-4937-81f2-3373d393121c; sweeper_uuid=a76859313fd448c5b4d555f538d723e0',
                'user-agent': get_useragent(),
                'content-type': 'application/x-www-form-urlencoded',
                'accept': 'application/json, text/plain, */*',
                'referer': category_url,
                'authority': 'www.wish.com',
                'sec-fetch-site': 'same-origin',
                'x-xsrftoken': '2|02cfc6b7|3172778b79e67f7c92f6e8c9409fa676|1570619572',
            }

            post_data = {
                "offset": start,
                "count": "50",
                "request_categories": False,
                "request_id": query,
                "request_branded_filter": False,
            }
            url = 'https://www.wish.com/api/feed/get-filtered-feed'
            try:
                # print('准备请求')
                res = requests.post(url, headers=headers, data=post_data, proxies=proxies())
            except Exception as e:
                mylog.logs().exception(sys.exc_info())
                return
            return res.json()

        except:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()

    # 获取商品链接
    def get_productlink(self, category_url, data):
        try:
            data = data['data']
            product_datas = data['products']
            if product_datas:
                for product_data in product_datas:
                    product_id = product_data['id']
                    protuct_link = f'{category_url}/product/{product_id}'
                    print(protuct_link)
                    queue_item = {"product_category": self.product_category,
                                  "product_link": protuct_link}
                    self.product_link_queue.put(queue_item)
            #     return True
            # else:
            #     return False
        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()

    def run(self):
        '''
        :return: 返回类别的所有商品链接
        '''
        try:

            self.clawer(self.category_url)
            print('已获取该类别所有连接！')
        except:
            mylog.logs().exception(sys.exc_info())
            mylog.logs().log('获取类别商品出错')
            traceback.print_exc()


class ParseProduct(Thread):
    def __init__(self, product_link_queue):
        super().__init__()
        # Thread.__init__(self)
        self.threadName = '采集线程' + ' ' + str(self._name)
        self.product_link_queue = product_link_queue
        # self.product_info_queue = product_info_queue

    def get_msg(self, product_link):
        try:
            cid = re.search(r'product/(.+)', product_link).group(1)
            csrf = make_csrf_token()
            headers = {
                "Host": "www.wish.com",
                "Connection": "keep-alive",
                'x-xsrftoken': csrf,
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": get_useragent(),
                # "User-Agent": get_useragent(),
                "Accept": "application/json, text/plain, */*",
                "Origin": "https://www.wish.com",
                "Content-Type": "application/x-www-form-urlencoded",
                # "Referer": "https://www.wish.com/feed/tabbed_feed_latest/product/5cad47469b83af6034a11c5e?&source=tabbed_feed_latest",
                "Referer": product_link,
                "Accept-Language": "en-US,en;q=0.9",
                'cookie': '_xsrf='+ csrf
            }

            post_data = {
                "cid": cid,
                "do_not_track": "false",
                "request_sizing_chart_info": "true",
            }

            url = 'https://www.wish.com/api/product/get'
            res = requests.post(url, headers=headers, data=post_data, timeout=30, proxies=proxies())
            # verify = False,
            print(res.url, res.status_code)
            # print(res.json())
            # with open('a.json', 'w', encoding='utf-8') as f:
            #     f.write(res.text)
            return res.json()

        except:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()

    def prase_product(self, product_data, product_category):
        try:
            product_info = {}

            data = product_data['data']
            # 商品类别
            product_info['category'] = product_category

            # 商品卖家
            seller_name = data['contest']['merchant_info']['title']
            product_info['seller_name'] = seller_name
            # 商品链接
            # web_url = data['app_indexing_data']['web_url']
            web_url = data['contest']['permalink']
            product_info['product_url'] = web_url
            # 商品id
            product_id = data['contest']['id']
            product_info['product_id'] = product_id
            # 商品名称
            name = data['contest']['name']
            product_info['product_name'] = name
            # 商品主图
            contest_page_picture = data['contest']['contest_page_picture']
            product_info['img_url'] = contest_page_picture
            # 商品图片
            extra_photo_urls = data['contest']['extra_photo_urls']
            extra_photo_urls = {item[0]: item[1].replace('small', 'large') for item in extra_photo_urls.items()}
            extra_photo_urls['0'] = contest_page_picture
            product_info['img_urls'] = extra_photo_urls
            # 商品关键词
            keywords = data['contest']['keywords'].split(',')
            product_info['keywords'] = keywords
            # 浏览量
            num_entered = data['contest']['num_entered']
            product_info['num_entered'] = num_entered
            # 商品描述
            description = data['contest']['description']
            product_info['description'] = description
            # 成交量
            purchase_amount = data['contest']['num_bought']
            product_info['purchase amount'] = purchase_amount
            # 评论数
            comments = int(data['contest']['product_rating']['rating_count'])
            product_info['comments'] = comments
            # 星级
            grade_star = data['contest']['product_rating']['rating']
            product_info['grade_star'] = str(round(grade_star, 1))

            # 商品属性列表
            product_attr_list = data['contest']['commerce_product_info']['variations']
            attr_data_list = []
            for attr_data in product_attr_list:
                try:
                    attr_color = attr_data['color']
                    attr_size = attr_data['size']
                    attr_price = int(attr_data['localized_price']['localized_value'])
                    attr_currency = attr_data['localized_price']['currency_code']
                    attr_photo_id = attr_data['sequence_id']
                    if str(attr_photo_id) in list(extra_photo_urls.keys()):
                        attr_photo_url = extra_photo_urls[str(attr_photo_id)]
                    else:
                        attr_photo_url = extra_photo_urls['0']
                    attr_data_list.append(
                        {'attr_color': attr_color, 'attr_size': attr_size, 'attr_price': attr_price,
                         "attr_currency": attr_currency,
                         'attr_photo_url': attr_photo_url})
                except Exception as err:
                    mylog.logs().exception(sys.exc_info())
                    traceback.print_exc()

            # print('商品信息：', product_info['product_id'], product_info['product_name'])
            product_info['attr_data_list'] = attr_data_list
            return product_info

        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()

    def clawer(self, product_link, product_category):
        # try:
        print('正在采集商品信息：', product_link)
        # time.sleep(random.randint(1, 3))
        data = self.get_msg(product_link)
        product_info = self.prase_product(data, product_category)
        # print(product_info)
        self.save_msg(product_info)

    def save_msg(self, product_info):
        try:
            md = hashlib.md5()
            md.update(product_info['product_name'].encode())
            mongo_id = md.hexdigest()
            crawl_time = datetime.datetime.now()
            product_info['crawl_time'] = crawl_time
            collection.save({"_id": mongo_id, "detail": product_info})
            print('保存商品成功，商品信息：', product_info['product_id'], product_info['product_name'])

        except Exception as err:
            print('保存数据库失败')
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()

    def run(self):
        print('启动：', self.threadName)
        time.sleep(1)
        while True:
            product_queue_item = self.product_link_queue.get()
            product_link = product_queue_item['product_link']
            product_category = product_queue_item['product_category']
            # product_link = "https://www.wish.com/feed/tag_54ac6e18f8a0b3724c6c473f/product/5a684cb6ac373c665329886c"
            self.clawer(product_link, product_category)


def main(product_dict):
    url = 'https://www.wish.com/feed/'
    for cagegory_tag, cagegory in product_dict.items():
        print(cagegory_tag, cagegory)
        get_all_products_link = GetAllProductsLink(url + cagegory_tag, product_link_queue, cagegory)
        get_all_products_link.run()
        # break

    thread_list = []
    for i in range(1, 6):
        thread = ParseProduct(product_link_queue=product_link_queue)
        thread.setDaemon(True)
        thread.start()
        thread_list.append(thread)

    while True:
        if not product_link_queue.empty():
            pass
        else:
            break

    time.sleep(10)
    print('爬虫结束')


product_dict = {
    "tag_53dc186421a86318bdc87f31": "Shoes",
    'tag_53dc186421a86318bdc87f28': "Makeup & Beauty",
    'tag_53dc186421a86318bdc87f22': "Wallets & Bags",
    "tag_53dc186321a86318bdc87ef9": "Tops",
    "tag_53dc186321a86318bdc87f07": "Bottoms",
    "tag_53dc186421a86318bdc87f16": "Accessories",
    "tag_53dc186421a86318bdc87f1c": "Watches",
    "tag_53dc186421a86318bdc87f0f": "Phone Upgrades",
    "tag_53e9157121a8633c567eb0c2": "Home Decor",
    "tag_53dc186421a86318bdc87f20": "Gadgets",
    "tag_53e9157121a8633c567eb0cf": "Baby & Kids",
    "tag_54ac6e18f8a0b3724c6c473f": "Hobbies",
    "tag_5499e5f0f8a0b3220598bd3b_fine": "Sterling Silver Jewelry"
}
product_link_queue = Queue()

# # 商品链接队列

if __name__ == '__main__':
    main(product_dict)
