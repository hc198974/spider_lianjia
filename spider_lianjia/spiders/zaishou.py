from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
import json
from pandas import Series, DataFrame
from bs4 import BeautifulSoup
from spider_lianjia.items import SpiderZaishouItem


def get_xiaoqu(n1, n2):
    list = []
    for line in open('data/lianjia.json', 'r', encoding='utf-8'):
        list.append(json.loads(line))

    df = DataFrame(list)
    l = [x for x in df['title']]
    s = [x for x in set(l) if x is not None]
    s.sort()
    item = s[n1:n2]
    return item


def get_content(g):
    guaPai = ''
    previous = ''
    for i in g:
        if i.string == '挂牌时间':
            guaPai = i.find_next_sibling().string
        else:
            if i.string == '上次交易':
                previous = i.find_next_sibling().string

    return guaPai, previous


class ZaishouSpider(CrawlSpider):
    name = 'zaishou'
    allowed_domains = ['dl.lianjia.com']
    s = get_xiaoqu(25, 35)
    current_page = 1
    start_urls = ['https://dl.lianjia.com/ershoufang/pg1rs%s' % p for p in s]
    # rules的工作关系：1、各个rule是并列关系，都是从start_urls里的开始界面，先后有一定影响，但scrapy有去重功能，提取两次也只执行一次
    # 2、有callback的会进入提取的页面执行callback方法，没有callback的会进入该页面重新查找rules规则。
    # 3、linkExtractor使用的是正则表达式，表达式一定要正确，否则会对判断方向产生误导。
    rules = (
        Rule(LinkExtractor(allow='/ershoufang/(pg\d+)?rs[^"]+')),
        Rule(LinkExtractor(allow='/ershoufang/\d+\.html'), callback='parse_item'),
    )

    def parse_item(self, response):
        soup = BeautifulSoup(response.text, 'lxml')
        title = response.xpath(
            '//div[@class="communityName"]/a[1]/text()').get()
        totalPrice = response.xpath('//span[@class="total"]/text()').get()
        unitPrice = response.xpath(
            '//span[@class="unitPriceValue"]/text()').get()
        room = response.xpath(
            "//div[@class='room']/div[@class='mainInfo']/text()").get()
        type = response.xpath(
            "//div[@class='mainInfo' and @title!='']/text()").get()
        area = response.xpath(
            "//div[@class='area' ]/div[@class='mainInfo']/text()").get()
        quyu = response.xpath("//span[@class='info']/a[1]/text()").get()
        district = response.xpath("//span[@class='info']/a[2]/text()").get()
        seller = response.xpath("//div[@class='brokerName']/a/text()").get()
        g = soup.select('.content ul li span')

        guaPai = get_content(g)[0]
        previous = get_content(g)[1]
        item = SpiderZaishouItem(title=title, room=room, area=area, quyu=quyu, district=district,
                                 totalPrice=totalPrice, unitPrice=unitPrice, type=type,
                                 seller=seller, guaPai=guaPai, previous=previous)
        yield item
