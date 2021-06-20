from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
import json
from pandas import Series, DataFrame
from bs4 import BeautifulSoup
from spider_lianjia.items import SpiderZaishouItem
import requests
from lxml import etree
import re
import pymongo

def get_xiaoqu(n1, n2):
    #将pymongo数据转换成Dataframe
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["db_lianjia"]
    mycol = mydb["chengjiao"]

    x = list(mycol.find())
    df=DataFrame(x)
    #把没用的id删除掉
    df=df.drop('_id',axis=1)  
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
    s = get_xiaoqu(45, 55)
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


class get_zaishou_one(object):
    def get_totalpage(self, response):
        r = response.xpath("//div[@class='page-box house-lst-page-box']//@page-data")
        totalPage = re.search('\d+', str(r[0])).group()
        return totalPage

    def get_zaishou(self, s):
        html = requests.get('https://dl.lianjia.com/ershoufang/rs' + s)
        r = etree.HTML(html.text)
        totalPage = self.get_totalpage(r)
        l = []
        for x in range(1, (int(totalPage) + 1)):
            print(x)
            html = requests.get('https://dl.lianjia.com/ershoufang/pg' + str(x) + 'rs' + s)
            r = etree.HTML(html.text)
            list = r.xpath("//a[@class='' and @data-el='ershoufang']/@href") + r.xpath(
                "//a[@class='LOGCLICKDATA ']/@href")
            for i in list:
                response = etree.HTML(requests.get(i).text)
                title = response.xpath('//div[@class="communityName"]/a[1]/text()')[0]
                totalPrice = response.xpath('//span[@class="total"]/text()')[0]
                unitPrice = response.xpath('//span[@class="unitPriceValue"]/text()')[0]
                room = response.xpath("//div[@class='room']/div[@class='mainInfo']/text()")[0]
                type = response.xpath("//div[@class='mainInfo' and @title!='']/text()")[0]
                area = response.xpath("//div[@class='area']/div[@class='mainInfo']/text()")[0]
                quyu = response.xpath("//span[@class='info']/a[1]/text()")[0]
                district = response.xpath("//span[@class='info']/a[2]/text()")[0]
                seller = response.xpath("//div[@class='brokerName']/a/text()")[0]
                builtDate = response.xpath("//div[@class='area']/div[@class='subInfo noHidden']/text()")[0]
                soup = BeautifulSoup(requests.get(i).text, 'lxml')
                g = soup.select('.content ul li span')
                guaPai = get_content(g)[0]
                previous = get_content(g)[1]
                l.append([title, totalPrice, unitPrice, room, type, area, quyu, district, seller, builtDate, guaPai,
                          previous])
        return l
        
