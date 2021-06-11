import requests
import scrapy
from lxml import etree
from scrapy import Request
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
import json
from pandas import Series,DataFrame
from bs4 import BeautifulSoup
from spider_lianjia.items import SpiderZaishouItem
import re

def get_xiaoqu(n1,n2):
    list = []
    for line in open('data/lianjia.json', 'r', encoding='utf-8'):
        list.append(json.loads(line))

    df = DataFrame(list)
    l=[x for x in set(df['title'])]
    item=l[n1:n2]
    print(item)
    return item

def get_totalpage(str):
    html=requests.get('https://dl.lianjia.com/ershoufang/pg1rs'+str)
    response=etree.HTML(html.text)
    total_page = response.xpath(
        "//div[@class='page-box house-lst-page-box']//@page-data")
    total_page = int(re.search('\d+',total_page[0]).group())
    return total_page

class ZaishouSpider(CrawlSpider):
    name = 'zaishou'
    allowed_domains = ['dl.lianjia.com']
    s=get_xiaoqu(0,10)
    for i in s:
        page=get_totalpage(i)
        for j in range(1,page+1):
            start_urls = ['https://dl.lianjia.com/ershoufang/pg'+str(j)+'rs'+str(i)]

    rules = (
        Rule(LinkExtractor(allow='./ershoufang/\d+\.html'),callback='parse_item'),  # allow里面是正则表达式
        # Rule(LinkExtractor(allow='/ershoufang/pg(\d+)(rs[^"]+)'), callback='parse_item')  # allow里面是正则表达式

    )

    def parse_item(self, response):
        soup=BeautifulSoup(response.text,'lxml')
        title = response.xpath('//div[@class="communityName"]/a[1]/text()').get()
        totalPrice = response.xpath('//span[@class="total"]/text()').get()
        unitPrice = response.xpath('//span[@class="unitPriceValue"]/text()').get()
        room = response.xpath("//div[@class='room']/div[@class='mainInfo']/text()").get()
        type = response.xpath("//div[@class='mainInfo' and @title!='']/text()").get()
        area = response.xpath("//div[@class='area' ]/div[@class='mainInfo']/text()").get()
        quyu = response.xpath("//span[@class='info']/a[1]/text()").get()
        district = response.xpath("//span[@class='info']/a[2]/text()").get()
        seller=response.xpath("//div[@class='brokerName']/a/text()").get()
        g=soup.select('.content ul li span')
        for i in g:
            if i.string=='挂牌时间':
                target1=i
            if i.string=='上次交易':
                target2=i

        guaPai=target1.find_next_sibling().string
        previous=target2.find_next_sibling().string
        item = SpiderZaishouItem(title=title, room=room, area=area, quyu=quyu,district=district,
                                 totalPrice=totalPrice, unitPrice=unitPrice,previous=previous,type=type,
                                 guaPai=guaPai, seller=seller)
        yield item

