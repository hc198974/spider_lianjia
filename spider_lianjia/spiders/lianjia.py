import scrapy
from scrapy import Request
from scrapy import linkextractors
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from spider_lianjia.items import SpiderLianjiaItem
from bs4 import BeautifulSoup
import re


class LianjiaSpider(CrawlSpider):
    name = 'lianjia'
    allowed_domains = ['dl.lianjia.com']
    current_page = 1
    start_urls = ['https://dl.lianjia.com/chengjiao/pg%s' %
                  p for p in range(70, 80)]

    rules = (
        Rule(LinkExtractor(allow='./chengjiao/.+\.html')),  # allow里面是正则表达式
        Rule(LinkExtractor(allow='/chengjiao/c\d+'), callback='parse_chengjiao')
    )

    def parse_chengjiao(self, response):
        if self.current_page == 1:
            # 获得totalpage
            total_page = response.xpath(
                "//div[@class='page-box house-lst-page-box']//@page-data").re("\d+")
            total_page = int(total_page[0])
            lianjie = response.xpath('//h1/a/@href').get()

            for i in range(1, total_page+1):
                url = 'https://dl.lianjia.com' + \
                    re.search('.+chengjiao/', lianjie).group()+'pg' + \
                    str(i)+re.search('c\d+', lianjie).group()
                yield Request(url, callback=self.parse_lianjia)

    def parse_lianjia(self, response):
        temp = response.xpath('//div[@class="title"]/a/text()').getall()
        dealDate = response.xpath('//div[@class="dealDate"]/text()').getall()
        totalPrice = response.xpath(
            '//div[@class="totalPrice"]/span/text()').getall()
        unitPrice = response.xpath(
            '//div[@class="unitPrice"]/span/text()').getall()
        dealCycle = response.xpath(
            "//span[@class='dealCycleTxt']/span[2]/text()").getall()
        guaPai = response.xpath(
            "//span[@class='dealCycleTxt']/span[1]/text()").getall()  # 挂牌价有的有有的没有
        soup = BeautifulSoup(response.text, 'lxml')
        s = soup.select('.info ')

        for i in range(0, len(dealDate)):
            if s[i].select('.agent_name') == []:
                seller = '无'
            else:
                seller = s[i].select('.agent_name')[0].string

            try:
                test = dealCycle[i]
                # 可能会出现有车位的情况，将会发生IndexError
                if len(temp[i].split()) == 3:
                    title = temp[i].split()[0]
                    room = temp[i].split()[1]
                    area = temp[i].split()[2]
                else:
                    title = temp[i].split()[0]+' '+temp[i].split()[1]
                    room = temp[i].split()[2]
                    area = temp[i].split()[3]

                item = SpiderLianjiaItem(title=title, room=room, area=area, dealDate=dealDate[i],
                                         totalPrice=totalPrice[i], unitPrice=unitPrice[i], dealCycle=dealCycle[i],
                                         guaPai=guaPai[i], seller=seller)
                yield item
            except IndexError as e:
                # 可能会出现有车位的情况，将会发生IndexError
                if len(temp[i].split()) == 3:
                    title = temp[i].split()[0]
                    room = temp[i].split()[1]
                    area = temp[i].split()[2]
                else:
                    title = temp[i].split()[0] + ' ' + temp[i].split()[1]
                    room = temp[i].split()[2]
                    area = temp[i].split()[3]

                item = SpiderLianjiaItem(title=title, room=room, area=area, dealDate=dealDate[i],
                                         totalPrice=totalPrice[i], unitPrice=unitPrice[i], dealCycle=guaPai[i],
                                         guaPai='挂牌0元', seller=seller)
                yield item
