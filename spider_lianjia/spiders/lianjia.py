import scrapy
from scrapy import Request
from scrapy import linkextractors
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from spider_lianjia.items import SpiderLianjiaItem
import re


class LianjiaSpider(CrawlSpider):
    name = 'lianjia'
    allowed_domains = ['dl.lianjia.com']
    current_page = 1
    start_urls = ['https://dl.lianjia.com/chengjiao/pg%s' %
                  p for p in range(1, 10)]

    rules = (
        Rule(LinkExtractor(allow='./chengjiao/.+\.html')),  # allow里面是正则表达式
        Rule(LinkExtractor(allow='/chengjiao/c\d+'), callback='parse_chengjiao')
    )

    def parse_chengjiao(self, response):
        if self.current_page == 1:
            # 获得totalpage
            total_page = response.xpath("//div[@class='page-box house-lst-page-box']//@page-data").re("\d+")
            total_page = int(total_page[0])
            lianjie = response.xpath('//h1/a/@href').get()

            
            for i in range(1, total_page+1):
                url= 'https://dl.lianjia.com'+re.search('.+chengjiao/', lianjie).group()+'pg'+str(i)+re.search('c\d+', lianjie).group()
                yield Request(url, callback=self.parse_lianjia)

    def parse_lianjia(self, response):
        temp= response.xpath('//div[@class="title"]/a/text()').getall()
        dealDate=response.xpath('//div[@class="dealDate"]/text()').getall()
        totalPrice=response.xpath('//div[@class="totalPrice"]/span/text()').getall()
        unitPrice=response.xpath('//div[@class="unitPrice"]/span/text()').getall()


        for i in range(0,len(dealDate)):
            if len(temp[i].split())==3:
                title=temp[i].split()[0]
                room=temp[i].split()[1]
                area=temp[i].split()[2]
            else:
                title=temp[i].split()[0]+' '+temp[i].split()[1]
                room=temp[i].split()[1]
                area=temp[i].split()[2]

            item = SpiderLianjiaItem(title=title,room=room,area=area,dealDate=dealDate[i],totalPrice=totalPrice[i],unitPrice=unitPrice[i])
            yield item