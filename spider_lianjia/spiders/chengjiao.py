import scrapy
from scrapy import Request
from scrapy import linkextractors
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from spider_lianjia.items import SpiderLianjiaItem
from bs4 import BeautifulSoup
import re
import requests
from lxml import etree
from urllib.parse import quote,urlencode


class LianjiaSpider(CrawlSpider):
    name = 'chengjiao'
    allowed_domains = ['dl.lianjia.com']
    current_page = 1
    start_urls = ['https://dl.lianjia.com/chengjiao/pg%s' %
                  p for p in range(40, 50)]

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

class get_chengjiao_one(object):
    def get_seller(self,s,*page):
        base_url = 'https://dl.lianjia.com/chengjiao/display?'
        headers = {
            'Host': 'dl.lianjia.com',
            'Referer': 'https://dl.lianjia.com/chengjiao/'+str(s)+'.html',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest',
        }
        params = {
            'hid':str(s)
        }
        url = base_url + urlencode(params)
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                try:
                    seller =response.json().get('data').get('name')
                    return seller
                except AttributeError as e:
                    return '无'
        except requests.ConnectionError as e:
            print('Error', e.args)

    def get_chengjiao(self,s):
        html = requests.get('https://dl.lianjia.com/chengjiao/rs'+quote(s))
        r = etree.HTML(html.text)
        list = r.xpath("//div[@class='title']/a[@target='_blank' and not (@class)]/@href")
        l=[]
        for i in list:#seller是ajax内容
            num=re.search('\d+',i).group()
            # seller=self.get_seller(num)#seller影响速度，而且这个ajax应该是广告
            response = etree.HTML(requests.get(i).text)
            title = response.xpath("//div[@class='wrapper']/text()")[0].split()[0]
            room = response.xpath("//div[@class='wrapper']/text()")[0].split()[1]
            area = response.xpath("//div[@class='wrapper']/text()")[0].split()[2]
            totalPrice = response.xpath("//span[@class='dealTotalPrice']/i/text()")[0]
            unitPrice = response.xpath("//div[@class='price']/b/text()")[0]
            type = response.xpath("//div[@class='base']/div[2]/ul/li[7]/text()")[0]
            guapaiPrice = response.xpath("//div[@class='msg']/span[1]/label/text()")[0]
            dealCycle = response.xpath("//div[@class='msg']/span[2]/label/text()")[0]
            dealDate = re.search('\d+\.\d+\.\d+', response.xpath("//div[@class='wrapper']/span/text()")[0]).group()
            builtDate = response.xpath("//div[@class='base']/div[2]/ul/li[8]/text()")[0]
            quyu = response.xpath("//div[@class='deal-bread']/a[3]/text()")[0]
            quyu = quyu[0:len(quyu) - 5]
            district = response.xpath("//div[@class='deal-bread']/a[4]/text()")[0]
            district = district[:len(district) - 5]
            l.append([title,totalPrice,unitPrice,room,type,area,quyu,district,builtDate,guapaiPrice,dealCycle,dealDate])
        return l