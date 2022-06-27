from wsgiref import headers
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
from urllib.parse import quote, urlencode
import pymongo
from pandas import DataFrame
import time
from selenium import webdriver

def denglu():
    driver = webdriver.Chrome(
        'C:\\Users\\Administrator\\Desktop\\spider\\chromedriver.exe')
    time.sleep(1)

    # 将打开的Chrome网页全屏
    driver.maximize_window()

    driver.get('https://dl.lianjia.com/')
    time.sleep(1)
    # driver.find_element_by_xpath('/html/body/div[20]/div[4]').click()  #关闭弹出框
    # time.sleep(1)
    driver.find_element_by_xpath(
        '/html/body/div[1]/div/div[3]/div/div/div[1]/span/a[1]/span').click()  # 选择登录
    time.sleep(1)
    driver.find_element_by_xpath(
        '//*[@id="loginModel"]/div[2]/div[2]/form/div[8]/a').click()  # 选择账号密码登录
    time.sleep(1)

    # 输入自己已经注册好的账号（最好是手机号哟）
    driver.find_element_by_xpath(
        '//*[@id="loginModel"]/div[2]/div[2]/form/ul/li[1]/input').send_keys('18642678245')
    time.sleep(0.5)
    # 输入密码
    driver.find_element_by_xpath(
        '//*[@id="loginModel"]/div[2]/div[2]/form/ul/li[3]/input').send_keys('hc2008011505')
    time.sleep(0.5)
    # 点击登录
    driver.find_element_by_xpath(
        '//*[@id="loginModel"]/div[2]/div[2]/form/div[7]').click()
    time.sleep(1)

    sel_cookies = driver.get_cookies()  # 获取selenium侧的cookies
    jar = requests.cookies.RequestsCookieJar()  # 先构建RequestsCookieJar对象
    for i in sel_cookies:
        # 将selenium侧获取的完整cookies的每一个cookie名称和值传入RequestsCookieJar对象
        # domain和path为可选参数，主要是当出现同名不同作用域的cookie时，为了防止后面同名的cookie将前者覆盖而添加的
        jar.set(i['name'], i['value'], domain=i['domain'], path=i['path'])

    session = requests.session()  # requests以session会话形式访问网站
    # 将配置好的RequestsCookieJar对象加入到requests形式的session会话中
    session.cookies.update(jar)
    driver.close()
    return session

def get_xiaoqu():
    # 将pymongo数据转换成Dataframe
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["db_lianjia"]
    mycol = mydb["chengjiao"]

    x = list(mycol.find())
    df = DataFrame(x)
    # 把没用的id删除掉
    df = df.drop('_id', axis=1)
    l = [x for x in df['title']]
    s = [x for x in set(l) if x is not None]
    s.sort()
    return s


class LianjiaSpider(CrawlSpider):
    # 获得小区列表
    mongodata = get_xiaoqu()
    name = 'chengjiao'
    allowed_domains = ['dl.lianjia.com']
    current_page = 1

    start_urls = ['https://dl.lianjia.com/chengjiao/pg%s' %

                  p for p in range(3, 4)]

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
        quyu = re.sub('二手房成交', '', response.xpath(
            "//div[@class='crumbs fl']/a[3]/text()").get())  # 获得区域
        district = re.sub('二手房成交', '', response.xpath(
            "//div[@class='crumbs fl']/a[4]/text()").get())  # 获得位置
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
                # 可能会出现有车位的情况，将会发生IndexError
                if len(temp[i].split()) == 3:
                    title = temp[i].split()[0]
                    room = temp[i].split()[1]
                    area = temp[i].split()[2]
                else:
                    title = temp[i].split()[0]+' '+temp[i].split()[1]
                    room = temp[i].split()[2]
                    area = temp[i].split()[3]

                if not title in self.mongodata:
                    item = SpiderLianjiaItem(title=title, room=room, area=area, dealDate=dealDate[i],
                                             totalPrice=totalPrice[i], unitPrice=unitPrice[i], dealCycle=dealCycle[i],
                                             guaPai=guaPai[i], seller=seller, quyu=quyu, district=district)
                    yield item
            except IndexError as e:
                # 可能会出现有车位的情况，将会发生IndexError，此时对车位不做处理
                pass


class get_chengjiao_one(object):
    def __init__(self):
        pass

    def get_seller(self, s, *page):
        base_url = 'https://dl.lianjia.com/chengjiao/display?'
        headers = {
            'Host': 'dl.lianjia.com',
            'Referer': 'https://dl.lianjia.com/chengjiao/'+str(s)+'.html',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest',
        }
        params = {
            'hid': str(s)
        }
        url = base_url + urlencode(params)
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                try:
                    seller = response.json().get('data').get('name')
                    return seller
                except AttributeError as e:
                    return '无'
        except requests.ConnectionError as e:
            print('Error', e.args)

    def get_totalpage(self, response):
        r = response.xpath(
            "//div[@class='page-box house-lst-page-box']/@page-data")
        total_page = re.search('\d+', str(r[0])).group()
        return total_page

    def get_chengjiao(self, s):
        session = denglu()
        html = session.get(
            'https://dl.lianjia.com/chengjiao/rs' + s)
        r = etree.HTML(html.text)
        total_page = self.get_totalpage(r)
        l = []
        for x in range(1, (int(total_page) + 1)):
            print(x)
            html = session.get(
                'https://dl.lianjia.com/chengjiao/pg' + str(x) + 'rs' + s)
            r = etree.HTML(html.text)
            list = r.xpath("//div[@class='title']/a/@href")
            for i in list:  # seller是ajax内容
                # num = re.search('\d+', i).group()
                # seller=self.get_seller(num)#seller影响速度，而且这个ajax应该是广告
                response = etree.Hsession.get(i).text
                title = response.xpath(
                    "//div[@class='wrapper']/text()")[0].split()[0]
                room = response.xpath(
                    "//div[@class='wrapper']/text()")[0].split()[1]
                area = response.xpath(
                    "//div[@class='wrapper']/text()")[0].split()[2]
                totalPrice = response.xpath(
                    "//span[@class='dealTotalPrice']/i/text()")[0]
                unitPrice = response.xpath("//div[@class='price']/b/text()")[0]
                type = response.xpath(
                    "//div[@class='base']/div[2]/ul/li[7]/text()")[0]
                guapaiPrice = response.xpath(
                    "//div[@class='msg']/span[1]/label/text()")[0]
                dealCycle = response.xpath(
                    "//div[@class='msg']/span[2]/label/text()")[0]
                dealDate = re.search(
                    '\d+\.\d+\.\d+', response.xpath("//div[@class='wrapper']/span/text()")[0]).group()
                builtDate = response.xpath(
                    "//div[@class='base']/div[2]/ul/li[8]/text()")[0]
                quyu = response.xpath(
                    "//div[@class='deal-bread']/a[3]/text()")[0]
                quyu = quyu[0:len(quyu) - 5]
                district = response.xpath(
                    "//div[@class='deal-bread']/a[4]/text()")[0]
                district = district[:len(district) - 5]
                l.append(
                    [title, totalPrice, unitPrice, room, type, area, quyu, district, builtDate, guapaiPrice, dealCycle,
                     dealDate])
        return l

