import sys
from unicodedata import name

sys.path.append(".")
import pickle
import os
from selenium import webdriver
import time
from pandas import DataFrame
import pymongo
from urllib.parse import urlencode
from lxml import etree
import requests
import re
from pyquery import PyQuery as pq
from bs4 import BeautifulSoup
from spider_lianjia.items import SpiderLianjiaItem
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy import Request


# 链家cookie的有效时间是1800秒,设置1700秒，有个余量
def check_cookie():
    bl = True
    now = time.time()
    changetime = os.stat(
        r"C:\Users\Administrator\Documents\GitHub\spider_lianjia\data\cookies"
    ).st_mtime

    if (now - changetime) < 1700:
        bl = True
    else:
        bl = False
    return bl


def save_jar(jar):
    with open("data\cookies", "wb") as f:
        pickle.dump(jar, f)


def load_jar():
    with open("data\cookies", "rb") as f:
        jar = pickle.load(f)
    return jar


def denglu():
    bl = check_cookie()
    if bl == True:
        jar = requests.cookies.RequestsCookieJar()  # 先构建RequestsCookieJar对象
        session = requests.session()  # 构建session对象

        jar = load_jar()
        session.cookies.update(jar)
        return session
    else:
        time.sleep(1)
        driver = webdriver.Chrome(
            "C:\\Users\\Administrator\\Documents\\GitHub\\spider_lianjia\\chromedriver98.exe"
        )

        # 将打开的Chrome网页全屏
        driver.maximize_window()

        driver.get("https://dl.lianjia.com/")
        time.sleep(1)
        # driver.find_element_by_xpath('/html/body/div[20]/div[4]').click()  #关闭弹出框
        # time.sleep(1)
        driver.find_element_by_xpath(
            "/html/body/div[1]/div/div[3]/div/div/div[1]/span/a[1]/span"
        ).click()  # 选择登录
        time.sleep(1)
        driver.find_element_by_xpath(
            # '//div[@id="loginModel"]/div[2]/div[2]/form/div[8]/a'
            "//a[@class='change_login_type _color']"
        ).click()  # 选择账号密码登录
        time.sleep(1)

        # 输入自己已经注册好的账号（最好是手机号哟）
        driver.find_element_by_xpath(
            # '//*[@id="loginModel"]/div[2]/div[2]/form/ul/li[1]/input'
            "//input[@class='phonenum_input']"
        ).send_keys("18642678245")
        time.sleep(0.5)
        # 输入密码
        driver.find_element_by_xpath(
            # '//*[@id="loginModel"]/div[2]/div[2]/form/ul/li[3]/input'
            "//input[@class='password_type password_input']"
        ).send_keys("hc2008011505")
        time.sleep(0.5)
        # 点击登录
        driver.find_element_by_xpath(
            # '//*[@id="loginModel"]/div[2]/div[2]/form/div[7]'
            "//div[@class='btn confirm_btn login_panel_op login_submit _bgcolor']"
        ).click()
        time.sleep(60)

        sel_cookies = driver.get_cookies()  # 获取selenium侧的cookies
        jar = requests.cookies.RequestsCookieJar()  # 先构建RequestsCookieJar对象
        for i in sel_cookies:
            # 将selenium侧获取的完整cookies的每一个cookie名称和值传入RequestsCookieJar对象
            # domain和path为可选参数，主要是当出现同名不同作用域的cookie时，为了防止后面同名的cookie将前者覆盖而添加的
            jar.set(i["name"], i["value"], domain=i["domain"], path=i["path"])

        session = requests.session()  # requests以session会话形式访问网站
        # 将配置好的RequestsCookieJar对象加入到requests形式的session会话中
        session.cookies.update(jar)
        save_jar(jar)
        driver.close()

        return session


def insert_db(item):
    client = pymongo.MongoClient(host="localhost", port=27017)
    db = client["db_lianjia"]
    collection = db["chengjiao"]
    collection.insert_many(item)


def dropduplicate_db():
    client = pymongo.MongoClient(host="localhost", port=27017)
    db = client["db_lianjia"]
    collection = db["chengjiao"]

    deleteData = collection.aggregate(
        [
            {
                "$group": {
                    "_id": {
                        "dealDate": "$dealDate",
                        "title": "$title",
                        "room": "$room",
                        "area": "$area",
                        "totalPrice": "$totalPrice",
                        "unitPrice": "$unitPrice",
                    },
                    "uniqueIds": {"$addToSet": "$_id"},
                    "count": {"$sum": 1},
                }
            },
            {"$match": {"count": {"$gt": 1}}},
        ]
    )
    first = True
    for d in deleteData:
        first = True
        for did in d["uniqueIds"]:
            if first != True:  # 第一个不删除
                collection.delete_one({"_id": did})
            first = False
    print("查重完毕")


def spyder_chengjiao(m, n):
    session = denglu()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36"
    }
    list = []
    print("开始爬取...")
    for i in range(m, n):
        print(i)
        url = "https://dl.lianjia.com/chengjiao/pg"
        req = session.get(url=url + str(i), headers=headers)
        doc = pq(req.text)
        p = doc(".listContent li")
        for x in p.items():
            p1 = x('.title a[target="_blank"]').text()
            p7 = p1.split(" ")
            p2 = x(".totalPrice .number").text()
            p3 = x(".unitPrice .number").text()
            p4 = x(".dealDate").text()
            p5 = x(".dealCycleTxt span:first-child").text()
            p6 = x(".dealCycleTxt span:last-child").text()

            # q=doc(q)
            # p8=q('.dealbread a:first-child').text()
            address = x("a").attr.href
            q = pq(session.get(url=address, headers=headers).text)
            q = q(".deal-bread a").text()
            p8 = re.sub("二手房成交", "", q.split(" ")[2])
            p9 = re.sub("二手房成交", "", q.split(" ")[3])
            try:
                if len(p7) == 3:
                    item = {
                        "title": p7[0],
                        "room": p7[1],
                        "area": p7[2],
                        "totalPrice": p2,
                        "unitPrice": p3,
                        "dealDate": p4,
                        "guaPai": p5,
                        "dealCycle": p6,
                        "seller": "0",
                        "district": p9,
                        "quyu": p8,
                    }
                    list.append(item)
                else:
                    item = {
                        "title": p7[0],
                        "room": p7[2],
                        "area": p7[3],
                        "totalPrice": p2,
                        "unitPrice": p3,
                        "dealDate": p4,
                        "guaPai": p5,
                        "dealCycle": p6,
                        "seller": "0",
                        "district": p9,
                        "quyu": p8,
                    }
                    list.append(item)
            except IndexError as e:
                if p7[1] != "车位":
                    item = {
                        "title": p7[0],
                        "room": p7[1],
                        "area": "0",
                        "totalPrice": p2,
                        "unitPrice": p3,
                        "dealDate": p4,
                        "guaPai": p5,
                        "dealCycle": p6,
                        "seller": "0",
                        "district": p9,
                        "quyu": p8,
                    }
                    # 这么写是防止如果找不到指，那item就会不变，不变的话，在list里就会有两个重复的值，存入数据库就会发生错误
                    list.append(item)

    print("爬取完毕...")
    insert_db(list)


def get_xiaoqu():
    # 将pymongo数据转换成Dataframe
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["db_lianjia"]
    mycol = mydb["chengjiao"]

    x = list(mycol.find())
    df = DataFrame(x)
    # 把没用的id删除掉
    df = df.drop("_id", axis=1)
    l = [x for x in df["title"]]
    s = [x for x in set(l) if x is not None]
    s.sort()
    return s


class LianjiaSpider(CrawlSpider):
    # 获得小区列表
    mongodata = get_xiaoqu()
    name = "chengjiao"
    allowed_domains = ["dl.lianjia.com"]
    current_page = 1

    start_urls = ["https://dl.lianjia.com/chengjiao/pg%s" % p for p in range(3, 4)]

    rules = (
        Rule(LinkExtractor(allow="./chengjiao/.+\.html")),  # allow里面是正则表达式
        Rule(LinkExtractor(allow="/chengjiao/c\d+"), callback="parse_chengjiao"),
    )

    def parse_chengjiao(self, response):
        if self.current_page == 1:
            # 获得totalpage
            total_page = response.xpath(
                "//div[@class='page-box house-lst-page-box']//@page-data"
            ).re("\d+")
            total_page = int(total_page[0])
            lianjie = response.xpath("//h1/a/@href").get()

            for i in range(1, total_page + 1):
                url = (
                    "https://dl.lianjia.com"
                    + re.search(".+chengjiao/", lianjie).group()
                    + "pg"
                    + str(i)
                    + re.search("c\d+", lianjie).group()
                )
                yield Request(url, callback=self.parse_lianjia)

    def parse_lianjia(self, response):
        quyu = re.sub(
            "二手房成交", "", response.xpath("//div[@class='crumbs fl']/a[3]/text()").get()
        )  # 获得区域
        district = re.sub(
            "二手房成交", "", response.xpath("//div[@class='crumbs fl']/a[4]/text()").get()
        )  # 获得位置
        temp = response.xpath('//div[@class="title"]/a/text()').getall()
        dealDate = response.xpath('//div[@class="dealDate"]/text()').getall()
        totalPrice = response.xpath('//div[@class="totalPrice"]/span/text()').getall()
        unitPrice = response.xpath('//div[@class="unitPrice"]/span/text()').getall()
        dealCycle = response.xpath(
            "//span[@class='dealCycleTxt']/span[2]/text()"
        ).getall()
        guaPai = response.xpath(
            "//span[@class='dealCycleTxt']/span[1]/text()"
        ).getall()  # 挂牌价有的有有的没有

        soup = BeautifulSoup(response.text, "lxml")
        s = soup.select(".info ")

        for i in range(0, len(dealDate)):
            if s[i].select(".agent_name") == []:
                seller = "无"
            else:
                seller = s[i].select(".agent_name")[0].string

            try:
                # 可能会出现有车位的情况，将会发生IndexError
                if len(temp[i].split()) == 3:
                    title = temp[i].split()[0]
                    room = temp[i].split()[1]
                    area = temp[i].split()[2]
                else:
                    title = temp[i].split()[0] + " " + temp[i].split()[1]
                    room = temp[i].split()[2]
                    area = temp[i].split()[3]

                if not title in self.mongodata:
                    item = SpiderLianjiaItem(
                        title=title,
                        room=room,
                        area=area,
                        dealDate=dealDate[i],
                        totalPrice=totalPrice[i],
                        unitPrice=unitPrice[i],
                        dealCycle=dealCycle[i],
                        guaPai=guaPai[i],
                        seller=seller,
                        quyu=quyu,
                        district=district,
                    )
                    yield item
            except IndexError as e:
                # 可能会出现有车位的情况，将会发生IndexError，此时对车位不做处理
                pass


class get_chengjiao_one(object):
    def __init__(self):
        pass

    def get_seller(self, s, *page):
        base_url = "https://dl.lianjia.com/chengjiao/display?"
        headers = {
            "Host": "dl.lianjia.com",
            "Referer": "https://dl.lianjia.com/chengjiao/" + str(s) + ".html",
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
        }
        params = {"hid": str(s)}
        url = base_url + urlencode(params)
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                try:
                    seller = response.json().get("data").get("name")
                    return seller
                except AttributeError as e:
                    return "无"
        except requests.ConnectionError as e:
            print("Error", e.args)

    def get_totalpage(self, response):
        r = response.xpath("//div[@class='page-box house-lst-page-box']/@page-data")
        total_page = re.search("\d+", str(r[0])).group()
        return total_page

    def get_chengjiao(self, s):
        session = denglu()
        html = session.get("https://dl.lianjia.com/chengjiao/rs" + s)
        r = etree.HTML(html.text)
        total_page = self.get_totalpage(r)
        l = []
        for x in range(1, (int(total_page) + 1)):
            print(x)
            html = session.get(
                "https://dl.lianjia.com/chengjiao/pg" + str(x) + "rs" + s
            )
            r = etree.HTML(html.text)
            list = r.xpath("//div[@class='title']/a/@href")
            for i in list:  # seller是ajax内容
                # num = re.search('\d+', i).group()
                # seller=self.get_seller(num)#seller影响速度，而且这个ajax应该是广告
                response = etree.HTML(session.get(i).text)
                title = response.xpath("//div[@class='wrapper']/text()")[0].split()[0]
                room = response.xpath("//div[@class='wrapper']/text()")[0].split()[1]
                area = response.xpath("//div[@class='wrapper']/text()")[0].split()[2]
                totalPrice = response.xpath("//span[@class='dealTotalPrice']/i/text()")[
                    0
                ]
                unitPrice = response.xpath("//div[@class='price']/b/text()")[0]
                type = response.xpath("//div[@class='base']/div[2]/ul/li[7]/text()")[0]
                guapaiPrice = response.xpath(
                    "//div[@class='msg']/span[1]/label/text()"
                )[0]
                dealCycle = response.xpath("//div[@class='msg']/span[2]/label/text()")[
                    0
                ]
                dealDate = re.search(
                    "\d+\.\d+\.\d+",
                    response.xpath("//div[@class='wrapper']/span/text()")[0],
                ).group()
                builtDate = response.xpath(
                    "//div[@class='base']/div[2]/ul/li[8]/text()"
                )[0]
                quyu = response.xpath("//div[@class='deal-bread']/a[3]/text()")[0]
                quyu = quyu[0 : len(quyu) - 5]
                district = response.xpath("//div[@class='deal-bread']/a[4]/text()")[0]
                district = district[: len(district) - 5]
                l.append(
                    [
                        title,
                        totalPrice,
                        unitPrice,
                        room,
                        type,
                        area,
                        quyu,
                        district,
                        builtDate,
                        guapaiPrice,
                        dealCycle,
                        dealDate,
                    ]
                )
        return l
