from pyquery import PyQuery as pq
import pymongo
import re
import uuid
from selenium import webdriver
import time
import requests


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


def insert_db(item):
    client = pymongo.MongoClient(host='localhost', port=27017)
    db = client['db_lianjia']
    collection = db['chengjiao']
    collection.insert_many(item)


def dropduplicate_db():
    client = pymongo.MongoClient(host='localhost', port=27017)
    db = client['db_lianjia']
    collection = db['chengjiao']

    deleteData = collection.aggregate([{'$group': {'_id': {'dealDate': '$dealDate', 'title': '$title', 'room': '$room', 'area': '$area', 'totalPrice': '$totalPrice',
                                      'unitPrice': '$unitPrice'}, 'uniqueIds': {'$addToSet': "$_id"}, 'count': {'$sum': 1}}}, {'$match': {'count': {'$gt': 1}}}])
    first = True
    for d in deleteData:
        first = True
        for did in d['uniqueIds']:
            if first != True:  # 第一个不删除
                collection.delete_one({'_id': did})
            first = False
    print('查重完毕')

def spyder_chengjiao():
    session = denglu()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36'
        }
    list = []
    print('开始爬取')
    for i in range(1, 4):
        print(i)
        url = 'https://dl.lianjia.com/chengjiao/pg'
        req = session.get(url=url+str(i), headers=headers)
        doc = pq(req.text)
        p = doc('.listContent li')
        for x in p.items():
            p1 = x('.title a[target="_blank"]').text()
            p7 = p1.split(" ")
            p2 = x('.totalPrice .number').text()
            p3 = x('.unitPrice .number').text()
            p4 = x('.dealDate').text()
            p5 = x('.dealCycleTxt span:first-child').text()
            p6 = x('.dealCycleTxt span:last-child').text()

            # q=doc(q)
            # p8=q('.dealbread a:first-child').text()
            address = x('a').attr.href
            q = pq(session.get(url=address, headers=headers).text)
            q = q('.deal-bread a').text()
            p8 = re.sub('二手房成交', '', q.split(' ')[2])
            p9 = re.sub('二手房成交', '', q.split(' ')[3])
            try:
                if len(p7) == 3:
                    item = {'title': p7[0], 'room': p7[1], 'area': p7[2], 'totalPrice': int(
                        p2), 'unitPrice': p3, 'dealDate': p4, 'guaPai': p5, 'dealCycle': p6, 'seller': '0', 'district': p9, 'quyu': p8}
                    list.append(item)
                else:
                    item = {'title': p7[0], 'room': p7[2], 'area': p7[3], 'totalPrice': int(
                        p2), 'unitPrice': p3, 'dealDate': p4, 'guaPai': p5, 'dealCycle': p6, 'seller': '0', 'district': p9, 'quyu': p8}
                    list.append(item)
            except IndexError as e:
                if p7[1] != '车位':
                    item = {'title': p7[0], 'room': p7[1], 'area': '0', 'totalPrice': int(
                        p2), 'unitPrice': p3, 'dealDate': p4, 'guaPai': p5, 'dealCycle': p6, 'seller': '0', 'district': p9, 'quyu': p8}
                    # 这么写是防止如果找不到指，那item就会不变，不变的话，在list里就会有两个重复的值，存入数据库就会发生错误
                    list.append(item)

    print('爬取完毕')
    insert_db(list)

spyder_chengjiao()
print('开始查重')
dropduplicate_db()
