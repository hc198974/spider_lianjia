# -*- coding: utf-8 -*-
import requests
from lxml import etree
import re
import pymongo
from pandas import DataFrame

class xiaoqu():
    def __init__(self):
        pass
    
    def get_weizhi(self,xiaoqu):
        h1=requests.get('https://dl.lianjia.com/xiaoqu/rs'+xiaoqu)
        r1=etree.HTML(h1.text)
        url=r1.xpath("//div[@class='title']/a/@href")[0]
        
        h2=requests.get(url)
        r2=etree.HTML(h2.text)
        quyu=r2.xpath(("//div[@class='fl l-txt']/a[3]/text()"))[0]
        district=r2.xpath("//div[@class='fl l-txt']/a[4]/text()")[0]
        quyu=re.sub('小区', '', quyu)
        district=re.sub('小区','',district)        
        return quyu,district
    
    def get_list(self):
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
        return s



# x=xiaoqu()
# list=x.get_list()
# for i in list[0:10]:
#     print(x.get_weizhi(i))
    