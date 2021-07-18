# -*- coding: utf-8 -*-
import requests
from lxml import etree
import re
import pymongo
from pandas import DataFrame

class update_db():    
    def get_list(self):
        #将pymongo数据转换成Dataframe
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client["db_lianjia"]
        col = db["xiaoqu"]

        x = list(col.find())
        df=DataFrame(x)
        #把没用的id删除掉
        df=df.drop('_id',axis=1)
        l = [x for x in df['title']]
        s = [x for x in set(l) if x is not None]
        
        for i in s:
            quyu=df[df['title']==i]['quyu'].values[0]
            district=df[df['title']==i]['district'].values[0]
            db.chengjiao.update_many({'title':i},{'$set':{'quyu':quyu,'district':district}})
    

u=update_db()
u.get_list()
print('执行完毕')
    