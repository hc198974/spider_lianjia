import pymongo
import json
from pandas import DataFrame

list = []
for line in open('C:\\Users\\Administrator\\Desktop\\temp\\lianjia.json', 'r', encoding='utf-8'):
    j=json.loads(line)
    del j['_id']
    list.append(j)

client = pymongo.MongoClient(host='localhost',port=27017)
db=client.db_lianjia
collection=db.chengjiao
collection.insert_many(list)

# def get_xiaoqu():
#     #将pymongo数据转换成Dataframe
#     myclient = pymongo.MongoClient("mongodb://localhost:27017/")
#     mydb = myclient["db_lianjia"]
#     mycol = mydb["chengjiao"]

#     x = list(mycol.find())
#     df=DataFrame(x)
#     #把没用的id删除掉
#     df=df.drop('_id',axis=1)
#     l = [x for x in df['title']]
#     s = [x for x in set(l) if x is not None]
#     s.sort()
#     return s


