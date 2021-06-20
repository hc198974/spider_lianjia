import pymongo
import json
from pandas import DataFrame

# list = []
# for line in open('data/zaishou.json', 'r', encoding='utf-8'):
#     list.append(json.loads(line))
#
# client = pymongo.MongoClient(host='localhost',port=27017)
# db=client.db_lianjia
# collection=db.zaishou
# for i in range(0,len(list)):
#     collection.insert_one(list[i])

def get_xiaoqu():
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


