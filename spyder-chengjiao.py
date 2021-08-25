from pyquery import PyQuery as pq
import pymongo
import re
import uuid


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


def spyder_chengjiao():
    print('开始爬取')
    list = []
    for i in range(5,6):
        print(i)
        doc = pq('https://dl.lianjia.com/chengjiao/pg'+str(i))
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
            q = pq(x('a').attr.href)
            q = q('.deal-bread a').text()
            p8 = re.sub('二手房成交', '', q.split(' ')[2])
            p9 = re.sub('二手房成交', '', q.split(' ')[3])
            try:
                if len(p7) == 3:
                    item = { 'title': p7[0], 'room': p7[1], 'area': p7[2], 'totalPrice': int(
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
                    list.append(item)#这么写是防止如果找不到指，那item就会不变，不变的话，在list里就会有两个重复的值，存入数据库就会发生错误

    print('爬取完毕')
    insert_db(list)

spyder_chengjiao()
print('开始查重')
dropduplicate_db()
