# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import json
from scrapy.exporters import JsonLinesItemExporter
from spider_lianjia.items import SpiderLianjiaItem
from spider_lianjia.items import SpiderZaishouItem
import pymongo

class SpiderLianjiaPipeline:
    def __init__(self):
        self.client = pymongo.MongoClient(host='localhost',port=27017)
        self.db=self.client['db_lianjia']
        self.collection=self.db['chengjiao']

    def process_item(self, item, spider):
        self.collection.save(dict(item))
        return item

    def close_spider(self,spider):
        self.client.close()

class SpiderZaishouPipeline:
    def __init__(self):
        self.client = pymongo.MongoClient(host='localhost',port=27017)
        self.db=self.client['db_lianjia']
        self.collection=self.db['zaishou']

    def process_item(self, item, spider):
        self.collection.save(dict(item))
        return item

    def close_spider(self,spider):
        self.client.close()