# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class SpiderLianjiaItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    title = scrapy.Field()
    dealDate=scrapy.Field()
    totalPrice=scrapy.Field()
    unitPrice=scrapy.Field()
    room=scrapy.Field()
    area=scrapy.Field()
    dealCycle=scrapy.Field()
    guaPai=scrapy.Field()
    seller=scrapy.Field()