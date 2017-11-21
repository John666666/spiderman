# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
import json


class ScrapyDemoItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

class ChengyuItem(scrapy.Item):
    content = scrapy.Field()
    provenance = scrapy.Field()
    paraphrase = scrapy.Field()
    story = scrapy.Field()
    char_length = scrapy.Field()
    first_pinyin = scrapy.Field()
    first_pinyin_tone = scrapy.Field()
    last_pinyin = scrapy.Field()
    last_pinyin_tone = scrapy.Field()
    level = scrapy.Field()
    createtime = scrapy.Field()

    def to_string(self):
        map = {}
        for key in self.keys():
            map[key] = self.get(key)
        return json.dumps(map, ensure_ascii=False)

