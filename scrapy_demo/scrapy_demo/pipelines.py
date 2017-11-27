# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import re
import time

from items import ChengyuItem


class ScrapyDemoPipeline(object):
    def process_item(self, item, spider):
        return item

class ChengyuItemPipeline(object):
    '''
        Store ChengyuItem into mysql database.
    '''
    def __init__(self, **kwargs):
        import MySQLdb
        self.conn = MySQLdb.connect(**kwargs)
        self.cursor = self.conn.cursor()

    @classmethod
    def from_crawler(cls, crawler):
        '''
            If present, this classmethod is called to create a pipeline instance from a Crawler.
            It must return a new instance of the pipeline. Crawler object provides access to all Scrapy core components like
            settings and signals; it is a way for pipeline to access them and hook its functionality into Scrapy.
        :param crawler:
        :return:
        '''
        return cls(
            host=crawler.settings.get("MYSQL_HOST"),
            port=crawler.settings.get("MYSQL_PORT"),
            user=crawler.settings.get("MYSQL_USER"),
            passwd=crawler.settings.get("MYSQL_PASSWORD"),
            db=crawler.settings.get("MYSQL_DB"),
            use_unicode=crawler.settings.get("MYSQL_USE_UNICODE"),
            charset=crawler.settings.get("MYSQL_CHARSET"),
            autocommit=crawler.settings.get("MYSQL_AUTOCOMMIT")
        )

    def process_item(self, item, spider):
        '''
            This method is called for every item pipeline component.
            process_item() must either: return a dict with data, return an Item (or any descendant class) object,
            return a Twisted Deferred or raise DropItem exception.
            Dropped items are no longer processed by further pipeline components
        :param item:
        :param spider:
        :return:
        '''
        if isinstance(item, ChengyuItem):
            self.formatField(item)
            try:
                """
                    注意：这里不要用insert into aa(x,y,z) values (%s, %s, %s) % (x_v, y_v, z_v)这种方式， 因为一旦value中有引号会报错，
                         也不要在%s两边加引号， 会把值强制成字符串，比如None值
                """
                self.cursor.execute("""insert into chengyu (content, provenance, paraphrase, story, char_length,
                                    first_pinyin, first_pinyin_tone, last_pinyin, last_pinyin_tone, level, createtime)
                                     values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", [item.get("content"), item.get("provenance"),
                                    item.get("paraphrase"), item.get("story"), item.get("char_length"), item.get("first_pinyin"),
                                    item.get("first_pinyin_tone"), item.get("last_pinyin"), item.get("last_pinyin_tone"),
                                    item.get("level"), item.get("createtime")])
                self.conn.commit()
            except Exception, e:
                spider.logger.error("store data into mysql error: %s" % (e.args))
                self.conn.rollback()
        return item

    def formatField(self, item):
        provenance = item.get("provenance")
        if provenance is not None:
            reg = re.compile(r"[\w\.\s]+")
            provenance = re.sub(reg, "", provenance)
            item["provenance"] = provenance
        return item

    # This method is called when the spider is opened.
    def open_spider(self, spider):
        # 记录开始时间
        self.begin = time.time() * 1000

    # This method is called when the spider is closed.
    def close_spider(self, spider):
        #记录结束时间
        self.end = time.time() * 1000

        #关闭数据库连接
        self.conn.close()

        #获取统计信息
        report_list, count = spider.reportCollect()

        #输出统计日志
        spider.logger.info(u"爬取结束, 共计爬取成语%d条, 耗时: %d 秒." % (count, self.end - self.begin))
        for report in report_list:
            spider.logger.info(u"%s: %d个" % (report[0], report[1]))

