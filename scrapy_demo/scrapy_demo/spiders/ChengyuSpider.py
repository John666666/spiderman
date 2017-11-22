#!/usr/bin/python
# -*-coding:utf8-*-
__author__ = 'John'

import logging
import re
import time

import scrapy
from scrapy.http import Request
import pinyin
import sys

from ..items import ChengyuItem
from scrapy.utils.log import configure_logging

count = 0
result = {}

class ChengyuSpider(scrapy.Spider):

    name = "chengyu"

    custom_settings = {
        "ITEM_PIPELINES": {
            'scrapy_demo.pipelines.ChengyuItemPipeline': 400
        },
        "FEED_FORMAT": "csv"
    }

    logger = logging.getLogger(name)

    def __init__(self):
        self.init_log()

    def init_log(self):

        ##改变scrapy默认log选项
        configure_logging(settings={
            "LOG_LEVEL": "ERROR"
        })

        ## 设置自定义logger
        my_format = logging.Formatter("%(asctime)-15s %(levelname)s %(filename)s %(lineno)s %(process)s %(message)s")
        fileHandler = logging.FileHandler("%s_spider.log" % self.name, encoding="utf-8")
        fileHandler.setLevel(logging.INFO)
        fileHandler.setFormatter(my_format)
        self.logger.addHandler(fileHandler)

        console = logging.StreamHandler(sys.stdout)
        console.setLevel(logging.INFO)
        console.setFormatter(my_format)
        self.logger.addHandler(console)

    def start_requests(self):
        url_pattern = "https://chengyu.911cha.com/zishu_%d.html"
        for i in range(3, 13):
            url = url_pattern % i
            self.logger.info("start url: "+url)
            yield Request(url)

    # 解析成语列表
    def parse(self, response):
        chengyu_list = response.css("div.mcon ul li a")
        if chengyu_list is not None:
            for chengyu in chengyu_list:
                yield response.follow(chengyu.xpath("@href").extract_first(), callback=self.parse_detail)
        last_a = response.css("div.gclear.pp.bt.center.f14 a:last-child")
        text = last_a.css("::text").extract_first()
        if text is not None:
            if text.encode("utf-8") == '下一页':
                yield response.follow(last_a.xpath("@href").extract_first(), callback=self.parse)
            else:
                self.log.debug("Already last page")

    # 解析详情页
    def parse_detail(self, response):
        chengyuItem = ChengyuItem()
        #成语内容
        chengyuItem['content'] = response.css("div.panel div.mcon h2::text").extract_first()
        self.logger.info("%s" % chengyuItem.get("content"))
        p_list = response.css("div.panel div.mcon.bt.noi.f14 p")
        if p_list is not None:
            for p_ele in p_list:
                category = p_ele.css("span.green::text").extract_first()
                if category is None:
                    continue
                if len(category) == 1:
                        category = p_ele.css("span.green a::text").extract_first().encode("utf-8")
                else:
                    category = category.encode("utf-8")

                if category.find("成语解释：") > -1:
                    # 成语释义
                    chengyuItem['paraphrase'] = "".join(p_ele.css("::text").extract()[1:])
                elif category.find("成语出处：") > -1:
                    #成语出处
                    chengyuItem['provenance'] = "".join(p_ele.css("::text").extract()[1:])
                elif category.find("常用程度：") > -1:
                    # 难度: 常用(1) 一般(10)
                    # 常用成语/一般成语/
                    level_text = p_ele.css("::text").extract_first()
                    if level_text.encode("utf-8").find("常用") > -1:
                        chengyuItem['level'] = 1
                    else:
                        chengyuItem['level'] = 10
                elif category.find("成语故事") > -1:
                    #成语故事
                    chengyuItem['story'] = "".join(p_ele.css("::text").extract()[2:]).lstrip().rstrip()
                else:
                    continue
        if chengyuItem['content'] is not None:
            ## 补充扩展属性
            reg = re.compile("[，。,.]")
            format_content = re.sub(reg, "", chengyuItem['content'])

            #成语字数
            chengyuItem['char_length'] = len(format_content)

            #首字拼音
            first_pinyin = pinyin.get(format_content[0], format="numerical")
            chengyuItem['first_pinyin'] = first_pinyin[:-1]
            chengyuItem['first_pinyin_tone'] = first_pinyin[-1]

            #末字拼音
            last_pinyin = pinyin.get(format_content[-1], format="numerical")
            chengyuItem['last_pinyin'] = last_pinyin[:-1]
            chengyuItem['last_pinyin_tone'] = last_pinyin[-1]

            #当前时间
            chengyuItem['createtime'] = time.strftime('%Y-%m-%d %H:%M:%S')

            # 统计爬取信息
            self.collectItem(chengyuItem)

            yield chengyuItem


    def collectItem(self, item):
        '''
            统计爬取到的成语
        :param item: 成语Item
        :return:
        '''
        # self.logger.info("item: %s" % item.to_string())
        char_length = item.get("char_length")
        global count
        global result
        count += 1
        key = u"%s字成语" % char_length
        if result.has_key(key):
            result[key] = result.get(key) + 1
        else:
            result[key] = 1

    def reportCollect(self):
        report_list = sorted(result.items(), cmp=lambda x, y: {cmp(x[1], y[1])}, reverse=True)
        return report_list, count


