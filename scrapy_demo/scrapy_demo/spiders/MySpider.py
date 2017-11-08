#!/usr/bin/python
# -*-coding:utf8-*-
__author__ = 'John'

import scrapy
import os
import logging

#评分块xpath
score_container_xpath = "//*[@id=\"comments\"]/div/div[@class=\"comment\"]"
#评分xpath, path相对于评分块
score_xpath = "h3/span[2]/span[2]/@title"
#评论xpath, path相对于评分块
discuss_xpath = "p/text()"
count = 0

class MySpider(scrapy.Spider):
    name = "MySpider"
    start_urls = [
        "https://movie.douban.com/subject/26363254/comments?start=0&limit=20&sort=new_score&status=P&percent_type="
    ]
    def parse(self, response):
        global count
        with open("D:/zhanlang_scrapy.txt", "a") as f:
            for page in response.xpath(score_container_xpath):
                if page is not None:
                    score = page.xpath(score_xpath)[0].extract()
                    discuss = page.xpath(discuss_xpath)[0].extract()
                    f.write("%s\t%s\n" % (score.encode("utf-8"), discuss.encode("utf-8")))
                    count = count + 1
        self.log("当前已爬取%d条评论" % count, logging.INFO)
        # next_page = response.xpath("//div[@id=\"paginator\"]/a[@class=\"next\"]/@href").extract_first()
        next_page = response.css("div#paginator a.next::attr(href)").extract_first()
        self.log("next_page: %s" % next_page, logging.INFO)
        if next_page is not None:
            yield response.follow(next_page, self.parse)


