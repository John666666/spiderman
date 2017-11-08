#!/usr/bin/python
# -*-coding:utf8-*-
__author__ = 'John'

import requests
import sys
from lxml import etree
session = requests.session();
index_url = "https://movie.douban.com/subject/26363254/comments?start=%d&limit=%d&sort=new_score&status=P&percent_type="
page_size = 20

#评分块xpath
score_container_xpath = "//*[@id=\"comments\"]/div/div[@class=\"comment\"]"
#评分xpath, path相对于评分块
score_xpath = "h3/span[2]/span[2]/@title"
#评论xpath, path相对于评分块
discuss_xpath = "p/text()"

collect = {}
count = 0

print sys.maxint
with open("d:/zhanlang_discuss.txt", "w") as file:
    for idx in range(0, sys.maxint, page_size):
        resp = session.get(index_url % (idx, page_size))
        score_container_list = etree.HTML(resp.content).xpath(score_container_xpath)
        if not score_container_list:
            print score_container_list
            break
        for score_container in score_container_list:
            score_level = score_container.xpath(score_xpath)[0]
            discuss = score_container.xpath(discuss_xpath)[0]
            if len(score_level) > 2:
                score_level = u"未知"
            file.write("%s\t%s\n" % (score_level.encode("utf-8"), discuss.encode("utf-8")))
            if collect.has_key(score_level):
                collect[score_level] = collect.get(score_level, 1) + 1
            else:
                collect[score_level] = 1
        count += page_size
        print u"爬取进度%d条." % count

print u"爬取完毕, 共计爬取%d条评论!" % count

#排序
result = sorted(collect.items(), lambda a, b: cmp(a[1], b[1]), reverse=True)

print u"评论统计如下:"
for item in result:
    print u"%s: %d条" % (item[0], item[1])