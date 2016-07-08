#!/usr/bin/python
# -*-coding:utf8-*-
__author__ = 'John'

import xlrd
import requests
import urllib
import re

execl_path = u'C:/Users/John/Desktop/图片样本分类.xlsx'
category_file = u'C:/Users/John/Desktop/category_conf.txt'
new_category_file = u'C:/Users/John/Desktop/category_conf_new.txt'

category_list = []
def loadCategory():
    del category_list[:]
    for line in open(category_file):
        if not line or len(line.strip()) < 1:
            continue
        if line.startswith('#'):
            continue
        category_list.append(line.strip().decode('utf-8'))


baike_url = 'http://baike.baidu.com/search/word?word=%s'
regex = re.compile(u'\u767e\u5ea6\u767e\u79d1\u5c1a\u672a\u6536\u5f55\u8bcd\u6761')

def isExsitsInBaike(text):
    if isinstance(text, unicode):
        text = text.encode('utf-8')
    url = baike_url % (text)
    myHeads = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36'}
    response = requests.get(url, headers=myHeads)
    #print response.text
    html_text = response.text.encode('iso-8859-1').decode('utf-8')
    if not regex.search(html_text):
        return True
    else:
        return False


def loadExcelCategory():
    book = xlrd.open_workbook(execl_path)
    for sheet in book.sheets():
        for cidx in range(sheet.ncols):
            print u'-----------------------------------第%d列:%s-----------------------------------' % (cidx, sheet.col_values(cidx)[0])
            for text in sheet.col_values(cidx):
                if not text or not text.strip():
                    continue
                text = text.strip()
                if not isExsitsInBaike(text):
                    print u'未被百度百科收录词条：%s' % text
                    continue
                if text in category_list:
                    print u'重复项: %s' % text
                    continue
                category_list.append(text)
        break

def printCateogryList():
    import os
    cate_file = open(new_category_file, 'w')
    for category in category_list:
        cate_file.write('%s%s' % (category.encode('utf-8'), '\n'))
    cate_file.close()

loadCategory()
loadExcelCategory()
printCateogryList()
