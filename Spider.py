#!/usr/bin/python
# -*-coding:utf-8-*-
__author__ = 'John'

from urllib import quote
import urllib2
import json
import os
import logging
import sys
import time

store_root = 'F:/temp/spiderman/'
# store_root = '/luobotec/jun.li/spiderman/files/'
category_list = []



logger = logging.getLogger()
format = logging.Formatter('%(asctime)s - %(levelname)s [%(threadName)s] %(module)s:%(lineno)s - %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(format)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

class Spider:
    category_count = 0
    total_count = 0
    def __init__(self):
        self.__loadCategory()

    def __loadCategory(self):
        del category_list[:]
        for line in open("category_conf.txt"):
            if not line or len(line.strip()) < 1:
                continue
            if line.startswith('#'):
                continue
            category_list.append(line.strip().decode('utf-8'))

    def clearCategoryCount(self):
        self.category_count = 0

    def incrementCategoryCount(self):
        self.category_count += 1

    def incrementTotalCount(self):
        self.total_count += 1

    def spiderCategory(self, pageCount, pageSize):
        start_pageNum = 0
        if not category_list:
            logger.error("category list is empty!")
            return
        for category in category_list:
            logger.info('start fetch category %s' % category)
            self.clearCategoryCount()
            for pageNum in range(start_pageNum, pageCount, 1):
                url = self.buildUrl(category, pageNum, pageSize)
                try:
                    ret_str = self.getPage(url).strip()
                    json_obj = self.parseJson(ret_str)
                    self.handleResult(json_obj, category)
                except Exception, e:
                    logger.error('%s: %s' % (type(e), e))

    def buildUrl(self, category, pageNum, pageSize):
        pass

    def handleResult(self, json_obj, category):
        pass

    def storeFile(self, data, file):
        if not file or os.path.exists(file):
            logger.debug('file %s is not or exists, skip!' % file)
            return False
        folder = os.path.dirname(file)
        if not os.path.exists(folder):
            os.makedirs(folder)
        store_file = open(file, 'wb')
        store_file.write(data)
        store_file.close()
        return True

    def getPage(self, url):
        response = None
        try:
            request = urllib2.Request(url)
            request.add_header('User-Agent',
                               'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.87 Safari/537.36')
            response = urllib2.urlopen(request)
        except Exception, e:
            logger.error('%s: %s' % (type(e), e))
            return
        code = response.getcode()
        if code != 200:
            logger.error('http ret code: %d' % code)
            return
        ret = response.read()
        response.close()
        return ret

    def parseJson(self, str):
        if not str:
            return None
        str = str.decode('gbk',errors='ignore')
        return json.loads(str)


class BaiduImageSpider(Spider):
    def __init__(self):
        Spider.__init__(self)

    def spiderCategory(self, pageCount, pageSize=30):
        logger.info("start fetch Baidu image...")
        Spider.spiderCategory(self, pageCount, pageSize)

    def buildUrl(self, category, pageNum, pageSize):
        category = quote(category.encode('utf-8'))
        url = 'http://image.baidu.com/search/acjson?tn=resultjson_com&ipn=rj&ct=201326592&is=&fp=result&queryWord=%s&cl=&lm=&ie=utf-8&oe=utf-8&adpicid=&st=&z=&ic=&word=%s&s=&se=&tab=&width=&height=&face=&istype=&qc=&nc=&fr=&pn=%d&rn=%d&gsm=3c' % (
        category, category, pageNum*pageSize, pageSize)
        return url

    def handleResult(self, json_obj, category):
        if not json_obj or not json_obj['data']:
            return
        for image_item in json_obj['data']:
            try:
                pic_url = image_item.get('objURL')
                if pic_url:
                    pic_url = Utils.decodeBaiduImageUrl(pic_url)
                    logger.debug(pic_url)
                    idx = 0
                    _idx = idx = pic_url.rindex('/')
                    if _idx != -1:
                        idx = _idx + 1
                    fileName = pic_url[idx:]
                    image_file = self.getPage(pic_url)
                    if image_file:
                        file_name = store_root + 'baidu/%s/%s' % (category, fileName)
                        if not (file_name.endswith('.jpg') or file_name.endswith('.JPG') or file_name.endswith('.jpeg') or file_name.endswith('.JPEG')):
                            file_name += '.jpg'
                        if self.storeFile(image_file, file_name):
                            self.incrementCategoryCount()
                            self.incrementTotalCount()
                            logger.info('[progress] category %s, category_count: %d, total_count: %d' % (category, self.category_count, self.total_count))
            except Exception,e:
                logger.error('%s: %s' % (type(e), e))

class SogouImageSpider(Spider):
    def __init__(self):
        Spider.__init__(self)

    def spiderCategory(self, pageCount, pageSize = 48):
        logger.debug('start fetch Sogou Image...')
        Spider.spiderCategory(self, pageCount, pageSize)


    def buildUrl(self, category, pageNum, pageSize):
        category = quote(category.encode('utf-8'))
        url = 'http://pic.sogou.com/pics?query=%s&mood=0&picformat=0&mode=1&di=2&start=%d&reqType=ajax&tn=0&reqFrom=result' % (
        category, pageNum * pageSize)
        return url

    def handleResult(self, json_obj, category):
        if not json_obj or not json_obj.has_key('items'):
            return
        for image_item in json_obj.get('items'):
            # thumbURL = image_item.get('pic_url')
            pic_url = image_item.get('thumbUrl')
            if pic_url:
                logger.debug(pic_url)
                try:
                    idx = 0
                    _idx = idx = pic_url.rindex('/')
                    if _idx != -1:
                        idx = _idx + 1
                    fileName = pic_url[idx:]
                    image_file = self.getPage(pic_url)
                    if image_file:
                        file_name = store_root + 'sogou/%s/%s' % (category, fileName)
                        if not (file_name.endswith('.jpg') or file_name.endswith('.JPG') or file_name.endswith('.JPEG') or file_name.endswith('.jpeg')):
                            file_name += '.jpg'
                        if self.storeFile(image_file, file_name):
                            self.incrementCategoryCount()
                            self.incrementTotalCount()
                            logger.info('[progress] category %s, category_count: %d, total_count: %d' % (category, self.category_count, self.total_count))
                except Exception, e:
                    logger.error('%s: %s' % (type(e), e))


class SoImageSpider(Spider):
    '''
    360 Image Fetcher
    '''
    def __init__(self):
        Spider.__init__(self)

    def buildUrl(self, category, pageNum, pageSize):
        category = quote(category.encode('utf-8'))
        url = 'http://image.so.com/j?q=%s&src=srp&sn=%d&pn=%d' % (category, pageNum * pageSize, pageSize)
        return url

    def spiderCategory(self, pageCount, pageSize=30):
        logger.debug('start fetch 360 Image...')
        Spider.spiderCategory(self, pageCount, pageSize)

    def handleResult(self, json_obj, category):
        if not json_obj or not json_obj.has_key('list'):
            return
        for image_item in json_obj.get('list'):
            pic_url = image_item.get('thumb')
            if pic_url:
                logger.debug(pic_url)
                try:
                    idx = 0
                    _idx = idx = pic_url.rindex('/')
                    if _idx != -1:
                        idx = _idx + 1
                    fileName = pic_url[idx:]
                    image_file = self.getPage(pic_url)
                    if image_file:
                        file_name = store_root + '360/%s/%s' % (category, fileName)
                        if not (file_name.endswith('.jpg') or file_name.endswith('.JPG') or file_name.endswith('.JPEG') or file_name.endswith('.jpeg')):
                            file_name += '.jpg'
                        if self.storeFile(image_file, file_name):
                            self.incrementCategoryCount()
                            self.incrementTotalCount()
                            logger.info('[progress] category %s, category_count: %d, total_count: %d' % (category, self.category_count, self.total_count))
                except Exception, e:
                    logger.error('%s: %s' % (type(e), e))



class Utils:
    u"""
    解码百度图片搜索json中传递的url
    抓包可以获取加载更多图片时，服务器向网址传输的json。
    其中objURL是特殊的字符串
    解码前：
    ippr_z2C$qAzdH3FAzdH3Ffl_z&e3Bftgwt42_z&e3BvgAzdH3F4omlaAzdH3Faa8W3ZyEpymRmx3Y1p7bb&mla
    解码后：
    http://s9.sinaimg.cn/mw690/001WjZyEty6R6xjYdtu88&690
    使用下面两张映射表进行解码。
    """

    __str_table = {
        '_z2C$q': ':',
        '_z&e3B': '.',
        'AzdH3F': '/'
    }

    __char_table = {
        'w': 'a',
        'k': 'b',
        'v': 'c',
        '1': 'd',
        'j': 'e',
        'u': 'f',
        '2': 'g',
        'i': 'h',
        't': 'i',
        '3': 'j',
        'h': 'k',
        's': 'l',
        '4': 'm',
        'g': 'n',
        '5': 'o',
        'r': 'p',
        'q': 'q',
        '6': 'r',
        'f': 's',
        'p': 't',
        '7': 'u',
        'e': 'v',
        'o': 'w',
        '8': '1',
        'd': '2',
        'n': '3',
        '9': '4',
        'c': '5',
        'm': '6',
        '0': '7',
        'b': '8',
        'l': '9',
        'a': '0'
    }

    __transDic = None

    # 构建翻译字典
    @classmethod
    def __maketranTable(cls):
        if cls.__transDic:
            return cls.__transDic
        else:
            from_str = ''
            to_str = ''
            for key, value in cls.__char_table.items():
                from_str += key
                to_str += value
            if len(from_str) > 0 and len(to_str) and len(from_str) == len(to_str):
                from string import maketrans
                cls.__transDic = maketrans(from_str, to_str)
            else:
                cls.__transDic = None
            return cls.__transDic

    @classmethod
    def decodeBaiduImageUrl(cls, url):
        url = url.encode('utf-8')
        # 先替换字符串
        for key, value in cls.__str_table.items():
            url = url.replace(key, value)
        # 再替换剩下的字符
        if not cls.__transDic:
            cls.__transDic = cls.__maketranTable()
        return url.translate(cls.__transDic)

if __name__ == '__main__':

    start_time = time.time()
    # start Fetch Sogou Image
    spider = SogouImageSpider()
    spider.spiderCategory(15)
    # spider.spiderCategory(1)

    # start Fetch 360 Image
    spider = SoImageSpider()
    spider.spiderCategory(20)
    # spider.spiderCategory(1)

    # start Fetch Baidu Image
    #spider = BaiduImageSpider()
    #spider.spiderCategory(1)
    #spider.spiderCategory(1)

    end_time = time.time()
    print("fetch job is finish, cost: %d s." % ((end_time - start_time)))



