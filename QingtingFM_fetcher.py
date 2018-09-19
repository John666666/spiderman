#!/usr/bin/python
# -*-coding:utf8-*-
__author__ = 'John'

import json
import requests
import logging
import sys
import time
import threading

client_id = "OWI5M2UwMGUtZTFiZS0xMWU1LTkyM2YtMDAxNjNlMDAyMGFk"
client_secret = "ZTdiOTNhMTItNTQzMy0zNDY0LThhZDQtMzUxNjU0M2UxMzEz"

token = "ZTY2NmU3M2MtYThiOC00ZDIzLTlkMjUtNDBkOWM1Y2VlNTE0"

root_url = "http://api.open.qingting.fm"

logger = logging.getLogger()
format = logging.Formatter('%(asctime)s - %(levelname)s [%(threadName)s] %(module)s:%(lineno)s - %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(format)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# import MySQLdb

class QingTing:
    def __init__(self):
        #self.conn = MySQLdb.connect(host='localhost', user='nlu_cloud',
        #                            passwd='nlu_cloud123',
        #                            db='nlu_cloud', port=3306, charset='utf8')
        #self.cur = self.conn.cursor()
        pass

    def refreshToken(self):
        url = "/access?&grant_type=client_credentials&client_id=%s&client_secret=%s" % (client_id, client_secret)
        global token
        token = self.__post(url).get("access_token")


    def getOnDemandCategories(self):
        url = "/v6/media/categories"
        print json.dumps(self.__get(url), ensure_ascii=False)

    def getOnDemandCategoryAttrs(self, categoryId):
        url = "/v6/media/categories/%s" % (categoryId, )
        print json.dumps(self.__get(url), ensure_ascii=False)

    def getOnDemandChannels(self, categoryId, attrIds=None, curPage=1, pageSize=30):
        url = "/v6/media/categories/%s/channels/order/0"
        if attrIds is not None and isinstance(attrIds, list):
            url += "/attr"
            for attrId in attrIds:
                url += "/"+str(attrId)
        url += "/curpage/%s/pagesize/%s"
        finalUrl = url % (categoryId, curPage, pageSize)
        print json.dumps(self.__get(finalUrl), ensure_ascii=False)

    def getOnDemandChannelPrograms(self, channelId, curPage=1, pageSize=30):
        url = "/v6/media/channelondemands/%s/programs/curpage/%s/pagesize/%s" % (channelId, curPage, pageSize)
        print json.dumps(self.__get(url), ensure_ascii=False)

    def getOnDemandPlayUrl(self, channelId, programId):
        url = "/v6/media/audiostream/channelondemands/%s/programs/%s" % (channelId, programId)
        print json.dumps(self.__get(url), ensure_ascii=False)


    def __get(self, url, headers=None, data={}, withToken=True):
        final_url = "%s%s" % (root_url, url)
        if withToken:
            data["access_token"] = token
        req = requests.get(final_url, params=data, headers=headers)
        result = req.json()
        if result is not None and result["errorno"] == 20001:
            logger.info("token invalid, refresh token and retry")
            self.refreshToken()
            return self.__get(url, headers, data, withToken)
        return result


    def __post(self, url, headers=None, data=None):
        url = "%s%s" % (root_url, url)
        req = requests.post(url, params=data, headers=headers)
        return req.json()


if __name__ == '__main__':
    qtfm = QingTing()
    # qtfm.getOnDemandCategories()
    # qtfm.getOnDemandCategoryAttrs(3251)
    # qtfm.getOnDemandChannels(3251, attrIds=[1508, ])
    # qtfm.getOnDemandChannelPrograms(100706)
    qtfm.getOnDemandPlayUrl(83664, 342464)