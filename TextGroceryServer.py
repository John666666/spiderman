# coding=utf-8
__author__ = 'mengfl'
import sys
import time
import MySQLdb
import datetime
import json
import logging
from tgrocery import Grocery
import threading
import BaseHTTPServer
import urllib
import urlparse
reload(sys)
sys.setdefaultencoding('utf-8')
logger = logging.getLogger()
formatter = logging.Formatter('%(asctime)s  [%(levelname)s] %(filename)s [%(funcName)s:%(lineno)s] - %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# 全局变量
grocery = Grocery('resource_a')

# TextGrocery训练类
class TextGroceryTrain:

    def __init__(self):
        pass

    # 查询指定表的行数并返回
    def qryTotalRowNumByTable(self,cursor,table):
        cursor.execute('select COUNT(1) FROM ' + table)
        row = cursor.fetchone()
        if row == None:
            total_row_num = 0
        else:
            total_row_num = row[0]
        return total_row_num

    # 加载 nlu_Knowledge 表中的数据到训练文件中
    def loadNluKnowledge(self,cursor):
        logger.info('begin to load nlu_knowlege.')
        # 分页读取开始行号
        begin = 0
        # 分页大小设置为100
        page_size = 100
        # 读取表 nlu_knowledge 中的总行数
        total_row_num = self.qryTotalRowNumByTable(cursor, 'nlu_knowledge')
        while (begin < total_row_num):
            cursor.execute('select title,domain FROM nlu_knowledge order by id desc limit %s,%s',(begin,page_size))
            begin = begin + page_size
            datas = cursor.fetchall()
            for title,domain in datas:
                # 向文件中写入记录
                self.train_data_file.write(domain+':'+title+'\n')
            self.train_data_file.flush()
        logger.info('end to load nlu_knowlege. size = %d' %(total_row_num))

    # 加载 nlu_poem 表中的数据到训练文件中
    def loadNluPoem(self,cursor):
        logger.info('begin to load nlu_poem. ')
        # 分页读取开始行号
        begin = 0
        # 分页大小设置为100
        page_size = 100
        # 读取表 nlu_poem 中的总行数
        total_row_num = self.qryTotalRowNumByTable(cursor, 'nlu_poem')
        while (begin < total_row_num):
            cursor.execute('select title,domain FROM nlu_poem order by id desc limit %s,%s',(begin,page_size))
            begin = begin + page_size
            datas = cursor.fetchall()
            for title,domain in datas:
                # 向文件中写入记录
                self.train_data_file.write(domain+':'+title+'\n')
        logger.info('end to load nlu_poem. size = %d' % (total_row_num))

    # 加载 media 表中的数据到训练文件中
    def loadMedia(self,cursor):
        logger.info('begin to load media. ')
        # 分页读取开始行号
        begin = 0
        # 分页大小设置为100
        page_size = 100
        # 读取表 nlu_poem 中的总行数
        total_row_num = self.qryTotalRowNumByTable(cursor, 'media')
        while (begin < total_row_num):
            cursor.execute('select name FROM media order by id desc limit %s,%s',(begin,page_size))
            begin = begin + page_size
            datas = cursor.fetchall()
            for data in datas:
                # 向文件中写入记录
                self.train_data_file.write('media:'+data[0]+'\n')
        logger.info('end to load media. size = %d' % (total_row_num))
    # 加载训练数据
    def loadTrainData(self):

        self.train_data_file = open('/home/shimanqiang/TextGrocery/train_cn.txt', 'w')
        # 将文件 train_data_file 清空
        self.train_data_file.truncate()
        self.train_data_file.flush()

        self.nlu_cloud_conn = MySQLdb.connect(host='rdsl1xy9qgcil384wx75.mysql.rds.aliyuncs.com', user='nlu_cloud',
                                              passwd='nlu_cloud123',
                                              db='nlu_cloud', port=3306, charset='utf8')
        self.nlu_cloud_cur = self.nlu_cloud_conn.cursor()

        self.lb2api_conn = MySQLdb.connect(host='rdsl1xy9qgcil384wx75.mysql.rds.aliyuncs.com', user='lb2api',
                                           passwd='lb2api123',
                                           db='lb2api', port=3306, charset='utf8')
        self.lb2api_cur = self.lb2api_conn.cursor()

        self.loadNluKnowledge(self.nlu_cloud_cur)
        self.loadNluPoem(self.nlu_cloud_cur)
        self.loadMedia(self.lb2api_cur)

        self.nlu_cloud_cur.close()
        self.nlu_cloud_conn.close()
        self.lb2api_cur.close()
        self.lb2api_conn.close()
        self.train_data_file.close()

    # 训练主方法
    def train(self):
        try:
            self.loadTrainData()
            # 应用启动时默认的加载模型是resource_a，对等备份模型是resource_b
            global grocery
            if grocery.name == 'resource_a':
                self.train_grocery_name = 'resource_b'
            else:
                self.train_grocery_name = 'resource_a'
            logger.info('begin to train. train_grocery_name= %s' % (self.train_grocery_name))
            train_grocery = Grocery(self.train_grocery_name)
            train_grocery.train('train_cn.txt',':')
            train_grocery.save()
            grocery = train_grocery
            logger.info('end to train. train_grocery_name= %s' % (self.train_grocery_name))
        except Exception, e:
            logger.error("train Error %s" % (e))


class TextGroceryRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):

    # 处理一个GET请求
    def do_GET(self):
        uri = urlparse.urlparse(self.path).path.split('/')[-1]
        if 'refresh' == uri:
            train = TextGroceryTrain()
            train.train()
            result ='refresh success'
        else:
            result = self.classify(uri)

        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header("Content-Length", str(len(result)))
        self.end_headers()
        self.wfile.write(result)
    # 分类主方法
    def classify(self,text):
        input_txt = urllib.unquote(text)
        predict_result = grocery.predict(input_txt)
        output_txt = predict_result.predicted_y
        dict = predict_result.dec_values
        logger.debug('classify text=%s predicted_value=%s dec_value=%s' %(input_txt,predict_result.predicted_y,dict[predict_result.predicted_y]))
        if dict[predict_result.predicted_y] < 0.15:
            output_txt = ''
        return output_txt

if __name__ == '__main__':
    try:
        #程序启动时重新训练，默认训练resource_a
        train = TextGroceryTrain()
        train.train()
        serverAddress = ('', 8187)
        server = BaseHTTPServer.HTTPServer(serverAddress, TextGroceryRequestHandler)
        server.serve_forever()
    except KeyboardInterrupt:
        print '^C received ,shutting down server'