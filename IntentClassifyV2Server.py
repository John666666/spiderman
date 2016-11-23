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
import urllib2
from SocketServer import ThreadingTCPServer, StreamRequestHandler
from socket import *

reload(sys)
sys.setdefaultencoding('utf-8')
logger = logging.getLogger()
formatter = logging.Formatter('%(asctime)s  [%(levelname)s] %(filename)s [%(funcName)s:%(lineno)s] - %(message)s')
# handler = logging.StreamHandler(sys.stdout)
handler = logging.FileHandler('intentClassifyV2Server.log', 'w')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)
logging.basicConfig(level=logging.DEBUG)
handler.setLevel(logging.DEBUG)

# 全局变量 grocerySet
grocerySet = {}

# TextGrocery训练类
class TextGroceryTrain:
    trainSet = {}


    # 查询指定表的行数并返回
    def qryTotalRowNumByTable(self, cursor, table):
        cursor.execute('select COUNT(1) FROM ' + table)
        row = cursor.fetchone()
        if row == None:
            total_row_num = 0
        else:
            total_row_num = row[0]
        return total_row_num

    # 加载训练数据
    def loadTrainData(self):
        self.nlu_cloud_conn = MySQLdb.connect(host='rdsl1xy9qgcil384wx75.mysql.rds.aliyuncs.com', user='nlu_cloud',
                                              passwd='nlu_cloud123',
                                              db='nlu_cloud', port=3306, charset='utf8')
        self.nlu_cloud_cur = self.nlu_cloud_conn.cursor()
        self.loadIntentClassifyData(self.nlu_cloud_cur)
        self.nlu_cloud_cur.close()
        self.nlu_cloud_conn.close()

    # 加载意图分类原始数据,将数据写入到raw_train_cn.txt
    def loadIntentClassifyData(self, cursor):
        logger.info('begin to load intent_classify_data.')
        raw_train_data_file = open('raw_train_cn.txt','w')
        # 分页读取开始行号
        begin = 0
        # 分页大小设置为100
        page_size = 100
        # 读取表 nlu_knowledge 中的总行数
        total_row_num = self.qryTotalRowNumByTable(cursor, 'intent_classify_data')
        preDomain = ''
        while (begin < total_row_num):
            cursor.execute('select content,domain,intent FROM intent_classify_data order by domain desc limit %s,%s', (begin, page_size))
            begin = begin + page_size
            datas = cursor.fetchall()
            for content, domain,intent in datas:
                if preDomain != domain:
                    raw_train_data_file.write("@" + domain + '\n')
                    preDomain = domain
                # 向文件中写入记录
                raw_train_data_file.write(intent +":" + content + '\n')
            raw_train_data_file.flush()
        raw_train_data_file.close()
        logger.info('end to load intent_classify_data. size = %d' % (total_row_num))

    # 创建训练文件
    def createTrainDataFile(self):
        # 读取原始数据文件
        rawDataFile = open('raw_train_cn.txt')
        lines = rawDataFile.readlines()
        domainLines = []
        domain = None
        self.writeMainTrainFile(lines)
        for line in lines:
            if not line or len(line.strip()) < 1:
                continue
            if line.startswith('@'):
                if domain:
                    self.writeTrainFile(domain, domainLines)
                    domain = ''
                    domainLines = []
                domain = line[1:]
                if domain:
                    domain = domain.strip()
            else:
                domainLines.append(line)
        if domain:
            self.writeTrainFile(domain, domainLines)

    # 写主训练文件
    def writeMainTrainFile(self,lines):
        mainTrainFile = open('main_train.txt', 'w')
        domain = ''
        for line in lines:
            if not line or len(line.strip()) < 1:
                continue
            if line.startswith('@'):
                domain = line[1:].strip()
                continue
            else:
                intent, text = line.split(":", 2)
            mainTrainFile.write(domain + ':' + text)
        mainTrainFile.flush()
        mainTrainFile.close()
        self.trainSet['main'] = 'main_train.txt'

    # 写训练文件
    def writeTrainFile(self,domain, lines):
        trainFile = open(domain + '_train.txt', 'w')
        for line in lines:
            trainFile.write(line)
        trainFile.close()
        self.trainSet[domain] = domain + '_train.txt'

    # 训练主方法
    def train(self):
        try:
            self.loadTrainData()
            self.createTrainDataFile()
            for trainId in self.trainSet:
                global grocerySet
                if not grocerySet:
                    grocerySet = {}
                if not grocerySet.get(trainId):
                    train_grocery_name = trainId + '_resource_a'
                    train_grocery = Grocery(train_grocery_name)
                    train_grocery.train(self.trainSet[trainId], ':')
                    train_grocery.save()
                    grocerySet[trainId] = train_grocery
                else:
                    grocery = grocerySet[trainId]
                    train_grocery_name = trainId + '_resource_a'
                    if grocery.name == train_grocery_name:
                        train_grocery_name = trainId + '_resource_b'
                    train_grocery = Grocery(train_grocery_name)
                    train_grocery.train(self.trainSet[trainId], ':')
                    train_grocery.save()
                    grocerySet[trainId] = train_grocery
        except Exception, e:
            logger.error("train Error %s" % (e))

class TextGroceryTCPRequestHandler(StreamRequestHandler):
    def handle(self):
        logger.info('get connection from %s' % (self.client_address,))
        while True:
            # 客户端主动断开连接时，self.rfile.readline()会抛出异常
            try:
                data = self.rfile.readline().strip()
                if 'refresh' == data:
                    train = TextGroceryTrain()
                    train.train()
                    result = 'refresh success'
                else:
                    params = data.split('@')
                    if len(params) != 2:
                        logger.error('classify param num error. data=%s' %(data))
                        result = 'classify param number need to be 2'
                    else:
                        result = self.classify(params[0],params[1])

                self.wfile.write(result + '\n')
            except Exception, e:
                logger.error("connection reset client address=%s error=%s" % (self.client_address, e,))
                break
    # 分类主方法
    def classify(self,domain, text):
        logger.info('classify domain=%s text=%s'%(domain,text))
        input_txt = text
        global grocerySet
        if not grocerySet:
            logger.error('grocerySet do not inited.')
        else:
            if not grocerySet.get(domain):
                logger.error('grocery %s do not inited.' %(domain))
        grocery = grocerySet[domain]
        logger.info('classify grocery name=%s' % (grocery.name))
        predict_result = grocery.predict(input_txt)
        output_txt = predict_result.predicted_y
        dict = predict_result.dec_values
        logger.debug('classify text=%s predicted_value=%s dec_value=%s' % (
            input_txt, predict_result.predicted_y, dict[predict_result.predicted_y]))
        if dict[predict_result.predicted_y]:
            output_txt = output_txt + '##' + dict[predict_result.predicted_y]
        else:
            output_txt = 'None'
        return output_txt

if __name__ == '__main__':
    try:
        # 程序启动时重新训练，默认训练resource_a
        logger.info('begin to start Intent Classify Server.')
        train = TextGroceryTrain()
        train.train()
        serverAddress = ('', 8189)
        server = ThreadingTCPServer(serverAddress, TextGroceryTCPRequestHandler)
        logger.info('start Intent Classify Server successfully.')
        server.serve_forever()
    except KeyboardInterrupt:
        print '^C received ,shutting down server'
        sys.exit(-1)