# coding=utf-8
__author__ = 'mengfl'

import urllib2
import json
import logging
import threading
import sys
import time
import MySQLdb
import datetime
import re
import ConfigParser
reload(sys)
sys.setdefaultencoding('utf-8')

logger = logging.getLogger()


class HourlyNewsSpider:
    def __init__(self):
        try:
            config = ConfigParser.SafeConfigParser()
            config.read('config.ini')
            self.conn = MySQLdb.connect(host=config.get('db', 'lb2api.host'),
                                        user=config.get('db', 'lb2api.user'),
                                        passwd=config.get('db', 'lb2api.passwd'),
                                        db=config.get('db', 'lb2api.db'),
                                        port=config.getint('db', 'lb2api.port'),
                                        charset=config.get('db', 'lb2api.charset'))
            self.cur = self.conn.cursor()

        except MySQLdb.Error, e:
            logger.error('Mysql Error ' + e.args[0] + ' ' + e.args[1])

    def spiderNews(self):
        # 获取12点的新闻报道
        for hour in range(9, 21, 1):
            programsUrl = 'http://bk2.radio.cn/mms4/videoPlay/getMorePrograms.jspa?programName=正点播报' + str(hour) + '00&start=0&limit=3&channelId=18'
            if hour < 10:
                programsUrl = 'http://bk2.radio.cn/mms4/videoPlay/getMorePrograms.jspa?programName=正点播报0' + str(hour) + '00&start=0&limit=3&channelId=18'
            try:
                request = urllib2.Request(programsUrl)
                response = urllib2.urlopen(request)
                ret = response.read().strip()
                if not ret:
                    logger.error('get hourly news failed. hour=&s' %(hour))
                else:
                    string = ret.decode('utf-8', errors='ignore')
                    string = string[5:-1]
                    json_obj = json.loads(string)
                    if not json_obj or not json_obj.has_key('programs'):
                        logger.warn('get result hourly news failed. hour=&s' %(hour))
                    else:
                        for program in json_obj.get('programs'):
                            programId = program.get('programId')
                            programName = program.get('programName')
                            createTime = program.get('creationTime')
                            publishTime = createTime + ' '+ str(hour) + ":00:00"
                            if hour < 10:
                                publishTime = createTime + ' 0' + str(hour) + ":00:00"
                            dataUrl = 'http://bk2.radio.cn/mms4/videoPlay/getVodProgramPlayUrlJson.jspa?programId='+str(programId)+'&programVideoId=0&videoType=PC&terminalType=515104503&dflag=1'
                            dataRequest = urllib2.Request(dataUrl)
                            dataResponse = urllib2.urlopen(dataRequest)
                            ret = dataResponse.read().strip()
                            playUrl = re.search('<title>(.*)</title>',ret).group(1)
                            # 整点新闻发布时间
                            # 查询数据库中是否有数据，如果没有，插入，如果有，跳过
                            # 向数据库中插入新闻信息
                            news_media_sel_key = [programId]
                            self.cur.execute(
                                'select id from news_media where id = %s', news_media_sel_key)
                            row_num = self.cur.fetchone()
                            if row_num == None:
                                news_media_value = [programId, programName, '93', 'ChinaVoice', playUrl, '1',
                                                    publishTime,
                                                    datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                                    datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
                                self.cur.execute(
                                    'insert into news_media (ID,NAME,ALBUM_ID,SOURCE,PLAY_URL,STATUS,PUBLISH_TIME,CREATE_TIME,UPDATE_TIME) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                                    news_media_value)
                                self.conn.commit()
                                logger.info('insert news_media successfully programId=%s' % (str(programId)))
                            else:
                                continue


                response.close()
            except Exception, e:
                logger.error('get news play url failed , hour=%s ,error= %s: %s' % (str(hour), type(e), e))

# 定时处理类
class Timer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):

        while True:
            # 每隔一小时判断一次
            time.sleep(60 * 60)

if __name__ == '__main__':
    try:
        config = ConfigParser.SafeConfigParser()
        config.read('config.ini')

        formatter = logging.Formatter(
            '%(asctime)s  [%(levelname)s] %(filename)s [%(funcName)s:%(lineno)s] - %(message)s')
        handler = logging.FileHandler(config.get("log","file"), 'w')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(config.getint("log","level"))

        logger.info('begin to start hourlyNews spider .')
        spider = HourlyNewsSpider()
        while True:
            logger.info('spider hourly news begin at time=%s' %(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            spider.spiderNews()
            logger.info('spider hourly news end at time=%s' % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            # 每隔一小时判断一次
            time.sleep(60 * 60)

    except KeyboardInterrupt:
        print '^C received ,shutting down server'
        sys.exit(-1)