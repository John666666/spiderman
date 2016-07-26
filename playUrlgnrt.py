# coding=utf-8
__author__ = 'de'
import sys
import time
import MySQLdb
import datetime
import urllib2
import json
reload(sys)
sys.setdefaultencoding('utf-8')

try:
    conn = MySQLdb.connect(host='rdsl1xy9qgcil384wx75.mysql.rds.aliyuncs.com', user='nlu_cloud', passwd='nlu_cloud123',
                           db='nlu_cloud', port=3306, charset='utf8')
    cur = conn.cursor()
    cur.execute('select id,play_html from nlu_music WHERE play_url is null')
    datas = cur.fetchall()
    for data in datas:
        id = data[0]
        playHtml = data[1]
        if playHtml:
            idParam = playHtml.split('&')[1]
            songId = idParam.split('=',1)[1]
            dataUrl = 'http://music.163.com/api/song/detail/?id='+ songId + '&ids=%5B' + songId + '%5D&csrf_token='
            print 'dataUrl=' + dataUrl
            request = urllib2.Request(dataUrl)
            response = urllib2.urlopen(request)
            ret = response.read().strip()
            if not ret:
                print 'response error'
            else:
                str = ret.decode('utf-8', errors='ignore')
                json_obj = json.loads(str)
                if not json_obj or not json_obj.has_key('songs'):
                    print 'parse error json_obj='+json_obj
                else:
                    for song in json_obj.get('songs'):
                        mp3Url = song.get('mp3Url')
                        print 'songId='+ songId + ' mp3Url=' +mp3Url
                        update_music = [mp3Url ,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),id]
                        result = cur.execute(
                            'update nlu_music SET PLAY_URL = %s,UPDATE_TIME = %s WHERE ID = %s',
                            update_music)
                        print conn.commit()
                        break
            response.close()

    cur.close()
    conn.close()

except MySQLdb.Error, e:
    print "Mysql Error %d: %s" % (e.args[0], e.args[1])

