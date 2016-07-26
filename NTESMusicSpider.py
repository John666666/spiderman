# coding=utf-8
__author__ = 'mengfl'

import urllib2
import urllib
import json
import logging
from pyquery import PyQuery
from selenium import webdriver
from selenium.webdriver.common.by import By
import sys
import time
import MySQLdb
import datetime

reload(sys)
sys.setdefaultencoding('utf-8')

logger = logging.getLogger()
formatter = logging.Formatter('%(asctime)s  [%(levelname)s] %(filename)s [%(funcName)s:%(lineno)s] - %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


class NTESMusicSpider:
    # 专门爬取歌手列表页浏览器实例
    artists_driver = webdriver.PhantomJS()
    # 专门爬取歌手专辑列表的浏览器实例
    albums_driver = webdriver.PhantomJS()
    # 专门爬取
    # 专辑内容的浏览器实例
    album_driver = webdriver.PhantomJS()
    # 专门爬取歌曲内容的浏览器实例
    song_driver = webdriver.PhantomJS()
    # 已爬取的歌曲URL
    spideredUrls = {}
    def __init__(self):
        self.artists_driver.set_page_load_timeout(60)
        self.albums_driver.set_page_load_timeout(60)
        self.album_driver.set_page_load_timeout(60)
        self.song_driver.set_page_load_timeout(60)
        try:
            self.conn = MySQLdb.connect(host='rdsl1xy9qgcil384wx75.mysql.rds.aliyuncs.com', user='nlu_cloud',
                                        passwd='nlu_cloud123',
                                        db='nlu_cloud', port=3306, charset='utf8')
            self.cur = self.conn.cursor()
            self.loadSpideredSongUrls()
            self.spideredSongUrlsFile = open('E:/pythonworkspace/spider/spideredSongUrls.txt', 'a')

        except MySQLdb.Error, e:
            logger.error('Mysql Error ' + e.args[0] + ' ' + e.args[1])
    # 从文件中读取已爬取的歌曲/专辑列表
    def loadSpideredSongUrls(self):
        for line in open('E:/pythonworkspace/spider/spideredSongUrls.txt'):
            if not line or len(line.strip()) < 1:
                continue
            spiderSongUrl = line.strip('\n')
            self.spideredUrls[spiderSongUrl] = True

    # 爬取签约歌手
    def spiderSignedArtists(self):
        logger.info('start to spider music.163.com signed artists ...')
        start_time = time.time()
        artists_driver = webdriver.PhantomJS()
        self.artists_driver.get('http://music.163.com/#/discover/artist/signed/')
        self.artists_driver.set_window_size(4800, 8000)
        self.artists_driver.switch_to.frame('contentFrame')
        # m-sgerlist
        artistliEles = self.artists_driver.find_element_by_id('m-artist-box').find_elements(by=By.CSS_SELECTOR,
                                                                                            value='li > p')
        for artistEle in artistliEles:
            artistA = artistEle.find_element_by_tag_name('a')
            artistUrl = artistA.get_attribute('href')
            self.spiderArtist(artistUrl)

        try:
            self.conn.close()
            self.cur.close()
            self.artists_driver.quit()
            self.albums_driver.quit()
            self.album_driver.quit()
            self.song_driver.quit()
            self.spideredSongUrlsFile.close()
        except Exception, e:
            logger.error('Mysql Error ' + e.args[0] + ' ' + e.args[1])
        end_time = time.time()
        logger.info('end to spider music.163.com signed artists , cost:')
        logger.info(end_time - start_time)

    # 爬取歌手信息
    def spiderArtist(self, artistUrl):
        albumsUrl = 'http://music.163.com/artist/album?' + artistUrl.split('?')[1]
        logger.info('start to spider artist, url=' + artistUrl)
        logger.info('start to spider first page albums, albumsUrl=' + albumsUrl)
        try:
            albums_driver = webdriver.PhantomJS()
            # 获取当前歌手所有专辑
            self.albums_driver.get(albumsUrl)
            self.albums_driver.switch_to.frame('contentFrame')

            albumEles = self.albums_driver.find_elements(by=By.CSS_SELECTOR, value='#m-song-module li')
            for albumEle in albumEles:
                albumUrl = albumEle.find_element_by_css_selector('a').get_attribute('href')
                if True == self.spideredUrls.get(albumUrl):
                    logger.warn('album has been spidered.skip. albumUrl='+ albumUrl)
                else:
                    self.spiderAlbum(albumUrl)

            logger.info('end to spider first page albums, albumsUrl=' + albumsUrl)
            # 获取下一页的URL
            try:
                nextPageEle = self.albums_driver.find_element_by_css_selector('.u-page a.zbtn.znxt');
            except:
                logger.error('nextPage Button does not exist, url=' + artistUrl)
            else:
                nextPageUrl = self.albums_driver.find_element_by_css_selector('.u-page a.zbtn.znxt').get_attribute(
                    'href')
                self.spiderNextPageAlbums(nextPageUrl)
        except Exception, e:
            logger.error('spider first page albums, url=%s ,error= %s: %s' % (albumsUrl, type(e), e))
            logger.warn('restart to spider first page albums. albumsUrl=' + albumsUrl)
            self.spiderArtist(artistUrl)
        logger.info('end to spider artist, url=' + artistUrl)

    def spiderNextPageAlbums(self, albumsUrl):
        logger.info('begin to spider next page albums, url=' + albumsUrl)
        try:
            self.albums_driver.get(albumsUrl)
            self.albums_driver.switch_to.frame('contentFrame')
            albumEles = self.albums_driver.find_elements(by=By.CSS_SELECTOR, value='#m-song-module li')
            for albumEle in albumEles:
                albumUrl = albumEle.find_element_by_css_selector('a').get_attribute('href')
                self.spiderAlbum(albumUrl)
            # 获取下一页的URL
            try:
                nextPageEle = self.albums_driver.find_element_by_css_selector('.u-page a.zbtn.znxt');
            except:
                logger.error('nextPage Button does not exist, url=' + albumsUrl)
            else:
                nextPageUrl = self.albums_driver.find_element_by_css_selector('.u-page a.zbtn.znxt').get_attribute(
                    'href')
                if nextPageUrl != 'javascript:void(0)':
                    self.spiderNextPageAlbums(nextPageUrl)
        except Exception, e:
            logger.error('spider next page albums, url=%s ,error= %s: %s' % (albumsUrl, type(e), e))
            logger.warn('restart to spider next page albums. albumsUrl=' + albumsUrl)
            self.spiderNextPageAlbums(albumsUrl)

        logger.info('end to spider next page albums, url=' + albumsUrl)

    def spiderAlbum(self, albumUrl):
        logger.info('begin to spider album, url=' + albumUrl)
        try:
            self.album_driver.get(albumUrl)
            self.album_driver.switch_to.frame('contentFrame')
            # 获取专辑名称
            albumName = self.album_driver.find_element_by_css_selector('.topblk .f-ff2').text
            # 获取专辑封面
            albumCover = self.album_driver.find_element_by_css_selector('.cover img.j-img').get_attribute('data-src')
            # 获取专辑基本信息
            intrs = self.album_driver.find_elements(by=By.CSS_SELECTOR, value='.topblk .intr')
            # 歌手 发行时间 发行公司
            for field in intrs:
                fieldName, fieldValue = field.text.split('：', 1)
                if fieldName == '歌手':
                    singer = fieldValue.strip()

            # 展开专辑介绍
            try:
                self.album_driver.find_element_by_css_selector('#album-desc-spread').click()
            except:
                logger.error('album desc spread link does not exist, albumUrl=' + albumUrl)
            # 专辑介绍
            try:
                albumDesc = self.album_driver.find_element_by_css_selector('.n-albdesc').text
            except:
                logger.error('album desc does not exist, albumUrl=' + albumUrl)
            # 获取播放外链
            outchain = self.album_driver.find_element_by_css_selector('a[data-action=outchain]').get_attribute(
                'data-href')
            playHtml = self.gnrtPlayHtml(outchain)
            # 向数据库中插入专辑信息
            album_sel_key = [singer, albumName]
            self.cur.execute(
                'select id from nlu_music_album where singer = %s and chinese_name=%s', album_sel_key)
            row_num = self.cur.fetchone()
            if row_num == None:
                album_value = [albumName, singer, playHtml, albumCover,
                               datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                               datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
                self.cur.execute(
                    'insert into nlu_music_album (CHINESE_NAME,SINGER,PLAY_HTML,COVER_URL,CREATE_TIME,UPDATE_TIME) VALUES(%s,%s,%s,%s,%s,%s)',
                    album_value)
                albumRowId = self.cur.lastrowid
            else:
                logger.warn('album has exist. albumUrl='+albumUrl)
                albumRowId = row_num[0]
            # 获取专辑歌曲列表
            songEles = self.album_driver.find_elements(by=By.CSS_SELECTOR, value='.n-songtb tbody>tr')

            for songEle in songEles:
                songUrl = songEle.find_element_by_css_selector('a').get_attribute(
                    'href')
                if True == self.spideredUrls.get(songUrl):
                    logger.warn('song has been spidered.skip. songUrl='+ songUrl)
                else:
                    self.spiderSong(songUrl, albumRowId)

            self.album_driver.find_element_by_css_selector('a[data-action=outchain]').click()

            if albumUrl == self.album_driver.current_url:
                logger.warn(' current album have not the copyright. albumUrl=' + albumUrl)
                update_album_value = ['', datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), albumRowId]
                self.cur.execute(
                    'update nlu_music_album SET PLAY_HTML = %s,UPDATE_TIME = %s WHERE ID = %s',
                    update_album_value)

            self.conn.commit()
            # 记录当前专辑的URL到文件中
            self.spideredSongUrlsFile.write(albumUrl + '\n')
            self.spideredSongUrlsFile.flush()
        except Exception, e:
            logger.error('spider album error, url=%s ,error= %s: %s' % (albumUrl, type(e), e))
            logger.warn('restart to spider albumUrl. albumUrl=' + albumUrl)
            self.spiderAlbum(albumUrl)
        # self.album_driver.close()
        logger.info('end to spider album, url=' + albumUrl)

    def spiderSong(self, songUrl, albumRowId):
        logger.info('begin to spider song, url=' + songUrl)
        try:
            self.song_driver.get(songUrl)
            self.song_driver.switch_to.frame('contentFrame')

            try:
                expandEle = self.song_driver.find_element_by_id('flag_ctrl')
            except:
                logger.error('flag_ctrl not exist, songUrl=' + songUrl)
            else:
                expandEle.click()

            songName = self.song_driver.find_element_by_css_selector('.m-lycifo em.f-ff2').text
            intrs = self.song_driver.find_elements(by=By.CSS_SELECTOR, value='.m-lycifo p.des.s-fc4')
            for intr in intrs:
                name, value = intr.text.split('：', 1)
                if name == '歌手':
                    singer = value.strip()
                elif name == '所属专辑':
                    albumName = value.strip()
            # 获取歌曲封面链接
            songCoverUrl = self.song_driver.find_element_by_css_selector('.m-lycifo .u-cover img.j-img').get_attribute(
                'data-src')
            # 获取播放外链
            outchain = self.song_driver.find_element_by_css_selector('.m-lycifo a[data-action=outchain]').get_attribute(
                'data-href')
            playHtml = self.gnrtPlayHtml(outchain)

            # 向数据库中插入歌曲信息
            music_value = [songName, albumName, singer, playHtml, songCoverUrl, albumRowId,
                           datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                           datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")]

            self.song_driver.find_element_by_css_selector('.m-lycifo a[data-action=outchain]').click()

            if songUrl == self.song_driver.current_url:
                logger.warn('current song have not the copyright. songUrl=' + songUrl)
                # 记录当前歌曲的URL到文件中
                self.spideredSongUrlsFile.write(songUrl + '\n')
                self.spideredSongUrlsFile.flush()
            else:
                music_sel_key = [albumRowId, songName]
                self.cur.execute(
                    'select count(1) from nlu_music where album_id = %s and chinese_name = %s',
                    music_sel_key)
                row_num = self.cur.fetchone()
                if row_num[0] == 0:
                    self.cur.execute(
                        'insert into nlu_music (CHINESE_NAME,ALBUM_NAME,SINGER,PLAY_HTML,COVER_URL,ALBUM_ID,CREATE_TIME,UPDATE_TIME) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',
                        music_value)
                    self.conn.commit()
                else:
                    logger.warn('song has exist. songUrl='+songUrl)
                # 记录当前歌曲的URL到文件中
                self.spideredSongUrlsFile.write(songUrl+'\n')
                self.spideredSongUrlsFile.flush()
        except Exception, e:
            logger.error('spider song error, url=%s ,error= %s: %s' % (songUrl, type(e), e))
            logger.warn('restart to spider song. songUrl=' + songUrl)
            self.spiderSong(songUrl, albumRowId)
        # self.song_driver.close()
        logger.info('end to spider song, url=' + songUrl)

    # 生成播放HTML代码
    def gnrtPlayHtml(self, outchainUri):
        outchain_params = outchainUri.split('/')
        type = outchain_params[2]
        id = outchain_params[3]
        if '2' == type:
            playHtml = '<iframe frameborder="no" border="0" marginwidth="0" marginheight="0" ' \
                       'width=330 height=86 src="http://music.163.com/outchain/player?type=' \
                       + type + '&id=' + id + '&auto=1&height=66"></iframe>'
        elif '1' == type:
            playHtml = '<iframe frameborder="no" border="0" marginwidth="0" marginheight="0" ' \
                       'width=330 height=450 src="http://music.163.com/outchain/player?type=' \
                       + type + '&id=' + id + '&auto=1&height=430"></iframe>'
        return playHtml


if __name__ == '__main__':
    spider = NTESMusicSpider()
    spider.spiderSignedArtists()
    # spider.spiderArtist('10559')
    # spider.spiderAlbum('/album?id=2696067')
    # spider.spiderSong('/song?id=27928265')
    # spider.spiderSong('http://music.163.com/song?id=26508186',1)
    # spider.spiderSong('http://music.163.com/song?id=327536',1)
    # playHtml = spider.gnrtPlayHtml('/outchain/2/27928265/')
