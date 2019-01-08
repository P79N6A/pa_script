# -*- coding: utf-8 -*-
__author__ = "xiaoyuge"

import os
import PA_runtime
import sqlite3
from PA_runtime import *
import threading
import traceback
import clr
try:
    clr.AddReference('model_media')
    clr.AddReference('mimetype_dic')
except:
    pass
del clr
import hashlib
from model_media import *
import model_media
import mimetype_dic
from mimetype_dic import dic1, dic2

import re
import Queue
from threading import Thread
from PIL import Image
from PIL.ExifTags import TAGS

VERSION_APP_VALUE = 2


class MediaParse(object):
    def __init__(self, node, extractDeleted, extractSource):
        self.fs = node.FileSystem
        fileNode = self.fs.Search('/DCIM/.*\..*$')
        self.filesNode = []
        for node in fileNode:
            self.filesNode.append(node)
        self.extractDeleted = False
        self.extractSource = extractSource
        self.db = None
        self.mm = MM()
        self.cache_path = ds.OpenCachePath("MEDIA")
        md5_db = hashlib.md5()
        db_name = 'media'
        md5_db.update(db_name.encode(encoding = 'utf-8'))
        self.db_cache = self.cache_path + "\\" + md5_db.hexdigest().upper() + ".db"
        self.mime_dic = mimetype_dic.dic1
        self.medias = []

    def parse(self):
        if self.mm.need_parse(self.db_cache, VERSION_APP_VALUE):
            self.mm.db_create(self.db_cache)
            self.analyze_media()
            self.mm.db_insert_table_version(model_media.VERSION_KEY_DB, model_media.VERSION_VALUE_DB)
            self.mm.db_insert_table_version(model_media.VERSION_KEY_APP, VERSION_APP_VALUE)
            self.mm.db_commit()
            self.mm.db_close()
        generate = Generate(self.db_cache)
        models = generate.get_models()
        return models

    def analyze_media(self):
        q = Queue.Queue()
        for fileNode in self.filesNode:
            q.put(fileNode)
        while not q.empty():
            threads = []
            t = threading.Thread(target=self.ana_media, args=(q.get(),))
            t.start()
            t.join()
        for media in self.medias:
            self.mm.db_insert_table_media(media)
        self.mm.db_commit()

    def ana_media(self, fileNode):
        try:
            video_suffix = dic2['video']
            image_suffix = dic2['image']
            suffixes = []
            suffixes.extend(video_suffix)
            suffixes.extend(image_suffix)
            suffixes = tuple(suffixes)
            name = fileNode.PathWithMountPoint
            if name.endswith(suffixes):
                try:
                    media = Media()
                    canceller.ThrowIfCancellationRequested()
                    filePath = name  #文件路径
                    fileSize = os.path.getsize(filePath)  #文件大小
                    media.size = fileSize
                    filecCeateDate = os.path.getmtime(filePath)  #文件创建时间
                    media.add_date = filecCeateDate
                    fileModifyDate = os.path.getmtime(filePath)  #文件修改时间
                    media.modify_date = fileModifyDate
                    fileName = os.path.basename(filePath)  #文件名
                    media.display_name = fileName
                    fileSuffix = re.sub('.*\.', '', filePath)  #文件后缀
                    media.mime_type = fileSuffix
                    fileType = dic1[fileSuffix]  #文件类型
                    media.type = fileType
                    fileParFullDir = os.path.dirname(filePath)
                    fileParDir = os.path.basename(fileParFullDir)  #文件所在文件夹名
                    media.parent = fileParDir
                    fileAbsolutePath = fileNode.AbsolutePath
                    media.source = fileAbsolutePath
                    media.url = fileAbsolutePath
                    if fileType == 'image':
                        fileMetaData = self._get_exif_data(filePath, fileParDir)
                        fileLatitude = fileMetaData[1]  #文件纬度
                        media.latitude = fileLatitude
                        fileLongitude = fileMetaData[0]  #文件经度
                        media.longitude = fileLongitude
                        fileWidth = fileMetaData[2]  #图片宽
                        media.width = fileWidth
                        fileHeight = fileMetaData[3]  #图片高
                        media.height = fileHeight
                        media.datetaken = filecCeateDate
                    elif fileType == 'video':
                        fileDuration = self._get_video_duration(filePath)  #文件播放时长
                        media.duration = fileDuration
                    self.medias.append(media)
                    print(len(self.medias))
                except Exception as e:
                    print(e)
        except Exception as e:
            traceback.print_exc()

    @staticmethod
    def _get_exif_data(fname, fileParDir):
        '''获取图片metadata'''
        result = [None, None, None, None]
        if fileParDir == 'Camera':
            ret = {}
            img = Image.open(fname)
            if hasattr(img, '_getexif'):
                exifinfo = img._getexif()
                if exifinfo != None:
                    for tag, value in exifinfo.items():
                        decoded = TAGS.get(tag, tag)
                        ret[decoded] = value
            try:
                latitude = None
                longitude = None
                if 'GPSInfo' in ret.keys():
                    latitude = 0.0
                    longitude = 0.0
                    GPSInfo = ret['GPSInfo']
                    latitudeFlag = GPSInfo[1]
                    latitude = float(GPSInfo[2][0][0])/float(GPSInfo[2][0][1]) + float(GPSInfo[2][1][0])/float(GPSInfo[2][1][1])/float(60) + float(GPSInfo[2][2][0])/float(GPSInfo[2][2][1])/float(3600)
                    longitudeFlag = GPSInfo[3]
                    longitude = float(GPSInfo[4][0][0])/float(GPSInfo[4][0][1]) + float(GPSInfo[4][1][0])/float(GPSInfo[4][1][1])/float(60) + float(GPSInfo[4][2][0])/float(GPSInfo[4][2][1])/float(3600)
                result[0] = longitude
                result[1] = latitude
            except:
                traceback.print_exc()
            try:
                width = None
                height = None
                if 'ExifImageWidth' in ret.keys() and 'ImageLength' in ret.keys():
                    width = ret['ExifImageWidth']
                    height = ret['ImageLength']
                result[2] = width
                result[3] = height
            except:
                pass
        return result

    @staticmethod
    def _get_video_duration(filePath):
        '''获取视频的时长'''
        duration = 0
        return duration

def analyze_android_media(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(MediaParse(node, extractDeleted, extractSource).parse())
    pr.Build('多媒体')
    return pr

def execute(node, extractDeleted):
    return analyze_android_media(node, extractDeleted, False)