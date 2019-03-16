# -*- coding: utf-8 -*-
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
import re
import hashlib
from model_media import *
import model_media
import mimetype_dic
from mimetype_dic import dic1, dic2


import Queue
from threading import Thread
from PIL import Image
from PIL.ExifTags import TAGS

VERSION_APP_VALUE = 2


class MediaParse(object):
    def __init__(self, node, extractDeleted, extractSource):
        self.node = node
        self.fs = node.FileSystem
        self.dcim_node = None 
        self.nodes = []
        self.filesNode = []
        #self.extractDeleted = False
        self.extractDeleted = extractDeleted
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
        self.media_id = []
        self.deleted_media_id = []
        self.media_url = ''
        
    def parse(self):
        if self.mm.need_parse(self.db_cache, VERSION_APP_VALUE):
            self.mm.db_create(self.db_cache)
            if re.findall('/com.android.providers.media/databases/internal.db$', self.node.AbsolutePath):
                self.dcim_node = self.node.FileSystem.Search('/DCIM$')
                if len(self.dcim_node) == 0:
                    return []
                self.dcim_node = self.dcim_node[0]
                self.nodes = [self.node, self.node.Parent.GetByPath('/external.db')]
                self.analyze_media_with_db()
                self.analyze_thumbnails_with_db()
            else:
                fileNode = self.fs.Search('/DCIM/.*\..*$')
                for node in fileNode:
                    self.filesNode.append(node)
                self.analyze_media_with_file_system()
            self.mm.db_insert_table_version(model_media.VERSION_KEY_DB, model_media.VERSION_VALUE_DB)
            self.mm.db_insert_table_version(model_media.VERSION_KEY_APP, VERSION_APP_VALUE)
            self.mm.db_commit()
            self.mm.db_close()
        generate = Generate(self.db_cache, model_media.COORDINATE_TYPE_GPS)
        models = generate.get_models()
        return models

    def analyze_media_with_db(self):
        for i, node in enumerate(self.nodes):
            try:
                self.db = SQLiteParser.Database.FromNode(node, canceller)
                if self.db is None:
                    return 
                ts = SQLiteParser.TableSignature('files')
                for rec in self.db.ReadTableRecords(ts, self.extractDeleted, True):
                    media = Media()
                    canceller.ThrowIfCancellationRequested()
                    media.id = self._db_record_get_int_value(rec, '_id')
                    media.url = self._db_record_get_string_value(rec, '_data')
                    media.deleted = rec.IsDeleted
                    if not re.findall('DCIM', media.url):
                        continue
                    if re.findall('thumbnails', media.url):
                        continue
                    media.url = self.dcim_node.AbsolutePath + re.sub('.*DCIM', '', media.url)
                    media_url = os.path.basename(media.url)
                    try:
                        media_nodes = self.dcim_node.Search(media_url + '$')
                        if len(list(media_nodes)) != 0:
                            media.url = media_nodes[0].AbsolutePath
                            self.media_url = media_nodes[0].PathWithMountPoint
                            self.assignment(media)
                        else:
                            media.deleted = 1
                    except:
                        media.deleted = 1
                    media.size = self._db_record_get_int_value(rec, '_size')
                    media.add_date = self._db_record_get_int_value(rec, 'date_added')
                    media.modify_date = self._db_record_get_int_value(rec, 'date_modified')
                    media.suffix = self._db_record_get_string_value(rec, 'mime_type')
                    media.title = self._db_record_get_string_value(rec, 'title')
                    media.width = self._db_record_get_int_value(rec, 'width')
                    media.height = self._db_record_get_int_value(rec, 'height')
                    media.description = self._db_record_get_string_value(rec ,'description')
                    fileSuffix = re.sub('.*\.', '', os.path.basename(media.url))  #文件后缀
                    media.suffix = fileSuffix
                    if not fileSuffix in dic1:
                        continue
                    fileType = dic1[fileSuffix]  #文件类型
                    media.type = fileType
                    media.latitude = self._db_record_get_value(rec, 'latitude')
                    media.longitude = self._db_record_get_value(rec, 'longitude')
                    media.datetaken = self._db_record_get_int_value(rec, 'datetaken')
                    duration = self._db_record_get_int_value(rec, 'duration')
                    media.duration = float(duration/1000)
                    media.artist = self._db_record_get_string_value(rec, 'artist')
                    media.album = self._db_record_get_string_value(rec, 'album')
                    media.source = node.AbsolutePath
                    if media.id not in self.media_id:
                        self.media_id.append(media.id)
                    if media.id not in self.deleted_media_id and media.deleted == 1:
                        self.deleted_media_id.append(media.id)
                    self.mm.db_insert_table_media(media)
                self.mm.db_commit()
            except:
                traceback.print_exc()

    def analyze_thumbnails_with_db(self):
        for i, node in enumerate(self.nodes):
            try:
                self.db = SQLiteParser.Database.FromNode(node, canceller)
                if self.db is None:
                    return 
                ts = SQLiteParser.TableSignature('thumbnails')
                for rec in self.db.ReadTableRecords(ts, self.extractDeleted, True):
                    thumbnails = Thumbnails()
                    canceller.ThrowIfCancellationRequested()
                    id = self._db_record_get_int_value(rec, '_id')
                    thumbnails.url = self._db_record_get_string_value(rec, '_data')
                    thumbnails.url = self.dcim_node.AbsolutePath + re.sub('.*DCIM', '', thumbnails.url)
                    thumbnails.media_id = self._db_record_get_int_value(rec, 'image_id')
                    try:
                        media_url = os.path.basename(thumbnails.url).replace('+', '.')
                        thumbnail_path = ''
                        media_nodes = self.dcim_node.Search(media_url+'$')
                    except:
                        media_nodes = []
                    if len(list(media_nodes)) != 0:
                        thumbnails.url = media_nodes[0].AbsolutePath
                        thumbnail_path = media_nodes[0].PathWithMountPoint
                        #如果本地目录中缩略图存在，本地原图及数据库中原图记录不存在，则将缩略图判定为删除的原图
                        if thumbnails.media_id not in self.media_id:
                            media = Media()
                            media.url = thumbnails.url
                            media.width = self._db_record_get_int_value(rec, 'width')
                            media.height = self._db_record_get_int_value(rec, 'height')
                            media.source = node.AbsolutePath
                            media.deleted = 1
                            self.mm.db_insert_table_media(media)
                        #如果本地目录中缩略图存在，数据库中有原图记录但为删除记录，则将缩略图判定为删除的原图
                        elif thumbnails.media_id in self.deleted_media_id:
                            media = Media()
                            media.url = thumbnails.url
                            media.width = self._db_record_get_int_value(rec, 'width')
                            media.height = self._db_record_get_int_value(rec, 'height')
                            media.source = node.AbsolutePath
                            media.deleted = 1
                            self.mm.db_insert_table_media(media)
                    if thumbnail_path != '':
                        filecCeateDate = os.path.getmtime(thumbnail_path)  #文件创建时间
                        thumbnails.create_date = filecCeateDate
                    thumbnails.width = self._db_record_get_int_value(rec, 'width')
                    thumbnails.height = self._db_record_get_int_value(rec, 'height')
                    thumbnails.source = node.AbsolutePath
                    thumbnails.deleted = rec.IsDeleted
                    if thumbnail_path == '':
                        thumbnails.deleted = 1
                    if id != 0:
                        self.mm.db_insert_table_thumbnails(thumbnails)
                self.mm.db_commit()
            except:
                traceback.print_exc()

    def analyze_media_with_file_system(self):
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
            if name.endswith(suffixes) and re.findall(".thumbnail", name):
                try:
                    thumbnail = Thumbnails()
                    thumbnail.url = fileNode.AbsolutePath
                    thumbnail.create_date = os.path.getmtime(name)
                    thumbnail.source  = self.node.AbsolutePath
                    thumbnail.deleted  = 0
                    self.mm.db_insert_table_thumbnails(thumbnail)
                except:
                    pass
            elif name.endswith(suffixes):
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
                    #media.display_name = fileName
                    fileSuffix = re.sub('.*\.', '', filePath)  #文件后缀
                    media.suffix = fileSuffix
                    fileType = dic1[fileSuffix]  #文件类型
                    media.type = fileType
                    fileParFullDir = os.path.dirname(filePath)
                    fileParDir = os.path.basename(fileParFullDir)  #文件所在文件夹名
                    #media.parent = fileParDir
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
                        self.media_url = filePath
                        self.assignment(media)
                    elif fileType == 'video':
                        fileDuration = self._get_video_duration(filePath)  #文件播放时长
                        media.duration = fileDuration
                    self.medias.append(media)
                except Exception as e:
                    print(e)
        except Exception as e:
            traceback.print_exc()

    def get_exif_data(self, fname):
        '''获取图片metadata'''
        ret = {}
        return ret
        """ try:
            img = Image.open(fname)
            if hasattr(img, '_getexif'):
                exifinfo = img._getexif()
                if exifinfo != None:
                    for tag, value in exifinfo.items():
                        decoded = TAGS.get(tag, tag)
                        ret[decoded] = value
                return ret
        except:
            return {} """

    def assignment(self, media):
        '''给media详细信息赋值'''
        try:
            ret = self.get_exif_data(self.media_url)
            if ret is None:
                return
            if '42036' in ret:
                media.aperture = str(ret['42036'])
            if 'Artist' in ret:
                media.artist = ret['Artist']
            #if 'DateTimeOriginal' in ret:
            #    media.datetaken = ret['DateTimeOriginal']
            if 'Software' in ret:
                media.software = ret['Software']
            if 'ExifImageWidth' in ret:
                media.resolution = str(ret['ExifImageWidth']) + '*' + str(ret['ExifImageHeight'])
            if 'XResolution' in ret:
                if len(ret['XResolution']) == 2:
                    xr = ret['XResolution']
                    media.xresolution = str(int(xr[0]/xr[1]))
            if 'YResolution' in ret:
                if len(ret['YResolution']) == 2:
                    yr = ret['YResolution']
                    media.yresolution = str(int(yr[0]/yr[1]))
            if 'ColorSpace' in ret:
                ss = 'sRGB' if ret['ColorSpace'] == 1 else ret['ColorSpace']
                media.color_space = ss
            if 'Make' in ret:
                media.make = ret['Make']
            if 'Model' in ret:
                media.model = ret['Model']
            if 'ExposureTime' in ret:
                if len(ret['ExposureTime']) == 2:
                    et = ret['ExposureTime']
                    media.exposure_time = str(int(et[0]/et[1]))
            if 'ISOSpeedRatings' in ret:
                media.iso = str(ret['ISOSpeedRatings'])
            if 'FocalLength' in ret:
                if len(ret['FocalLength']) == 2:
                    fl = ret['FocalLength']
                    media.focal_length = str(int(fl[0]/fl[1]))
            if 'ExposureProgram' in ret:
                media.exposure_program = str(ret['ExposureProgram'])
            if 'ExifVersion' in ret:
                media.exif_version = str(ret['ExifVersion'])
        except:
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
                    try:
                        GPSInfo = ret['GPSInfo']
                        latitudeFlag = GPSInfo[1]
                        latitude = float(GPSInfo[2][0][0])/float(GPSInfo[2][0][1]) + float(GPSInfo[2][1][0])/float(GPSInfo[2][1][1])/float(60) + float(GPSInfo[2][2][0])/float(GPSInfo[2][2][1])/float(3600)
                        longitudeFlag = GPSInfo[3]
                        longitude = float(GPSInfo[4][0][0])/float(GPSInfo[4][0][1]) + float(GPSInfo[4][1][0])/float(GPSInfo[4][1][1])/float(60) + float(GPSInfo[4][2][0])/float(GPSInfo[4][2][1])/float(3600)
                    except:
                        pass
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

    @staticmethod
    def _db_record_get_string_value(record, column, default_value=''):
        if not record[column].IsDBNull:
            try:
                value = str(record[column].Value)
                #if record.Deleted != DeletedState.Intact:
                #    value = filter(lambda x: x in string.printable, value)
                return value
            except Exception as e:
                return default_value
        return default_value

    @staticmethod
    def _db_record_get_int_value(record, column, default_value=0):
        if not IsDBNull(record[column].Value):
            try:
                return int(record[column].Value)
            except Exception as e:
                return default_value
        return default_value

    @staticmethod
    def _db_record_get_value(record, column, default_value=0):
        if not IsDBNull(record[column].Value):
            try:
                return record[column].Value
            except Exception as e:
                return default_value
        return default_value

    @staticmethod
    def _db_record_get_blob_value(record, column, default_value=None):
        if not record[column].IsDBNull:
            try:
                value = record[column].Value
                return bytes(value)
            except Exception as e:
                return default_value
        return default_value


def analyze_android_media(node, extractDeleted, extractSource):
    pr = ParserResults()
    pr.Models.AddRange(MediaParse(node, extractDeleted, extractSource).parse())
    pr.Build('多媒体')
    return pr

def execute(node, extractDeleted):
    return analyze_android_media(node, extractDeleted, False)