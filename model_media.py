# -*- coding: utf-8 -*-

from PA_runtime import *
import clr
clr.AddReference('System.Core')
clr.AddReference('System.Xml.Linq')
clr.AddReference('System.Data.SQLite')
del clr

from System.IO import MemoryStream
from System.Text import Encoding
from System.Xml.Linq import *
from System.Linq import Enumerable
from System.Xml.XPath import Extensions as XPathExtensions
import PA.InfraLib.ModelsV2.Base.MediaFile as MediaFile
import PA.InfraLib.ModelsV2.Base as Base
import PA.InfraLib.ModelsV2.CommonEnum.CoordinateType as CoordinateType

import os
import System
import System.Data.SQLite as SQLite
import sqlite3
import re
import traceback

from mimetype_dic import dic1 as suffix2type, dic2 as type2suffix
from PIL import Image
from PIL.ExifTags import TAGS

SQL_CREATE_TABLE_MEDIA = '''
    CREATE TABLE IF NOT EXISTS media(
        id INTEGER,
        url TEXT,
        size INTEGER,
        add_date INTEGER,
        modify_date INTEGER,
        suffix TEXT,
        type TEXT,
        title TEXT,
        latitude DOUBLE,
        longitude DOUBLE,
        address TEXT,
        datetaken INTEGER,
        duration INTEGER,
        artist TEXT,
        album TEXT,
        width INTEGER,
        height INTEGER,
        description TEXT,
        aperture TEXT,
        color_space TEXT,
        exif_version TEXT,
        exposure_program TEXT,
        exposure_time TEXT,
        focal_length TEXT,
        iso TEXT,
        make TEXT,
        model TEXT,
        resolution TEXT,
        software TEXT,
        xresolution TEXT,
        yresolution TEXT,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER
    )'''

SQL_CREATE_TABLE_THUMBNAILS = '''
    CREATE TABLE IF NOT EXISTS thumbnails(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT,
        media_id INTEGER,
        width INTEGER,
        height INTEGER,
        create_date INTEGER,
        suffix TEXT,
        source TEXT,
        deleted INTEGER,
        repeated INTEGER
    )'''

SQL_CREATE_TABLE_MEDIA_LOG = '''CREATE TABLE IF NOT EXISTS media_log(
    id INTEGER  PRIMARY KEY AUTOINCREMENT,
    media_id INTEGER,
    add_date INTEGER,
    adjustment_date INTEGER,
    cloudbatchpublish_date INTEGER,
    create_date INTEGER,
    faceadjustment_date INTEGER,
    lastshare_date INTEGER,
    modify_date INTEGER,
    taken_date INTEGER,
    trash_date INTEGER,
    directory TEXT,
    filename TEXT,
    deleted INTEGER,
    source TEXT,
    repeated INTEGER
)'''

SQL_INSERT_TABLE_MEDIA_LOG = '''INSERT INTO media_log(id, media_id, add_date, adjustment_date, cloudbatchpublish_date, 
    create_date, faceadjustment_date, lastshare_date, 
    modify_date, taken_date, trash_date, directory, filename, deleted, source, repeated)
    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''

SQL_INSERT_TABLE_MEDIA = '''
    INSERT INTO media(id, url, size, add_date, modify_date, suffix, type, title, latitude, longitude, address,
        datetaken, duration, artist, album, width, height, description, aperture, color_space, exif_version,
        exposure_program, exposure_time, focal_length, iso, make, model, resolution, software, xresolution,
        yresolution, source, deleted, repeated)
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

SQL_INSERT_TABLE_THUMBNAILS = '''
    INSERT INTO thumbnails(id, url, media_id, width, height, create_date, suffix, source, deleted, repeated)
        values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

SQL_CREATE_TABLE_VERSION = '''
    create table if not exists version(
        key TEXT primary key,
        version INT)'''

SQL_INSERT_TABLE_VERSION = '''
    insert into version(key, version) values(?, ?)'''

COORDINATE_TYPE_GOOGLE = 1
COORDINATE_TYPE_GPS = 2

VERSION_KEY_DB = 'db'
VERSION_KEY_APP = 'app'

VERSION_VALUE_DB = 2

class MM(object):
    def __init__(self):
        self.db = None
        self.db_cmd = None
        self.db_trans = None

    def db_create(self,db_path):
        self.db_remove(db_path)
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(db_path))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        self.db_trans = self.db.BeginTransaction()
        self.db_create_table()
        self.db_commit()

    def db_commit(self):
        if self.db_trans is not None:
            self.db_trans.Commit()
        self.db_trans = self.db.BeginTransaction()

    def db_close(self):
        self.db_trans = None
        if self.db_cmd is not None:
            self.db_cmd.Dispose()
            self.db_cmd = None
        if self.db is not None:
            self.db.Close()
            self.db = None

    def db_remove(self, db_path):
        try:
            if os.path.exists(db_path):
                os.remove(db_path)
        except Exception as e:
            print("model_media db_create() remove %s error:%s"%(db_path, e))

    def db_create_table(self):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = SQL_CREATE_TABLE_MEDIA
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_THUMBNAILS
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_MEDIA_LOG
            self.db_cmd.ExecuteNonQuery()
            self.db_cmd.CommandText = SQL_CREATE_TABLE_VERSION
            self.db_cmd.ExecuteNonQuery()

    def db_insert_table(self, sql, values):
        if self.db_cmd is not None:
            self.db_cmd.CommandText = sql
            self.db_cmd.Parameters.Clear()
            for value in values:
                param = self.db_cmd.CreateParameter()
                param.Value = value
                self.db_cmd.Parameters.Add(param)
            self.db_cmd.ExecuteNonQuery()

    def db_insert_table_media(self, Media):
        self.db_insert_table(SQL_INSERT_TABLE_MEDIA, Media.get_values())

    def db_insert_table_thumbnails(self, Thumbnails):
        self.db_insert_table(SQL_INSERT_TABLE_THUMBNAILS, Thumbnails.get_values())

    def db_insert_table_media_log(self, MediaLog):
        self.db_insert_table(SQL_INSERT_TABLE_MEDIA_LOG, MediaLog.get_values())

    def db_insert_table_version(self, key, version):
        self.db_insert_table(SQL_INSERT_TABLE_VERSION, (key, version))

    @staticmethod
    def need_parse(cache_db, app_version):
        if not os.path.exists(cache_db):
            return True
        db = sqlite3.connect(cache_db)
        cursor = db.cursor()
        sql = 'select key,version from version'
        row = None
        db_version_check = False
        app_version_check = False
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
        except Exception as e:
            TraceService.Trace(TraceLevel.Error, "model_im.py Error: db:{} LINE {}".format(cache_db, traceback.format_exc()))

        while row is not None:
            if row[0] == VERSION_KEY_DB and row[1] == VERSION_VALUE_DB:
                db_version_check = True
            elif row[0] == VERSION_KEY_APP and row[1] == app_version:
                app_version_check = True
            row = cursor.fetchone()

        if cursor is not None:
            cursor.close()
        if db is not None:
            db.close()
        return not (db_version_check and app_version_check)


class Column(object):
    def __init__(self):
        self.source = None
        self.deleted = 0
        self.repeated = 0

    def __setattr__(self, name, value):
        if not IsDBNull(value):
            if isinstance(value, str):
                self.__dict__[name] = re.compile(u"[\\x00-\\x08\\x0b-\\x0c\\x0e-\\x1f]").sub('', value)
            else:
                self.__dict__[name] = value
        else:
            self.__dict__[name] = None

    def get_values(self):
        return(self.source, self.deleted, self.repeated)

class Media(Column):
    def __init__(self):
        super(Media, self).__init__()
        self.id = None
        self.url = None
        self.size = None
        self.add_date = None
        self.modify_date = None
        self.suffix = None
        self.type = None
        self.title = None
        self.latitude = None
        self.longitude = None
        self.address = None
        self.datetaken = None
        self.duration = None
        self.artist = None
        self.album = None
        self.width = None
        self.height = None
        self.description = None
        self.aperture = None
        self.color_space = None 
        self.exif_version = None
        self.exposure_program = None 
        self.exposure_time = None 
        self.focal_length = None 
        self.iso = None 
        self.make = None 
        self.model = None 
        self.resolution = None 
        self.software = None 
        self.xresolution = None
        self.yresolution = None

    def get_values(self):
        return (self.id, self.url, self.size, self.add_date, self.modify_date, self.suffix, self.type, self.title,
                self.latitude, self.longitude, self.address, self.datetaken, self.duration, self.artist, self.album, 
                self.width, self.height, self.description, self.aperture, self.color_space,  self.exif_version,
                self.exposure_program, self.exposure_time, self.focal_length, self.iso, self.make, self.model, 
                self.resolution, self.software, self.xresolution, self.yresolution) + super(Media, self).get_values()


class Thumbnails(Column):
    def __init__(self):
        super(Thumbnails, self).__init__()
        self.id = None
        self.url = None
        self.media_id = None
        self.width = None
        self.height = None
        self.create_date = None
        self.suffix = None
        
    def get_values(self):
        return (self.id, self.url, self.media_id, self.width, self.height, self.create_date, self.suffix) + super(Thumbnails, self).get_values()

class MediaLog(Column):
    def __init__(self):
        super(MediaLog, self).__init__()
        self.id = None
        self.media_id = None
        self.add_date = None 
        self.adjustment_date = None 
        self.cloudbatchpublish_date = None 
        self.create_date = None
        self.faceadjustment_date = None 
        self.lastshare_date = None
        self.modify_date = None
        self.taken_date = None 
        self.trash_date = None
        self.directory = None
        self.filename = None

    def get_values(self):
        return (self.id, self.media_id, self.add_date, self.adjustment_date, self.cloudbatchpublish_date, 
                self.create_date, self.faceadjustment_date, self.lastshare_date, self.modify_date, 
                self.taken_date, self.trash_date, self.directory, self.filename, 
                self.deleted, self.source, self.repeated) + super(MediaLog, self).get_values()

class Generate(object):

    def __init__(self, db_cache, CoordinateType):
        self.db_cache = db_cache
        self.db = None
        self.db_cmd = None
        self.db_trans = None
        self.coordinate_type = CoordinateType

    def get_models(self):
        models = []
        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.db_cache))
        self.db.Open()
        self.db_cmd = SQLite.SQLiteCommand(self.db)
        models.extend(self._get_model_media())
        models.extend(self._get_model_thumbnail())
        models.extend(self._get_model_media_log())
        self.db.Close()
        self.db = None
        return models

    def get_model_from_dir(self, node):
        '''从给定目录获取媒体信息转成model'''
        models = []
        image_suffix = type2suffix['image']
        video_suffix = type2suffix['video']
        audio_suffix = type2suffix['audio']
        for parent, dirnames, filenames in os.walk(node.PathWithMountPoint):
            for filename in filenames:
                suffix = re.sub('.*\.', '', filename)
                #获取jpg图片的exif信息
                if filename.endswith('jpg'):
                    file = os.path.join(parent, filename)
                    ret = self.get_exif_data(file)
                    models.extend(self.get_exif_info(ret, node))
                #获取其他类型图片信息
                elif suffix in image_suffix:
                    pass
                #获取视频信息
                elif suffix in video_suffix:
                    pass
                #获取音频信息
                elif suffix in audio_suffix:
                    pass
        return models

    def get_exif_data(self, fname):
        '''获取图片metadata'''
        ret = {}
        try:
            img = Image.open(fname)
            if hasattr(img, '_getexif'):
                exifinfo = img._getexif()
                if exifinfo != None:
                    for tag, value in exifinfo.items():
                        decoded = TAGS.get(tag, tag)
                        ret[decoded] = value
                return ret
        except:
            return {}

    def get_exif_info(self, ret, node):
        '''获取媒体文件的exif信息'''
        try:
            if ret is {}:
                return
            model = []
            image = MediaFile.ImageFile()
            path = node.PathWithMountPoint
            image.FileName = os.path.basename(path)
            image.Path = node.AbsolutePath
            image.Size = os.path.getsize(path)
            addTime = os.path.getctime(path)
            image.FileExtention = 'jpg'
            image.MimeType = 'image'
            image.AddTime = self._get_timestamp(addTime)
            location = Base.Location(image)
            coordinate = Base.Coordinate()
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
                coordinate.Longitude = longitude
                coordinate.Latitude = latitude
                coordinate.Type = CoordinateType.Google if self.coordinate_type == COORDINATE_TYPE_GOOGLE else CoordinateType.GPS
            except:
                pass
            location.Coordinate = coordinate
            location.Time = image.AddTime
            location.SourceType = LocationSourceType.Media
            image.Location = location
            modifyTime = os.path.getmtime(path)
            image.ModifyTime = self._get_timestamp(modifyTime)
            try:
                width = None
                height = None
                if 'ExifImageWidth' in ret.keys() and 'ImageLength' in ret.keys():
                    width = ret['ExifImageWidth']
                    height = ret['ImageLength']
                image.Height = height
                image.Width = width
            except:
                pass
            takenDate = os.path.getctime(path)
            image.TakenDate = self._get_timestamp(takenDate)
            if '42036' in ret:
                image.Aperture = str(ret['42036'])
            if 'Artist' in ret:
                image.Artist = ret['Artist']
            if 'ColorSpace' in ret:
                ss = 'sRGB' if ret['ColorSpace'] == 1 else ret['ColorSpace']
                image.ColorSpace = str(ss)
            if 'ExifVersion' in ret:
                image.ExifVersion = str(ret['ExifVersion'])
            if 'ExposureProgram' in ret:
                image.ExposureProgram = str(ret['ExposureProgram'])
            if 'ExposureTime' in ret:
                if len(ret['ExposureTime']) == 2:
                    et = ret['ExposureTime']
                    image.ExposureTime = str(int(et[0]/et[1]))
            if 'FocalLength' in ret:
                if len(ret['FocalLength']) == 2:
                    fl = ret['FocalLength']
                    image.FocalLength = str(int(fl[0]/fl[1]))
            if 'ISOSpeedRatings' in ret:
                image.ISO = str(ret['ISOSpeedRatings'])
            if 'Make' in ret:
                image.Make = ret['Make']
            if 'Model' in ret:
                image.Model = ret['Model']
            if 'ExifImageWidth' in ret:
                image.Resolution = str(ret['ExifImageWidth']) + '*' + str(ret['ExifImageHeight'])
            if 'Software' in ret:
                image.Software = ret['Software']
            if 'XResolution' in ret:
                if len(ret['XResolution']) == 2:
                    xr = ret['XResolution']
                    image.XResolution = str(int(xr[0]/xr[1]))
                    if image.XResolution is not '' and image.XResolution is not None:
                        image.XResolution = image.XResolution + ' dpi'
            if 'YResolution' in ret:
                if len(ret['YResolution']) == 2:
                    yr = ret['YResolution']
                    image.XResolution = str(int(yr[0]/yr[1]))
                    if image.YResolution is not '' and image.YResolution is not None:
                        image.YResolution = image.YResolution + ' dpi'
            image.SourceFile = node.AbsolutePath
            if location.Coordinate is not None:
                model.append(location)
            model.append(image)
            return model
        except:
            return []

    def _get_model_media(self):
        model = []
        try:
            self.db_cmd.CommandText = '''select distinct * from media order by deleted ASC'''
            sr = self.db_cmd.ExecuteReader()
            pk = []
            while(sr.Read()):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if sr[1] is None or sr[6] is None:
                        continue
                    if sr[0] is None or sr[0] == 0:
                        continue
                    media_type = sr[6]
                    media_id = sr[0]
                    deleted = sr[19]
                    #remove duplication
                    if media_id in pk:
                        continue
                    else:
                        pk.append(media_id)
                    #audio convert
                    if media_type == "audio":
                        audio = MediaFile.AudioFile()
                        audio.FileName = self._db_reader_get_string_value(sr, 7)
                        audio.Path = self._db_reader_get_string_value(sr, 1)
                        #audio.NodeOrUrl.Init(path)
                        audio.Size = self._db_reader_get_int_value(sr, 2)
                        addTime = self._db_reader_get_int_value(sr, 3)
                        audio.AddTime = self._get_timestamp(addTime)
                        audio.Description = self._db_reader_get_string_value(sr, 17)
                        media_log = self._get_media_log(sr[0])
                        for log in media_log:
                            video.Logs.Add(log)
                        modifyTime = self._db_reader_get_int_value(sr, 4)
                        audio.FileExtention = self._db_reader_get_string_value(sr, 5)
                        audio.MimeType = self._db_reader_get_string_value(sr, 5)
                        audio.ModifyTime = self._get_timestamp(modifyTime)
                        audio.Album = self._db_reader_get_string_value(sr, 14)
                        audio.Artist = self._db_reader_get_string_value(sr, 13)
                        audio.duration = self._db_reader_get_int_value(sr, 12)
                    #image convert
                    elif media_type == "image":
                        image = MediaFile.ImageFile()
                        image.FileName = self._db_reader_get_string_value(sr, 7)
                        image.Path = self._db_reader_get_string_value(sr, 1)
                        #image.NodeOrUrl.Init(path)
                        image.Size = self._db_reader_get_int_value(sr, 2)
                        addTime = self._db_reader_get_int_value(sr, 3)
                        image.FileExtention = self._db_reader_get_string_value(sr, 5)
                        image.MimeType = self._db_reader_get_string_value(sr, 5)
                        image.AddTime = self._get_timestamp(addTime)
                        image.Description = self._db_reader_get_string_value(sr, 17)
                        location = Base.Location(image)
                        coordinate = Base.Coordinate()
                        if not IsDBNull(sr[8]):
                            coordinate.Latitude = float(sr[8])
                        if not IsDBNull(sr[9]):
                            coordinate.Longitude = float(sr[9])
                            coordinate.Type = CoordinateType.Google if self.coordinate_type == COORDINATE_TYPE_GOOGLE else CoordinateType.GPS
                        location.Coordinate = coordinate
                        location.Time = image.AddTime
                        location.AddressName = self._db_reader_get_string_value(sr, 10)
                        location.SourceType = LocationSourceType.Media
                        image.Location = location
                        media_log = self._get_media_log(sr[0])
                        for log in media_log:
                            image.Logs.Add(log)
                        modifyTime = self._db_reader_get_int_value(sr, 4)
                        image.ModifyTime = self._get_timestamp(modifyTime)
                        image.Height = self._db_reader_get_int_value(sr, 16)
                        image.Width = self._db_reader_get_int_value(sr, 15)
                        takenDate = self._db_reader_get_int_value(sr, 11)
                        image.TakenDate = self._get_timestamp(takenDate)
                        image.Thumbnail = self._get_media_thumbnail(media_id)
                        image.Aperture = self._db_reader_get_string_value(sr,18)
                        image.Artist = self._db_reader_get_string_value(sr,13)
                        image.ColorSpace = self._db_reader_get_string_value(sr,19)
                        image.ExifVersion = self._db_reader_get_string_value(sr,20)
                        image.ExposureProgram = self._db_reader_get_string_value(sr,21)
                        image.ExposureTime = self._db_reader_get_string_value(sr,22)
                        image.FocalLength = self._db_reader_get_string_value(sr,23)
                        image.ISO = self._db_reader_get_string_value(sr,24)
                        image.Make = self._db_reader_get_string_value(sr,25)
                        image.Model = self._db_reader_get_string_value(sr,26)
                        image.Resolution = self._db_reader_get_string_value(sr,27)
                        image.Software = self._db_reader_get_string_value(sr,28)
                        image.XResolution = self._db_reader_get_string_value(sr,29)
                        if image.XResolution is not '':
                            image.XResolution = image.XResolution + ' dpi'
                        image.YResolution = self._db_reader_get_string_value(sr,30)
                        if image.YResolution is not '':
                            image.YResolution = image.YResolution + ' dpi'
                        image.SourceFile = self._get_source_file(str(sr[31]))
                        image.Deleted = self._convert_deleted_status(sr[32])
                        #location = Base.Location(image)
                        #coordinate = Base.Coordinate()
                        #if not IsDBNull(sr[8]):
                        #    coordinate.Latitude = float(sr[8])
                        #if not IsDBNull(sr[9]):
                        #    coordinate.Longitude = float(sr[9])
                        #    coordinate.Type = CoordinateType.Google if self.coordinate_type == COORDINATE_TYPE_GOOGLE else CoordinateType.GPS
                        #location.Coordinate = coordinate
                        #location.Time = image.AddTime
                        #location.AddressName = self._db_reader_get_string_value(sr, 10)
                        if not IsDBNull(sr[9]):
                            model.append(location)
                        model.append(image)
                    #video convert
                    elif media_type == "video":
                        video = MediaFile.VideoFile()
                        video.FileName = self._db_reader_get_string_value(sr, 7)
                        video.Path = self._db_reader_get_string_value(sr, 1)
                        #video.NodeOrUrl.Init(path)
                        video.Size = self._db_reader_get_int_value(sr, 2)
                        addTime = self._db_reader_get_int_value(sr, 3)
                        video.FileExtention = self._db_reader_get_string_value(sr, 5)
                        video.MimeType = self._db_reader_get_string_value(sr, 5)
                        video.AddTime = self._get_timestamp(addTime)
                        video.Description = self._db_reader_get_string_value(sr, 17)
                        location = Base.Location(video)
                        coordinate = Base.Coordinate()
                        if not IsDBNull(sr[8]):
                            coordinate.Latitude = float(sr[8])
                        if not IsDBNull(sr[9]):
                            coordinate.Longitude = float(sr[9])
                            coordinate.Type = CoordinateType.Google if self.coordinate_type == COORDINATE_TYPE_GOOGLE else CoordinateType.GPS
                        location.Coordinate = coordinate
                        location.Time = video.AddTime
                        location.AddressName = self._db_reader_get_string_value(sr, 10)
                        location.SourceType = LocationSourceType.Media
                        video.Location = location
                        media_log = self._get_media_log(sr[0])
                        for log in media_log:
                            video.Logs.Add(log)
                        modifyTime = self._db_reader_get_int_value(sr, 4)
                        takenDate = self._db_reader_get_int_value(sr, 11)
                        video.TakenDate = self._get_timestamp(takenDate)
                        video.ModifyTime = self._get_timestamp(modifyTime)
                        if not IsDBNull(sr[12]):
                            video.Duration = sr[12]
                        video.SourceFile = self._get_source_file(str(sr[31]))
                        video.Deleted = self._convert_deleted_status(sr[32])
                        #location = Base.Location(video)
                        #coordinate = Base.Coordinate()
                        #if not IsDBNull(sr[8]):
                        #    coordinate.Latitude = float(sr[8])
                        #if not IsDBNull(sr[9]):
                        #    coordinate.Longitude = float(sr[9])
                        #    coordinate.Type = CoordinateType.Google if self.coordinate_type == COORDINATE_TYPE_GOOGLE else CoordinateType.GPS
                        #location.Coordinate = coordinate
                        #location.Time = video.AddTime
                        #location.AddressName = self._db_reader_get_string_value(sr, 10)
                        if not IsDBNull(sr[9]):
                            model.append(location)
                        model.append(video)
                except:
                    traceback.print_exc()
            sr.Close()
        except:
            pass
        return model

    def _get_model_thumbnail(self):
        model = []
        try:
            self.db_cmd.CommandText = '''select distinct * from thumbnails order by deleted ASC'''
            sr = self.db_cmd.ExecuteReader()
            pk = []
            while(sr.Read()):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    if sr[0] is None or sr[0] == 0:
                        continue
                    id = sr[0]
                    if id in pk:
                        continue
                    else:
                        pk.append(id)
                    thumbnail = MediaFile.ThumbnailFile()
                    filePath = self._db_reader_get_string_value(sr, 1)
                    thumbnail.FileName = os.path.basename(filePath)
                    thumbnail.Path = filePath
                    thumbnail.Height = self._db_reader_get_int_value(sr, 4)
                    thumbnail.Width = self._db_reader_get_int_value(sr, 3)
                    addTime = self._db_reader_get_int_value(sr, 5)
                    thumbnail.AddTime = self._get_timestamp(addTime)
                    thumbnail.FileExtention = self._db_reader_get_string_value(sr, 6)
                    thumbnail.MimeType = self._db_reader_get_string_value(sr, 6)
                    thumbnail.SourceFile = self._get_source_file(str(sr[7]))
                    thumbnail.Deleted = self._convert_deleted_status(sr[8])
                    model.append(thumbnail)
                except:
                    traceback.print_exc()
            sr.Close()
        except:
            traceback.print_exc()
        return model

    def _get_model_media_log(self):
        model = []
        try:
            self.db_cmd.CommandText = '''select distinct * from media_log'''
            sr = self.db_cmd.ExecuteReader()
            while(sr.Read()):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    add_date = self.transtime(sr, 2)
                    adj_date = self.transtime(sr, 3)
                    cloudshare_date = self.transtime(sr, 4)
                    create_date = self.transtime(sr, 5)
                    faceadj_date = self.transtime(sr, 6)
                    lastshare_date = self.transtime(sr, 7)
                    modify_date = self.transtime(sr, 8)
                    taken_date = self.transtime(sr, 9)
                    trash_date = self.transtime(sr, 10)
                    dates = {add_date:"添加", adj_date:"调整", cloudshare_date:"icloud分享", create_date:"创建", 
                             faceadj_date:"面部调整", lastshare_date:"分享", modify_date:"修改", taken_date:"拍摄", trash_date:"删除"}
                    for date in sorted(dates.items(), key = lambda x:x[0], reverse=True):
                        if date[0] is None:
                            continue
                        log = MediaFile.MediaLog()
                        #log.FilePath = self._db_reader_get_string_value(sr, 14)
                        log.Operating = date[1]
                        log.OperatingTime = str(date[0])
                        model.append(log)
                except:
                    traceback.print_exc()
            sr.Close()
        except:
            traceback.print_exc()
        return model

    def _get_media_log(self, media_id):
        model = []
        try:
            db_cmd = SQLite.SQLiteCommand(self.db)
            db_cmd.CommandText = '''select distinct * from media_log where media_id = {}'''.format(media_id)
            sr = db_cmd.ExecuteReader()
            while(sr.Read()):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    add_date = self.transtime(sr, 2)
                    adj_date = self.transtime(sr, 3)
                    cloudshare_date = self.transtime(sr, 4)
                    create_date = self.transtime(sr, 5)
                    faceadj_date = self.transtime(sr, 6)
                    lastshare_date = self.transtime(sr, 7)
                    modify_date = self.transtime(sr, 8)
                    taken_date = self.transtime(sr, 9)
                    trash_date = self.transtime(sr, 10)
                    dates = {add_date:"添加", adj_date:"调整", cloudshare_date:"icloud分享", create_date:"创建", 
                             faceadj_date:"面部调整", lastshare_date:"分享", modify_date:"修改", taken_date:"拍摄", trash_date:"删除"}
                    for date in sorted(dates.items(), key = lambda x:x[0], reverse=True):
                        if date[0] is None:
                            continue
                        log = MediaFile.MediaLog()
                        #log.FilePath = self._db_reader_get_string_value(sr, 14)
                        log.Operating = date[1]
                        log.OperatingTime = str(date[0])
                        model.append(log)
                except:
                    pass
        except:
            return []
        return model

    def _get_media_thumbnail(self, media_id):
        try:
            db_cmd = SQLite.SQLiteCommand(self.db)
            db_cmd.CommandText = '''select distinct * from thumbnails where media_id = {}'''.format(media_id)
            sr = db_cmd.ExecuteReader()
            while(sr.Read()):
                try:
                    if canceller.IsCancellationRequested:
                        break
                    thumbnail = MediaFile.ThumbnailFile()
                    filePath = self._db_reader_get_string_value(sr, 1)
                    thumbnail.FileName = os.path.basename(filePath)
                    thumbnail.Path = filePath
                    thumbnail.Height = self._db_reader_get_int_value(sr, 4)
                    thumbnail.Width = self._db_reader_get_int_value(sr, 3)
                    addTime = self._db_reader_get_int_value(sr, 5)
                    thumbnail.AddTime = self._get_timestamp(addTime)
                    thumbnail.FileExtention = self._db_reader_get_string_value(sr, 6)
                    thumbnail.MimeType = self._db_reader_get_string_value(sr, 6)
                    thumbnail.SourceFile = self._get_source_file(str(sr[7]))
                    thumbnail.Deleted = self._convert_deleted_status(sr[8])
                    return thumbnail
                except:
                    traceback.print_exc()
        except:
            pass

    def _get_source_file(self, source_file):
        if isinstance(source_file, str):
            return source_file.replace('/', '\\')
        return source_file

    def _get_timestamp(self, timestamp):
        try:
            if isinstance(timestamp, (long, float, str)) and len(str(timestamp)) > 10:
                timestamp = int(str(timestamp)[:10])
            if isinstance(timestamp, (Int64, long, int)) and len(str(timestamp)) == 10:
                ts = TimeStamp.FromUnixTime(timestamp, False)
                if not ts.IsValidForSmartphone():
                    ts = TimeStamp.FromUnixTime(0, False)
                return ts
        except:
            return TimeStamp.FromUnixTime(0, False)

    def transtime(self, reader, index, default_value=0):
        try:
            timestamp = reader.GetInt64(index) if not reader.IsDBNull(index) else default_value
            time = self._get_timestamp(timestamp)
            return time
        except Exception as e:
            return TimeStamp.FromUnixTime(0, False)

    @staticmethod
    def _db_reader_get_string_value(reader, index, default_value=''):
        return reader.GetString(index) if not reader.IsDBNull(index) else default_value

    @staticmethod
    def _db_reader_get_int_value(reader, index, default_value=0):
        return reader.GetInt64(index) if not reader.IsDBNull(index) else default_value

    @staticmethod
    def _db_reader_get_float_value(reader, index, default_value=0):
        return reader.GetFloat(index) if not reader.IsDBNull(index) else default_value

    @staticmethod
    def _convert_deleted_status(deleted):
        if deleted is None:
            return DeletedState.Unknown
        else:
            return DeletedState.Intact if deleted == 0 else DeletedState.Deleted


#class GenerateFromIM(object):
#    '''
#    get media-related data from table message(media message),
#    table account(headpic), table chatroom(groupic), 
#    table friend(headpic), table feed(image, video)
#    in im database and generate media model
#    '''
#    def __init__(self, db_cache, category):
#        self.db_cache = db_cache
#        self.category = category
#        self.db = None
#        self.db_cmd = None
#        self.db_trans = None

#    def get_models(self):
#        self.db = SQLite.SQLiteConnection('Data Source = {}'.format(self.db_cache))
#        self.db.Open()
#        self.db_cmd = SQLite.SQLiteCommand(self.db)
#        models = self._get_model_media()
#        self.db.Close()
#        self.db = None
#        return models

#    def _get_model_media(self):
#        model = []
#        try:
#            sql0x00 = '''select distinct photo from account where photo is not null'''
#            sql0x01 = '''select distinct photo from chatroom where photo is not null'''
#            sql0x02 = '''select distinct photo from friend where photo is not null'''
#            sql0x03 = '''select distinct image_path, video_path from feed where image_path is not null or video_path is not null'''
#            sql0x04 = '''select distinct media_path, type from message where media_path is not null'''
#            for i, sql in enumerate([sql0x00, sql0x01, sql0x02, sql0x03, sql0x04]):
#                self.db_cmd.CommandText = sql
#                sr = self.db_cmd.ExecuteReader()
#                while(sr.Read()):
#                    try:
#                        if canceller.IsCancellationRequested:
#                            break
#                        #head picture
#                        if i<3:
#                            #link filter
#                            image = MediaFile.ImageFile()
#                            path = self._db_reader_get_string_value(sr, 0)
#                            if path == '' or re.findall('.com', path):
#                                continue
#                            image.Path = path
#                            image.FileName = os.path.basenamelse(path)
#                            image.Description = "头像"
#                            image.Categories = self.category
#                            model.append(image)
#                        #feed media
#                        elif i == 3:
#                            image_path = self._db_reader_get_string_value(sr, 0)
#                            if image_path != '' and not re.findall('\.com', image_path):
#                                image = MediaFile.ImageFile()
#                                path = image_path
#                                image.Path = path
#                                image.FileName = os.path.basenamelse(path)
#                                image.Description = "动态图片"
#                                image.Categories = self.category
#                                model.append(image)
#                            video_path = self._db_reader_get_string_value(sr, 1)
#                            if video_path != '' and not re.findall('\.com', image_path):
#                                video = MediaFile.VideoFile()
#                                path = video_path
#                                video.Path = path
#                                video.FileName = os.path.basenamelse(path)
#                                video.Description = "动态视频"
#                                video.Categories = self.category
#                                model.append(video)
#                        #message media
#                        elif i == 4:
#                            type = self._db_reader_get_int_value(sr, 1)
#                            media_path = self._db_reader_get_string_value(sr, 0)
#                            if media_path == '' or re.findall('\.com', media_path):
#                                continue
#                            #image
#                            if type == 2:
#                                image = MediaFile.ImageFile()
#                                path = media_path
#                                image.Path = path
#                                image.FileName = os.path.basenamelse(path)
#                                image.Description = "消息图片"
#                                image.Categories = self.category
#                                model.append(image)
#                            #audio
#                            elif type == 3:
#                                audio = MediaFile.AudioFile()
#                                path = media_path
#                                audio.Path = path
#                                audio.FileName = os.path.basenamelse(path)
#                                audio.Description = "消息语音"
#                                audio.Categories = self.category
#                                model.append(audio)
#                            #video
#                            elif type == 4:
#                                video = MediaFile.VideoFile()
#                                path = media_path
#                                video.Path = path
#                                video.FileName = os.path.basenamelse(path)
#                                video.Description = "动态视频"
#                                video.Categories = self.category
#                                model.append(video)
#                    except:
#                        traceback.print_exc()
#                sr.Close()
#        except:
#            pass
#        return model

    @staticmethod
    def _db_reader_get_string_value(reader, index, default_value=''):
        return reader.GetString(index) if not reader.IsDBNull(index) else default_value

    @staticmethod
    def _db_reader_get_int_value(reader, index, default_value=0):
        return reader.GetInt64(index) if not reader.IsDBNull(index) else default_value